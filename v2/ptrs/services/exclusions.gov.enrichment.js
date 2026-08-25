const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { lookupAbnByNumber } = require("@/data_cleanse/abn-lookup.util");

const UNKNOWN_GOV_ABNS_SQL = `
  WITH distinct_stage_abns AS (
    SELECT DISTINCT
      NULLIF(
        regexp_replace(COALESCE(s."data"->>'payee_entity_abn', ''), '\\D', '', 'g'),
        ''
      ) AS "abn"
    FROM "tbl_ptrs_stage_row" s
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
  )
  SELECT d."abn"
  FROM distinct_stage_abns d
  LEFT JOIN "tbl_ptrs_gov_entity_ref" g
    ON g."deletedAt" IS NULL
    AND NULLIF(regexp_replace(COALESCE(g."abn", ''), '\\D', '', 'g'), '') = d."abn"
  WHERE d."abn" IS NOT NULL
    AND g."id" IS NULL
  ORDER BY d."abn" ASC
`;

const EXISTING_GOV_ABNS_SQL = `
  SELECT DISTINCT
    NULLIF(regexp_replace(COALESCE(g."abn", ''), '\\D', '', 'g'), '') AS "abn"
  FROM "tbl_ptrs_gov_entity_ref" g
  WHERE g."deletedAt" IS NULL
    AND NULLIF(regexp_replace(COALESCE(g."abn", ''), '\\D', '', 'g'), '') IN (:abns)
`;

const LOCK_GOV_ABN_SQL = `
  SELECT pg_advisory_xact_lock(hashtext(:abn))
`;

async function mapWithConcurrency(items, concurrency, worker) {
  const results = new Array(items.length);
  let cursor = 0;

  async function runWorker() {
    while (cursor < items.length) {
      const index = cursor;
      cursor += 1;
      results[index] = await worker(items[index], index);
    }
  }

  const workerCount = Math.max(1, Math.min(concurrency, items.length || 1));
  await Promise.all(Array.from({ length: workerCount }, () => runWorker()));
  return results;
}

async function findUnknownGovCandidateAbns({
  sequelize,
  transaction,
  customerId,
  ptrsId,
}) {
  const [rows] = await sequelize.query(UNKNOWN_GOV_ABNS_SQL, {
    replacements: { customerId, ptrsId },
    transaction,
  });

  return Array.isArray(rows)
    ? rows.map((row) => row.abn).filter(Boolean)
    : [];
}

async function persistNewGovernmentReferences({
  sequelize,
  govEntityModel,
  candidates,
}) {
  if (!Array.isArray(candidates) || !candidates.length) {
    return 0;
  }

  const transaction = await sequelize.transaction();

  try {
    const candidateAbns = candidates.map((row) => row.abn).sort();
    for (const abn of candidateAbns) {
      await sequelize.query(LOCK_GOV_ABN_SQL, {
        replacements: { abn },
        transaction,
      });
    }

    const [existing] = await sequelize.query(EXISTING_GOV_ABNS_SQL, {
      replacements: { abns: candidateAbns },
      transaction,
    });

    const existingAbns = new Set(
      (existing || [])
        .map((row) => String(row.abn || "").replace(/\D/g, ""))
        .filter(Boolean),
    );

    const rowsToInsert = candidates.filter(
      (row) => !existingAbns.has(row.abn),
    );

    if (rowsToInsert.length) {
      await govEntityModel.bulkCreate(rowsToInsert, { transaction });
    }

    await transaction.commit();
    return rowsToInsert.length;
  } catch (error) {
    if (!transaction.finished) {
      try {
        await transaction.rollback();
      } catch (_) {}
    }
    throw error;
  }
}

async function enrichGovReferenceFromStageRows({
  sequelize = db.sequelize,
  customerId,
  ptrsId,
  lookupAbnByNumberFn = lookupAbnByNumber,
  govEntityModel = db.PtrsGovEntityRef,
  beginCustomerTransaction = beginTransactionWithCustomerContext,
}) {
  if (!sequelize) throw new Error("sequelize is required");
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const candidateTransaction = await beginCustomerTransaction(customerId);
  let unknownAbns;

  try {
    unknownAbns = await findUnknownGovCandidateAbns({
      sequelize,
      transaction: candidateTransaction,
      customerId,
      ptrsId,
    });
    await candidateTransaction.commit();
  } catch (error) {
    if (!candidateTransaction.finished) {
      try {
        await candidateTransaction.rollback();
      } catch (_) {}
    }
    throw error;
  }

  const stats = {
    distinctStageAbnsToCheck: unknownAbns.length,
    lookupAttempts: 0,
    lookupFailures: 0,
    unresolvedCount: 0,
    nonGovernmentCount: 0,
    inactiveGovernmentCount: 0,
    governmentMatches: 0,
    inserted: 0,
  };

  if (!unknownAbns.length) {
    return stats;
  }

  if (!process.env.ABR_GUID) {
    logger.logEvent("warn", "PTRS government ABN enrichment skipped: ABR_GUID missing", {
      action: "PtrsV2GovAbnEnrichmentSkipped",
      customerId,
      ptrsId,
      candidateCount: unknownAbns.length,
    });
    return {
      ...stats,
      skipped: "ABR_GUID missing",
    };
  }

  const lookupResults = await mapWithConcurrency(unknownAbns, 5, async (abn) => {
    stats.lookupAttempts += 1;
    try {
      const result = await lookupAbnByNumberFn(abn);
      if (result?.exception) {
        stats.lookupFailures += 1;
        logger.logEvent("warn", "PTRS government ABN enrichment lookup failed", {
          action: "PtrsV2GovAbnLookupFailed",
          customerId,
          ptrsId,
          abn,
          error: result.exception,
        });
        return { ...result, error: result.exception };
      }
      return result;
    } catch (error) {
      stats.lookupFailures += 1;
      logger.logEvent("warn", "PTRS government ABN enrichment lookup failed", {
        action: "PtrsV2GovAbnLookupFailed",
        customerId,
        ptrsId,
        abn,
        error: error.message,
      });
      return {
        requestAbn: abn,
        found: false,
        error: error.message,
      };
    }
  });

  const inserts = [];
  for (const result of lookupResults) {
    if (!result?.found || !result?.abn) {
      if (!result?.error) stats.unresolvedCount += 1;
      continue;
    }

    if (!result.isGovernmentEntity) {
      stats.nonGovernmentCount += 1;
      continue;
    }

    if (
      result.isCurrentAbn !== true ||
      String(result.entityStatusCode || "").trim().toLowerCase() !== "active"
    ) {
      stats.inactiveGovernmentCount += 1;
      continue;
    }

    stats.governmentMatches += 1;
    inserts.push({
      abn: result.abn,
      name: result.name || result.abn,
      category: result.entityTypeDescription || result.entityTypeCode || "Government",
    });
  }

  if (!inserts.length) {
    return stats;
  }

  const uniqueInserts = Array.from(
    new Map(inserts.map((row) => [row.abn, row])).values(),
  );

  stats.inserted = await persistNewGovernmentReferences({
    sequelize,
    govEntityModel,
    candidates: uniqueInserts,
  });

  logger.logEvent("info", "PTRS government ABN enrichment completed", {
    action: "PtrsV2GovAbnEnrichmentDone",
    customerId,
    ptrsId,
    distinctStageAbnsToCheck: stats.distinctStageAbnsToCheck,
    lookupAttempts: stats.lookupAttempts,
    lookupFailures: stats.lookupFailures,
    governmentMatches: stats.governmentMatches,
    inserted: stats.inserted,
  });

  return stats;
}

module.exports = {
  enrichGovReferenceFromStageRows,
  findUnknownGovCandidateAbns,
  persistNewGovernmentReferences,
};

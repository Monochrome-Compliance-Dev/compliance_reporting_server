const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  isValidAbn,
  lookupAbnByNumber,
  normalizeAbnDigits,
} = require("@/data_cleanse/abn-lookup.util");

const UNKNOWN_GOV_ABNS_SQL = `
  WITH distinct_stage_abns AS MATERIALIZED (
    SELECT DISTINCT
      NULLIF(
        regexp_replace(
          COALESCE(
            NULLIF(BTRIM(s."payeeEntityAbn"), ''),
            s."data"->>'payee_entity_abn',
            ''
          ),
          '\\D',
          '',
          'g'
        ),
        ''
      ) AS "abn"
    FROM "tbl_ptrs_stage_row" s
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
  ),
  existing_gov_abns AS MATERIALIZED (
    SELECT DISTINCT
      NULLIF(
        regexp_replace(COALESCE(g."abn", ''), '\\D', '', 'g'),
        ''
      ) AS "abn"
    FROM "tbl_ptrs_gov_entity_ref" g
    WHERE g."deletedAt" IS NULL
  ),
  current_negative_abr_results AS MATERIALIZED (
    SELECT cache."abn"
    FROM "tbl_ptrs_abr_lookup_cache" cache
    WHERE cache."expiresAt" > now()
  )
  SELECT d."abn"
  FROM distinct_stage_abns d
  LEFT JOIN existing_gov_abns g ON g."abn" = d."abn"
  LEFT JOIN current_negative_abr_results cache ON cache."abn" = d."abn"
  WHERE d."abn" IS NOT NULL
    AND d."abn" ~ '^[0-9]{11}$'
    AND g."abn" IS NULL
    AND cache."abn" IS NULL
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

const ABR_NEGATIVE_CACHE_TTL_MS = 24 * 60 * 60 * 1000;

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

  return Array.isArray(rows) ? rows.map((row) => row.abn).filter(Boolean) : [];
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

    const rowsToInsert = candidates.filter((row) => !existingAbns.has(row.abn));

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

async function persistNegativeAbrResults({ abrCacheModel, candidates }) {
  if (!Array.isArray(candidates) || !candidates.length) return 0;
  if (!abrCacheModel || typeof abrCacheModel.bulkCreate !== "function") {
    throw new Error("PtrsAbrLookupCache model is required");
  }

  await abrCacheModel.bulkCreate(candidates, {
    updateOnDuplicate: [
      "classification",
      "checkedAt",
      "expiresAt",
      "updatedAt",
    ],
  });
  return candidates.length;
}

async function enrichGovReferenceFromStageRows({
  sequelize = db.sequelize,
  customerId,
  ptrsId,
  lookupAbnByNumberFn = lookupAbnByNumber,
  govEntityModel = db.PtrsGovEntityRef,
  abrCacheModel = db.PtrsAbrLookupCache,
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
    invalidCandidateAbnsSkipped: 0,
    lookupFailures: 0,
    unresolvedCount: 0,
    nonGovernmentCount: 0,
    inactiveGovernmentCount: 0,
    governmentMatches: 0,
    inserted: 0,
    negativeResultsCached: 0,
  };

  if (!unknownAbns.length) {
    return stats;
  }

  if (!process.env.ABR_GUID) {
    logger.logEvent(
      "warn",
      "PTRS government ABN enrichment skipped: ABR_GUID missing",
      {
        action: "PtrsV2GovAbnEnrichmentSkipped",
        customerId,
        ptrsId,
        candidateCount: unknownAbns.length,
      },
    );
    return {
      ...stats,
      skipped: "ABR_GUID missing",
    };
  }

  const lookupResults = await mapWithConcurrency(
    unknownAbns,
    5,
    async (abn) => {
      const normalizedAbn = normalizeAbnDigits(abn);
      if (!isValidAbn(normalizedAbn)) {
        stats.invalidCandidateAbnsSkipped += 1;
        return {
          requestAbn: normalizedAbn,
          found: false,
          invalidCandidate: true,
        };
      }

      stats.lookupAttempts += 1;
      try {
        const result = await lookupAbnByNumberFn(normalizedAbn);
        if (result?.exception) {
          stats.lookupFailures += 1;
          logger.logEvent(
            "warn",
            "PTRS government ABN enrichment lookup failed",
            {
              action: "PtrsV2GovAbnLookupFailed",
              customerId,
              ptrsId,
              abn: normalizedAbn,
              error: result.exception,
            },
          );
          return { ...result, error: result.exception };
        }
        return result;
      } catch (error) {
        stats.lookupFailures += 1;
        logger.logEvent(
          "warn",
          "PTRS government ABN enrichment lookup failed",
          {
            action: "PtrsV2GovAbnLookupFailed",
            customerId,
            ptrsId,
            abn: normalizedAbn,
            error: error.message,
          },
        );
        return {
          requestAbn: normalizedAbn,
          found: false,
          error: error.message,
        };
      }
    },
  );

  const inserts = [];
  const negativeCacheRows = [];
  const checkedAt = new Date();
  const expiresAt = new Date(checkedAt.getTime() + ABR_NEGATIVE_CACHE_TTL_MS);
  for (const result of lookupResults) {
    if (result?.invalidCandidate) continue;
    if (!result?.found || !result?.abn) {
      if (!result?.error) stats.unresolvedCount += 1;
      continue;
    }

    if (!result.isGovernmentEntity) {
      stats.nonGovernmentCount += 1;
      negativeCacheRows.push({
        abn: result.abn,
        classification: "NON_GOVERNMENT",
        checkedAt,
        expiresAt,
      });
      continue;
    }

    if (
      result.isCurrentAbn !== true ||
      String(result.entityStatusCode || "")
        .trim()
        .toLowerCase() !== "active"
    ) {
      stats.inactiveGovernmentCount += 1;
      negativeCacheRows.push({
        abn: result.abn,
        classification: "INACTIVE_GOVERNMENT",
        checkedAt,
        expiresAt,
      });
      continue;
    }

    stats.governmentMatches += 1;
    inserts.push({
      abn: result.abn,
      name: result.name || result.abn,
      category:
        result.entityTypeDescription || result.entityTypeCode || "Government",
    });
  }

  stats.negativeResultsCached = await persistNegativeAbrResults({
    abrCacheModel,
    candidates: negativeCacheRows,
  });

  const uniqueInserts = Array.from(
    new Map(inserts.map((row) => [row.abn, row])).values(),
  );

  if (uniqueInserts.length) {
    stats.inserted = await persistNewGovernmentReferences({
      sequelize,
      govEntityModel,
      candidates: uniqueInserts,
    });
  }

  logger.logEvent("info", "PTRS government ABN enrichment completed", {
    action: "PtrsV2GovAbnEnrichmentDone",
    customerId,
    ptrsId,
    distinctStageAbnsToCheck: stats.distinctStageAbnsToCheck,
    lookupAttempts: stats.lookupAttempts,
    invalidCandidateAbnsSkipped: stats.invalidCandidateAbnsSkipped,
    lookupFailures: stats.lookupFailures,
    governmentMatches: stats.governmentMatches,
    inserted: stats.inserted,
    negativeResultsCached: stats.negativeResultsCached,
  });

  return stats;
}

module.exports = {
  enrichGovReferenceFromStageRows,
  findUnknownGovCandidateAbns,
  persistNegativeAbrResults,
  persistNewGovernmentReferences,
};

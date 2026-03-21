const db = require("@/db/database");
const { QueryTypes } = require("sequelize");
const { logger } = require("@/helpers/logger");
const {
  buildStableInputHash,
  getLatestExecutionRun,
} = require("./ptrs.service");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

async function buildStageInputSnapshot({
  customerId,
  ptrsId,
  profileId,
  transaction,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!profileId) throw new Error("profileId is required");

  const [
    latestSuccessfulMapRun,
    mappedRowCount,
    mappedRowMaxUpdatedAt,
    paymentTermMapUpdatedAt,
    paymentTermMapCount,
    paymentTermChangeUpdatedAt,
    paymentTermChangeCount,
  ] = await Promise.all([
    db.PtrsExecutionRun.findOne({
      where: {
        customerId,
        ptrsId,
        profileId,
        step: "map",
        status: "success",
      },
      attributes: ["id", "inputHash", "finishedAt"],
      order: [
        ["startedAt", "DESC"],
        ["id", "DESC"],
      ],
      raw: true,
      transaction,
    }),
    db.PtrsMappedRow.count({
      where: { customerId, ptrsId },
      transaction,
    }),
    db.PtrsMappedRow.max("updatedAt", {
      where: { customerId, ptrsId },
      transaction,
    }),
    (async () => {
      const rows = await db.sequelize.query(
        `
        SELECT MAX("updatedAt") AS "maxUpdatedAt"
        FROM "tbl_ptrs_payment_term_map"
        WHERE "customerId" = :customerId
          AND "profileId" = :profileId
          AND "deletedAt" IS NULL
        `,
        {
          type: QueryTypes.SELECT,
          replacements: { customerId, profileId },
          transaction,
        },
      );
      return rows && rows[0] ? rows[0].maxUpdatedAt || null : null;
    })(),
    (async () => {
      const rows = await db.sequelize.query(
        `
        SELECT COUNT(1)::int AS "count"
        FROM "tbl_ptrs_payment_term_map"
        WHERE "customerId" = :customerId
          AND "profileId" = :profileId
          AND "deletedAt" IS NULL
        `,
        {
          type: QueryTypes.SELECT,
          replacements: { customerId, profileId },
          transaction,
        },
      );
      return rows && rows[0] ? Number(rows[0].count) || 0 : 0;
    })(),
    (async () => {
      const rows = await db.sequelize.query(
        `
        SELECT MAX("updatedAt") AS "maxUpdatedAt"
        FROM "tbl_ptrs_payment_term_change"
        WHERE "customerId" = :customerId
          AND "profileId" = :profileId
          AND "deletedAt" IS NULL
        `,
        {
          type: QueryTypes.SELECT,
          replacements: { customerId, profileId },
          transaction,
        },
      );
      return rows && rows[0] ? rows[0].maxUpdatedAt || null : null;
    })(),
    (async () => {
      const rows = await db.sequelize.query(
        `
        SELECT COUNT(1)::int AS "count"
        FROM "tbl_ptrs_payment_term_change"
        WHERE "customerId" = :customerId
          AND "profileId" = :profileId
          AND "deletedAt" IS NULL
        `,
        {
          type: QueryTypes.SELECT,
          replacements: { customerId, profileId },
          transaction,
        },
      );
      return rows && rows[0] ? Number(rows[0].count) || 0 : 0;
    })(),
  ]);

  return {
    ptrsId,
    customerId,
    profileId: profileId || null,
    latestSuccessfulMapRun: {
      id: latestSuccessfulMapRun?.id || null,
      inputHash: latestSuccessfulMapRun?.inputHash || null,
      finishedAt: latestSuccessfulMapRun?.finishedAt || null,
    },
    mappedRows: {
      rowCount: Number(mappedRowCount) || 0,
      maxUpdatedAt: mappedRowMaxUpdatedAt || null,
    },
    paymentTermMap: {
      profileId: profileId || null,
      count: Number(paymentTermMapCount) || 0,
      maxUpdatedAt: paymentTermMapUpdatedAt || null,
    },
    paymentTermChanges: {
      profileId: profileId || null,
      count: Number(paymentTermChangeCount) || 0,
      maxUpdatedAt: paymentTermChangeUpdatedAt || null,
    },
  };
}

async function getStageStaleness({
  customerId,
  ptrsId,
  profileId,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!profileId) throw new Error("profileId is required");

  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const isExternalTx = !!transaction;

  try {
    const snapshot = await buildStageInputSnapshot({
      customerId,
      ptrsId,
      profileId,
      transaction: t,
    });

    const inputHash = buildStableInputHash(snapshot);
    const previous = await getLatestExecutionRun({
      customerId,
      ptrsId,
      step: "stage",
      transaction: t,
    });

    const existingStageCount = await db.PtrsStageRow.count({
      where: { customerId, ptrsId, deletedAt: null },
      transaction: t,
    });

    const previousHash = previous?.inputHash || null;
    const hasChanged = !previousHash || previousHash !== inputHash;

    const result = {
      step: "stage",
      inputHash,
      previousHash,
      hasChanged,
      previousRunId: previous?.id || null,
      existingStageCount: Number(existingStageCount) || 0,
      snapshot,
    };

    logger.info("PTRS v2 stage staleness evaluated", {
      action: "PtrsV2StageStalenessEvaluated",
      customerId,
      ptrsId,
      profileId,
      inputHash,
      previousHash,
      hasChanged,
      previousRunId: previous?.id || null,
      existingStageCount: Number(existingStageCount) || 0,
      latestSuccessfulMapRunId: snapshot?.latestSuccessfulMapRun?.id || null,
      latestSuccessfulMapInputHash:
        snapshot?.latestSuccessfulMapRun?.inputHash || null,
      latestSuccessfulMapFinishedAt:
        snapshot?.latestSuccessfulMapRun?.finishedAt || null,
      mappedRowCount: Number(snapshot?.mappedRows?.rowCount) || 0,
      mappedRowMaxUpdatedAt: snapshot?.mappedRows?.maxUpdatedAt || null,
      paymentTermMapCount: Number(snapshot?.paymentTermMap?.count) || 0,
      paymentTermMapMaxUpdatedAt:
        snapshot?.paymentTermMap?.maxUpdatedAt || null,
      paymentTermChangeCount: Number(snapshot?.paymentTermChanges?.count) || 0,
      paymentTermChangeMaxUpdatedAt:
        snapshot?.paymentTermChanges?.maxUpdatedAt || null,
    });

    if (!isExternalTx && !t.finished) {
      await t.commit();
    }

    return result;
  } catch (err) {
    if (!isExternalTx && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function getStageCompletionGate({
  customerId,
  ptrsId,
  profileId,
  transaction = null,
}) {
  const staleness = await getStageStaleness({
    customerId,
    ptrsId,
    profileId,
    transaction,
  });

  const hasRows = Number(staleness?.existingStageCount || 0) > 0;
  const stale = !!staleness?.hasChanged;

  const gateMeta = {
    action: "PtrsV2StageCompletionGateEvaluated",
    customerId,
    ptrsId,
    profileId,
    hasRows,
    stale,
    existingStageCount: Number(staleness?.existingStageCount || 0),
    previousRunId: staleness?.previousRunId || null,
    inputHash: staleness?.inputHash || null,
    previousHash: staleness?.previousHash || null,
  };

  if (!hasRows) {
    logger.info("PTRS v2 stage completion gate: missing stage", {
      ...gateMeta,
      ready: false,
      reason: "missing-stage",
    });
    return {
      ready: false,
      reason: "missing-stage",
      hasRows: false,
      stale,
      existingStageCount: Number(staleness?.existingStageCount || 0),
      previousRunId: staleness?.previousRunId || null,
      inputHash: staleness?.inputHash || null,
      previousHash: staleness?.previousHash || null,
      snapshot: staleness?.snapshot || null,
    };
  }

  if (stale) {
    logger.info("PTRS v2 stage completion gate: stale", {
      ...gateMeta,
      ready: false,
      reason: "stale",
    });
    return {
      ready: false,
      reason: "stale",
      hasRows: true,
      stale: true,
      existingStageCount: Number(staleness?.existingStageCount || 0),
      previousRunId: staleness?.previousRunId || null,
      inputHash: staleness?.inputHash || null,
      previousHash: staleness?.previousHash || null,
      snapshot: staleness?.snapshot || null,
    };
  }

  logger.info("PTRS v2 stage completion gate: ready", {
    ...gateMeta,
    ready: true,
    reason: "ready",
  });
  return {
    ready: true,
    reason: "ready",
    hasRows: true,
    stale: false,
    existingStageCount: Number(staleness?.existingStageCount || 0),
    previousRunId: staleness?.previousRunId || null,
    inputHash: staleness?.inputHash || null,
    previousHash: staleness?.previousHash || null,
    snapshot: staleness?.snapshot || null,
  };
}

module.exports = {
  buildStageInputSnapshot,
  getStageStaleness,
  getStageCompletionGate,
};

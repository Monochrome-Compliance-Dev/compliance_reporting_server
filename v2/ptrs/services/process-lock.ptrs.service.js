const db = require("@/db/database");
const { logger } = require("@/helpers/logger");

const PROCESS_EXECUTION_LOCK_PREFIX = "ptrs:process";

function buildProcessExecutionLockKey({ customerId, ptrsId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  return `${PROCESS_EXECUTION_LOCK_PREFIX}:${customerId}:${ptrsId}`;
}

async function acquireProcessExecutionLock({ customerId, ptrsId }) {
  const lockKey = buildProcessExecutionLockKey({ customerId, ptrsId });
  const client = await db.getPgPool().connect();

  try {
    const result = await client.query(
      "SELECT pg_try_advisory_lock(hashtextextended($1, 0)) AS acquired",
      [lockKey],
    );
    if (result.rows?.[0]?.acquired !== true) {
      client.release();
      return null;
    }
  } catch (error) {
    client.release(error);
    throw error;
  }

  let released = false;
  return {
    lockKey,
    async release() {
      if (released) return;
      released = true;
      let releaseError = null;
      try {
        const result = await client.query(
          "SELECT pg_advisory_unlock(hashtextextended($1, 0)) AS released",
          [lockKey],
        );
        if (result.rows?.[0]?.released !== true) {
          logger.warn("PTRS process execution advisory lock was not owned", {
            customerId,
            ptrsId,
          });
        }
      } catch (error) {
        releaseError = error;
        logger.error("Failed to release PTRS process execution advisory lock", {
          customerId,
          ptrsId,
          error: error.message,
        });
      } finally {
        client.release(releaseError || undefined);
      }
    },
  };
}

async function tryAcquireProcessExecutionReconciliationLock({
  customerId,
  ptrsId,
  transaction,
}) {
  if (!transaction) throw new Error("transaction is required");
  const lockKey = buildProcessExecutionLockKey({ customerId, ptrsId });
  const rows = await db.sequelize.query(
    `
    SELECT pg_try_advisory_xact_lock(
      hashtextextended(:processExecutionLockKey, 0)
    ) AS acquired
    `,
    {
      replacements: { processExecutionLockKey: lockKey },
      type: db.sequelize.QueryTypes.SELECT,
      transaction,
    },
  );
  return rows?.[0]?.acquired === true;
}

module.exports = {
  PROCESS_EXECUTION_LOCK_PREFIX,
  acquireProcessExecutionLock,
  buildProcessExecutionLockKey,
  tryAcquireProcessExecutionReconciliationLock,
};

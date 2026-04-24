const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

async function getExclusionsSummary({ customerId, ptrsId, profileId = null }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const sequelize = db?.sequelize;
  if (!sequelize) {
    throw new Error("Database not initialised: db.sequelize missing");
  }

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const totalSql = `
      SELECT COUNT(*)::int AS "totalExcludedRows"
      FROM "tbl_ptrs_stage_row" s
      WHERE
        s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
        AND s."excludedTradeCreditPayment" = true
    `;

    const multiReasonSql = `
      SELECT 0::int AS "multiReasonRows"
    `;

    const byReasonSql = `
      SELECT
        COALESCE(NULLIF(trim(s."excludeReason"), ''), 'UNKNOWN') AS reason,
        COUNT(*)::int AS count
      FROM "tbl_ptrs_stage_row" s
      WHERE
        s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
        AND s."excludedTradeCreditPayment" = true
      GROUP BY COALESCE(NULLIF(trim(s."excludeReason"), ''), 'UNKNOWN')
      ORDER BY COUNT(*) DESC, reason ASC
    `;

    const [totalRows] = await sequelize.query(totalSql, {
      replacements: { customerId, ptrsId, profileId },
      transaction: t,
    });

    const [multiReasonRows] = await sequelize.query(multiReasonSql, {
      replacements: { customerId, ptrsId, profileId },
      transaction: t,
    });

    const [reasonRows] = await sequelize.query(byReasonSql, {
      replacements: { customerId, ptrsId, profileId },
      transaction: t,
    });

    await t.commit();

    const reasonCounts = Array.isArray(reasonRows)
      ? reasonRows.reduce((acc, row) => {
          acc[row.reason] = Number(row.count || 0);
          return acc;
        }, {})
      : {};

    return {
      totalExcludedRows: Number(totalRows?.[0]?.totalExcludedRows || 0),
      multiReasonRows: Number(multiReasonRows?.[0]?.multiReasonRows || 0),
      byReason: reasonCounts,
    };
  } catch (error) {
    await t.rollback();
    throw error;
  }
}

module.exports = {
  getExclusionsSummary,
};

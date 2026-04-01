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
        AND COALESCE((s."data"->>'exclude')::boolean, false) = true
    `;

    const multiReasonSql = `
      SELECT COUNT(*)::int AS "multiReasonRows"
      FROM "tbl_ptrs_stage_row" s
      WHERE
        s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
        AND COALESCE((s."data"->>'exclude')::boolean, false) = true
        AND (
          CASE
            WHEN jsonb_typeof(COALESCE(s."data"->'exclude_reasons', '[]'::jsonb)) = 'array'
              THEN jsonb_array_length(COALESCE(s."data"->'exclude_reasons', '[]'::jsonb))
            WHEN trim(COALESCE(s."data"->>'exclude_reason', '')) <> ''
              THEN 1
            ELSE 0
          END
        ) > 1
    `;

    const byReasonSql = `
      WITH base_rows AS (
        SELECT s."data"
        FROM "tbl_ptrs_stage_row" s
        WHERE
          s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
          AND COALESCE((s."data"->>'exclude')::boolean, false) = true
      ),
      reason_rows AS (
        SELECT jsonb_array_elements_text(b."data"->'exclude_reasons') AS reason
        FROM base_rows b
        WHERE jsonb_typeof(COALESCE(b."data"->'exclude_reasons', '[]'::jsonb)) = 'array'

        UNION ALL

        SELECT b."data"->>'exclude_reason' AS reason
        FROM base_rows b
        WHERE (
          jsonb_typeof(COALESCE(b."data"->'exclude_reasons', '[]'::jsonb)) <> 'array'
          OR jsonb_array_length(COALESCE(b."data"->'exclude_reasons', '[]'::jsonb)) = 0
        )
        AND trim(COALESCE(b."data"->>'exclude_reason', '')) <> ''
      )
      SELECT
        COALESCE(reason, 'UNKNOWN') AS reason,
        COUNT(*)::int AS count
      FROM reason_rows
      GROUP BY COALESCE(reason, 'UNKNOWN')
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

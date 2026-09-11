const {
  appendJsonbTextArray,
  appendJsonbTextArrayAtPath,
  applyExcludeFlags,
  applyMetaBase,
} = require("./exclusions.shared");

const REASON_SQL = "'INTRA_GROUP'";
const INTRA_GROUP_SUPPLIER_PREFIXES = Object.freeze(["S", "106"]);
const COMMENT_SQL =
  "'Intra-group payment — Veolia supplier number begins S or 106'";
const SOURCE_ACCOUNT_SQL = `UPPER(BTRIM(COALESCE(
  s."sourceAccountCode", s."data"->>'source_account_code', ''
)))`;
const MATCH_SQL = `(${INTRA_GROUP_SUPPLIER_PREFIXES.map(
  (prefix) => `${SOURCE_ACCOUNT_SQL} LIKE '${prefix}%'`,
).join(" OR ")})`;

function isIntraGroupSupplier(sourceAccountCode) {
  const account = String(sourceAccountCode || "").trim().toUpperCase();
  return INTRA_GROUP_SUPPLIER_PREFIXES.some((prefix) =>
    account.startsWith(prefix),
  );
}

function buildIntraGroupExclusionSql() {
  const dataSql = appendJsonbTextArray(
    "exclude_comment",
    COMMENT_SQL,
    appendJsonbTextArray(
      "exclude_reasons",
      REASON_SQL,
      applyExcludeFlags('s."data"', REASON_SQL),
    ),
  );
  const metaSql = appendJsonbTextArrayAtPath(
    "exclusions,comments",
    COMMENT_SQL,
    appendJsonbTextArrayAtPath(
      "exclusions,reasons",
      REASON_SQL,
      applyMetaBase('s."meta"'),
    ),
  );
  return `
    UPDATE "tbl_ptrs_stage_row" s
    SET "data" = ${dataSql}, "meta" = ${metaSql}, "updatedAt" = now()
    WHERE s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND ${MATCH_SQL}
      AND NOT COALESCE(s."data"->'exclude_reasons', '[]'::jsonb)
        @> jsonb_build_array('INTRA_GROUP'::text)
  `;
}

async function applyIntraCompanyExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
}) {
  const [, metadata] = await sequelize.query(buildIntraGroupExclusionSql(), {
    replacements: { customerId, ptrsId },
    transaction,
  });
  return Number(metadata?.rowCount || 0);
}

async function previewIntraCompanyExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  effectiveLimit,
}) {
  const [countRows] = await sequelize.query(
    `SELECT COUNT(*)::int AS "matchedCount",
       COUNT(*) FILTER (WHERE COALESCE(s."data"->'exclude_reasons', '[]'::jsonb)
         @> jsonb_build_array('INTRA_GROUP'::text))::int AS "alreadyExcludedCount"
     FROM "tbl_ptrs_stage_row" s
     WHERE s."customerId" = :customerId AND s."ptrsId" = :ptrsId
       AND s."deletedAt" IS NULL AND ${MATCH_SQL}`,
    { replacements: { customerId, ptrsId }, transaction },
  );
  const [sampleRows] = await sequelize.query(
    `SELECT s."rowNo" AS "row_no", s."sourceAccountCode" AS "source_account_code",
       s."payeeEntityName" AS "payee_entity_name", s."payeeEntityAbn" AS "payee_entity_abn",
       s."paymentAmount" AS "payment_amount", ${COMMENT_SQL} AS "exclude_comment",
       COALESCE(s."data"->'exclude_reasons', '[]'::jsonb)
         @> jsonb_build_array('INTRA_GROUP'::text) AS "alreadyExcluded"
     FROM "tbl_ptrs_stage_row" s
     WHERE s."customerId" = :customerId AND s."ptrsId" = :ptrsId
       AND s."deletedAt" IS NULL AND ${MATCH_SQL}
     ORDER BY s."rowNo", s."id" LIMIT :limit`,
    {
      replacements: { customerId, ptrsId, limit: effectiveLimit },
      transaction,
    },
  );
  return {
    matched: Number(countRows?.[0]?.matchedCount || 0),
    alreadyExcluded: Number(countRows?.[0]?.alreadyExcludedCount || 0),
    sampleRows: Array.isArray(sampleRows) ? sampleRows : [],
  };
}

module.exports = {
  applyIntraCompanyExclusion,
  buildIntraGroupExclusionSql,
  isIntraGroupSupplier,
  previewIntraCompanyExclusion,
};

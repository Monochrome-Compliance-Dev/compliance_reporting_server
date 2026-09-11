const {
  appendJsonbTextArray,
  appendJsonbTextArrayAtPath,
  applyExcludeFlags,
  applyMetaBase,
} = require("./exclusions.shared");

const REASON_SQL = "'EMPLOYEE_PAYMENT'";
const EMPLOYEE_SUPPLIER_PREFIX = "103";
const EMPLOYEE_DOCUMENT_TYPES = Object.freeze(["K1", "EZ"]);
const COMMENT_SQL =
  "'Employee payment — Veolia supplier number begins 103 or SAP document type is K1/EZ'";
const SOURCE_ACCOUNT_SQL = `UPPER(BTRIM(COALESCE(
  s."sourceAccountCode", s."data"->>'source_account_code', ''
)))`;
const DOCUMENT_TYPE_SQL = `UPPER(BTRIM(COALESCE(
  s."documentType", s."data"->>'document_type', ''
)))`;
const MATCH_SQL = `(${SOURCE_ACCOUNT_SQL} LIKE '${EMPLOYEE_SUPPLIER_PREFIX}%'
  OR ${DOCUMENT_TYPE_SQL} IN (${EMPLOYEE_DOCUMENT_TYPES.map((type) => `'${type}'`).join(", ")}))`;

function isEmployeePayment({ sourceAccountCode, documentType } = {}) {
  const account = String(sourceAccountCode || "").trim().toUpperCase();
  const document = String(documentType || "").trim().toUpperCase();
  return (
    account.startsWith(EMPLOYEE_SUPPLIER_PREFIX) ||
    EMPLOYEE_DOCUMENT_TYPES.includes(document)
  );
}

function buildEmployeeExclusionSql() {
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
        @> jsonb_build_array('EMPLOYEE_PAYMENT'::text)
  `;
}

async function applyEmployeeExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
}) {
  const [, metadata] = await sequelize.query(buildEmployeeExclusionSql(), {
    replacements: { customerId, ptrsId },
    transaction,
  });
  return Number(metadata?.rowCount || 0);
}

async function previewEmployeeExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  effectiveLimit,
}) {
  const [countRows] = await sequelize.query(
    `SELECT COUNT(*)::int AS "matchedCount",
       COUNT(*) FILTER (WHERE COALESCE(s."data"->'exclude_reasons', '[]'::jsonb)
         @> jsonb_build_array('EMPLOYEE_PAYMENT'::text))::int AS "alreadyExcludedCount"
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
         @> jsonb_build_array('EMPLOYEE_PAYMENT'::text) AS "alreadyExcluded"
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
  applyEmployeeExclusion,
  buildEmployeeExclusionSql,
  isEmployeePayment,
  previewEmployeeExclusion,
};

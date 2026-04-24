const { QueryTypes } = require("sequelize");

const EXCLUSION_REASON = "PAYMENT_TERMS";
const EXCLUSION_COMMENT =
  "Excluded due to immediate/non-trade-credit payment terms (0 days).";

function buildPaymentTermsMatchSql() {
  return `
    (
      s."paymentTermDays" = 0
      OR trim(COALESCE(s."paymentTermRaw", '')) = '0'
    )
  `;
}

async function applyPaymentTermsExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
}) {
  if (!sequelize) throw new Error("sequelize is required");
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const matchSql = buildPaymentTermsMatchSql();

  const sql = `
    WITH matched_rows AS (
      SELECT
        s."id",
        :comment::text AS "excludeComment"
      FROM "tbl_ptrs_stage_row" s
      WHERE s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
        AND ${matchSql}
        AND COALESCE(s."excludeReason", '') <> :reason
    )
    UPDATE "tbl_ptrs_stage_row" s
    SET
      "excludedTradeCreditPayment" = true,
      "excludeReason" = :reason,
      "updatedAt" = now()
    FROM matched_rows mr
    WHERE s."id" = mr."id"
  `;

  const [, meta] = await sequelize.query(sql, {
    replacements: {
      customerId,
      ptrsId,
      reason: EXCLUSION_REASON,
      comment: EXCLUSION_COMMENT,
    },
    transaction,
  });

  return Number(meta?.rowCount || 0);
}

async function previewPaymentTermsExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  effectiveLimit = 10,
}) {
  if (!sequelize) throw new Error("sequelize is required");
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const matchSql = buildPaymentTermsMatchSql();

  const matchedRowsCte = `
    WITH matched_rows AS (
      SELECT
        s."id",
        s."rowNo",
        s."payeeEntityName",
        s."payeeEntityAbn",
        s."invoiceReferenceNumber",
        s."sourceAccountCode",
        s."paymentDate",
        s."paymentAmount",
        s."paymentTermRaw",
        s."paymentTermDays",
        s."excludedTradeCreditPayment",
        s."excludeReason",
        :comment::text AS "excludeComment"
      FROM "tbl_ptrs_stage_row" s
      WHERE s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
        AND ${matchSql}
    )
  `;

  const countSql = `
    ${matchedRowsCte}
    SELECT
      COUNT(*)::int AS matched,
      COUNT(*) FILTER (
        WHERE COALESCE("excludeReason", '') = :reason
      )::int AS "alreadyExcluded"
    FROM matched_rows
  `;

  const sampleSql = `
    ${matchedRowsCte}
    SELECT
      "rowNo" AS row_no,
      "payeeEntityName" AS payee_entity_name,
      "payeeEntityAbn" AS payee_entity_abn,
      "invoiceReferenceNumber" AS invoice_reference_number,
      "sourceAccountCode" AS account_code,
      CASE
        WHEN "paymentDate" IS NOT NULL THEN "paymentDate"::text
        ELSE NULL
      END AS payment_date,
      CASE
        WHEN "paymentAmount" IS NOT NULL THEN "paymentAmount"::text
        ELSE NULL
      END AS payment_amount,
      "paymentTermRaw" AS payment_term_raw,
      "paymentTermDays" AS payment_term_days,
      CASE
        WHEN COALESCE("excludeReason", '') = :reason THEN true
        ELSE false
      END AS "alreadyExcluded",
      "excludeComment" AS exclude_comment
    FROM matched_rows
    ORDER BY "rowNo" ASC
    LIMIT :limit
  `;

  const [countRows, sampleRows] = await Promise.all([
    sequelize.query(countSql, {
      type: QueryTypes.SELECT,
      replacements: {
        customerId,
        ptrsId,
        reason: EXCLUSION_REASON,
        comment: EXCLUSION_COMMENT,
      },
      transaction,
    }),
    sequelize.query(sampleSql, {
      type: QueryTypes.SELECT,
      replacements: {
        customerId,
        ptrsId,
        limit: Number(effectiveLimit) || 10,
        reason: EXCLUSION_REASON,
        comment: EXCLUSION_COMMENT,
      },
      transaction,
    }),
  ]);

  return {
    matched: Number(countRows?.[0]?.matched || 0),
    alreadyExcluded: Number(countRows?.[0]?.alreadyExcluded || 0),
    sampleRows: Array.isArray(sampleRows) ? sampleRows : [],
  };
}

module.exports = {
  applyPaymentTermsExclusion,
  previewPaymentTermsExclusion,
};

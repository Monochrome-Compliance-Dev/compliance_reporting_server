const { QueryTypes } = require("sequelize");

const EXCLUSION_REASON = "PREPAID";
const EXCLUSION_COMMENT = "Prepayment — matched payment terms.";

function buildPrepaidMatchSql() {
  return `(
    trim(COALESCE(s."paymentTermRaw", '')) ILIKE '%prepaid%'
    OR trim(COALESCE(s."paymentTermRaw", '')) ILIKE '%pre-pay%'
    OR trim(COALESCE(s."paymentTermRaw", '')) ILIKE '%prepay%'
  )`;
}

async function applyPrepaidExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
}) {
  const matchSql = buildPrepaidMatchSql();

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

  return Number(meta?.rowCount ?? 0) || 0;
}

async function previewPrepaidExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  effectiveLimit,
}) {
  const matchSql = buildPrepaidMatchSql();

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
      COUNT(*)::int AS "matchedCount",
      COUNT(*) FILTER (
        WHERE COALESCE("excludeReason", '') = :reason
      )::int AS "alreadyExcludedCount"
    FROM matched_rows
  `;

  const [countRows] = await sequelize.query(countSql, {
    type: QueryTypes.SELECT,
    replacements: {
      customerId,
      ptrsId,
      reason: EXCLUSION_REASON,
      comment: EXCLUSION_COMMENT,
    },
    transaction,
  });

  const matched = Number(countRows?.matchedCount ?? 0) || 0;
  const alreadyExcluded = Number(countRows?.alreadyExcludedCount ?? 0) || 0;

  const sampleSql = `
    ${matchedRowsCte}
    SELECT
      "rowNo" AS "row_no",
      "payeeEntityName" AS "payee_entity_name",
      "payeeEntityAbn" AS "payee_entity_abn",
      "invoiceReferenceNumber" AS "invoice_reference_number",
      "sourceAccountCode" AS "account_code",
      CASE
        WHEN "paymentDate" IS NOT NULL THEN "paymentDate"::text
        ELSE NULL
      END AS "payment_date",
      CASE
        WHEN "paymentAmount" IS NOT NULL THEN "paymentAmount"::text
        ELSE NULL
      END AS "payment_amount",
      "paymentTermRaw" AS "payment_term_raw",
      "paymentTermDays" AS "payment_term_days",
      CASE
        WHEN COALESCE("excludeReason", '') = :reason THEN true
        ELSE false
      END AS "alreadyExcluded",
      "excludeComment" AS "exclude_comment"
    FROM matched_rows
    ORDER BY "rowNo" ASC
    LIMIT :limit
  `;

  const sampleRows = await sequelize.query(sampleSql, {
    type: QueryTypes.SELECT,
    replacements: {
      customerId,
      ptrsId,
      limit: effectiveLimit,
      reason: EXCLUSION_REASON,
      comment: EXCLUSION_COMMENT,
    },
    transaction,
  });

  return {
    matched,
    alreadyExcluded,
    sampleRows: Array.isArray(sampleRows) ? sampleRows : [],
  };
}

module.exports = {
  applyPrepaidExclusion,
  previewPrepaidExclusion,
};

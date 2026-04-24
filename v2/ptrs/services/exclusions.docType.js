async function applyDocTypeExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
}) {
  const reason = "DOC_TYPE";

  const sql = `
    WITH matched_rows AS (
      SELECT
        s."id",
        CASE
          WHEN s."documentType" = 'K1'
            THEN 'Document type exclusion — K1 document type'
          WHEN s."clearingDocument" LIKE '2000%'
            THEN 'Document type exclusion — clearing document begins 2000'
          WHEN s."documentType" IN ('Z', 'KZ', 'AB')
            AND s."clearingDocument" LIKE '5%'
            THEN 'Document type exclusion — internal/adjustment document type with clearing document beginning 5'
          ELSE 'Document type exclusion'
        END AS "excludeComment"
      FROM "tbl_ptrs_stage_row" s
      WHERE s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
        AND (
          s."documentType" = 'K1'
          OR s."clearingDocument" LIKE '2000%'
          OR (
            s."documentType" IN ('Z', 'KZ', 'AB')
            AND s."clearingDocument" LIKE '5%'
          )
        )
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
    replacements: { customerId, ptrsId, reason },
    transaction,
  });

  return Number(meta?.rowCount ?? 0) || 0;
}

async function previewDocTypeExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  effectiveLimit,
}) {
  const reason = "DOC_TYPE";

  const matchedRowsCte = `
    WITH matched_rows AS (
      SELECT
        s."id",
        s."rowNo",
        s."payerEntityAbn",
        s."payerEntityName",
        s."payeeEntityAbn",
        s."payeeEntityName",
        s."invoiceReferenceNumber",
        s."sourceAccountCode",
        s."paymentDate",
        s."paymentAmount",
        s."documentType",
        s."clearingDocument",
        s."excludedTradeCreditPayment",
        s."excludeReason",
        CASE
          WHEN s."documentType" = 'K1'
            THEN 'Document type exclusion — K1 document type'
          WHEN s."clearingDocument" LIKE '2000%'
            THEN 'Document type exclusion — clearing document begins 2000'
          WHEN s."documentType" IN ('Z', 'KZ', 'AB')
            AND s."clearingDocument" LIKE '5%'
            THEN 'Document type exclusion — internal/adjustment document type with clearing document beginning 5'
          ELSE 'Document type exclusion'
        END AS "excludeComment"
      FROM "tbl_ptrs_stage_row" s
      WHERE s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
        AND (
          s."documentType" = 'K1'
          OR s."clearingDocument" LIKE '2000%'
          OR (
            s."documentType" IN ('Z', 'KZ', 'AB')
            AND s."clearingDocument" LIKE '5%'
          )
        )
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
    replacements: { customerId, ptrsId, reason },
    transaction,
  });

  const matched = Number(countRows?.[0]?.matchedCount ?? 0) || 0;
  const alreadyExcluded =
    Number(countRows?.[0]?.alreadyExcludedCount ?? 0) || 0;

  const sampleSql = `
    ${matchedRowsCte}
    SELECT
      "rowNo" AS "row_no",
      "payerEntityAbn" AS "payer_entity_abn",
      "payerEntityName" AS "payer_entity_name",
      "payeeEntityAbn" AS "payee_entity_abn",
      "payeeEntityName" AS "payee_entity_name",
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
      "excludeComment" AS "exclude_comment",
      CASE
        WHEN COALESCE("excludeReason", '') = :reason THEN true
        ELSE false
      END AS "alreadyExcluded"
    FROM matched_rows
    ORDER BY "rowNo" ASC
    LIMIT :limit
  `;

  const [sampleRows] = await sequelize.query(sampleSql, {
    replacements: { customerId, ptrsId, reason, limit: effectiveLimit },
    transaction,
  });

  return {
    matched,
    alreadyExcluded,
    sampleRows: Array.isArray(sampleRows) ? sampleRows : [],
  };
}

module.exports = {
  applyDocTypeExclusion,
  previewDocTypeExclusion,
};

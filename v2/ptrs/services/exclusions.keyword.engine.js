const { buildKeywordMatchCondition } = require("./exclusions.shared");

async function applyKeywordExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  profileId,
}) {
  if (!profileId) throw new Error("profileId is required for keyword");

  const keywordMatchCondition = buildKeywordMatchCondition({
    stageAlias: "s",
    keywordAlias: "k",
  });
  const reason = "KEYWORD";

  const sql = `
    WITH matched_rows AS (
      SELECT
        s."id",
        ('Keyword exclusion — ' || k."keyword")::text AS "excludeComment",
        k."keyword" AS "matchedKeyword"
      FROM "tbl_ptrs_stage_row" s
      JOIN "tbl_ptrs_exclusion_keyword_customer_ref" k
        ON k."customerId" = :customerId
       AND k."profileId" = :profileId
       AND k."deletedAt" IS NULL
       AND (
         ${keywordMatchCondition}
       )
      WHERE s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
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
    replacements: { customerId, ptrsId, profileId, reason },
    transaction,
  });

  return Number(meta?.rowCount ?? 0) || 0;
}

async function previewKeywordExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  profileId,
  effectiveLimit,
}) {
  if (!profileId) throw new Error("profileId is required for keyword");

  const keywordMatchCondition = buildKeywordMatchCondition({
    stageAlias: "s",
    keywordAlias: "k",
  });
  const reason = "KEYWORD";

  const matchedRowsCte = `
    WITH matched_rows AS (
      SELECT
        s."id",
        s."rowNo",
        s."payeeEntityAbn",
        s."payeeEntityName",
        s."invoiceReferenceNumber",
        s."sourceAccountCode",
        s."paymentDate",
        s."paymentAmount",
        s."excludedTradeCreditPayment",
        s."excludeReason",
        ('Keyword exclusion — ' || k."keyword")::text AS "excludeComment",
        k."keyword" AS "matchedKeyword"
      FROM "tbl_ptrs_stage_row" s
      JOIN "tbl_ptrs_exclusion_keyword_customer_ref" k
        ON k."customerId" = :customerId
       AND k."profileId" = :profileId
       AND k."deletedAt" IS NULL
       AND (
         ${keywordMatchCondition}
       )
      WHERE s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
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
    replacements: { customerId, ptrsId, profileId, reason },
    transaction,
  });

  const matched = Number(countRows?.[0]?.matchedCount ?? 0) || 0;
  const alreadyExcluded =
    Number(countRows?.[0]?.alreadyExcludedCount ?? 0) || 0;

  const sampleSql = `
    ${matchedRowsCte}
    SELECT
      "rowNo" AS "row_no",
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
      "matchedKeyword" AS "matched_keyword",
      CASE
        WHEN COALESCE("excludeReason", '') = :reason THEN true
        ELSE false
      END AS "alreadyExcluded"
    FROM matched_rows
    ORDER BY "rowNo" ASC
    LIMIT :limit
  `;

  const [sampleRows] = await sequelize.query(sampleSql, {
    replacements: {
      customerId,
      ptrsId,
      profileId,
      reason,
      limit: effectiveLimit,
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
  applyKeywordExclusion,
  previewKeywordExclusion,
};

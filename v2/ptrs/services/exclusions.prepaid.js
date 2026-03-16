const {
  jsonText,
  appendJsonbTextArray,
  appendJsonbTextArrayAtPath,
  applyExcludeFlags,
  applyMetaBase,
} = require("./exclusions.shared");

async function applyPrepaidExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
}) {
  const reasonSql = `'PREPAID'`;
  const commentSql = `'Prepayment — matched payment terms / description'`;
  const paymentTermsExpr = jsonText("s", "payment_terms");
  const descriptionExpr = jsonText("s", "description");

  const dataBaseSql = applyExcludeFlags(`s."data"`, reasonSql);
  const dataWithReasonsSql = appendJsonbTextArray(
    "exclude_reasons",
    reasonSql,
    dataBaseSql,
  );
  const dataFinalSql = appendJsonbTextArray(
    "exclude_comment",
    commentSql,
    dataWithReasonsSql,
  );

  const metaBaseSql = applyMetaBase(`s."meta"`);
  const metaWithReasonSql = `
    jsonb_set(
      ${metaBaseSql},
      '{exclusions,reason}',
      CASE
        WHEN trim(COALESCE(${metaBaseSql}#>>'{exclusions,reason}', '')) <> ''
          THEN to_jsonb(${metaBaseSql}#>>'{exclusions,reason}')
        ELSE to_jsonb((${reasonSql})::text)
      END,
      true
    )
  `;
  const metaWithReasonsSql = appendJsonbTextArrayAtPath(
    "exclusions,reasons",
    reasonSql,
    metaWithReasonSql,
  );
  const metaFinalSql = appendJsonbTextArrayAtPath(
    "exclusions,comments",
    commentSql,
    metaWithReasonsSql,
  );

  const sql = `
    UPDATE "tbl_ptrs_stage_row" s
    SET
      "data" = ${dataFinalSql},
      "meta" = ${metaFinalSql},
      "updatedAt" = now()
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND (
        ${paymentTermsExpr} ILIKE '%prepaid%'
        OR ${paymentTermsExpr} ILIKE '%pre-pay%'
        OR ${paymentTermsExpr} ILIKE '%prepay%'
        OR ${descriptionExpr} ILIKE '%prepaid%'
        OR ${descriptionExpr} ILIKE '%pre-pay%'
        OR ${descriptionExpr} ILIKE '%prepay%'
      )
      AND NOT (
        COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('PREPAID'::text)
        OR COALESCE(s."data"->>'exclude_reason', '') = 'PREPAID'
      )
  `;

  const [, meta] = await sequelize.query(sql, {
    replacements: { customerId, ptrsId },
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
  const countSql = `
    SELECT
      COUNT(*)::int AS "matchedCount",
      SUM(
        CASE
          WHEN (
            COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('PREPAID'::text)
            OR COALESCE(s."data"->>'exclude_reason', '') = 'PREPAID'
          )
          THEN 1 ELSE 0
        END
      )::int AS "alreadyExcludedCount"
    FROM "tbl_ptrs_stage_row" s
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND (
        COALESCE(s."data"->>'payment_terms','') ILIKE '%prepaid%'
        OR COALESCE(s."data"->>'payment_terms','') ILIKE '%pre-pay%'
        OR COALESCE(s."data"->>'payment_terms','') ILIKE '%prepay%'
        OR COALESCE(s."data"->>'description','') ILIKE '%prepaid%'
        OR COALESCE(s."data"->>'description','') ILIKE '%pre-pay%'
        OR COALESCE(s."data"->>'description','') ILIKE '%prepay%'
      )
  `;

  const [countRows] = await sequelize.query(countSql, {
    replacements: { customerId, ptrsId },
    transaction,
  });

  const matched = Number(countRows?.[0]?.matchedCount ?? 0) || 0;
  const alreadyExcluded =
    Number(countRows?.[0]?.alreadyExcludedCount ?? 0) || 0;

  const sampleSql = `
    SELECT
      s."rowNo" AS "rowNo",
      s."data"->>'payee_entity_name' AS "payee_entity_name",
      s."data"->>'payee_entity_abn' AS "payee_entity_abn",
      s."data"->>'invoice_reference_number' AS "invoice_reference_number",
      s."data"->>'account_code' AS "account_code",
      s."data"->>'payment_date' AS "payment_date",
      s."data"->>'payment_amount' AS "payment_amount",
      s."data"->>'payment_terms' AS "payment_terms",
      s."data"->>'description' AS "description",
      CASE
        WHEN (
          COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('PREPAID'::text)
          OR COALESCE(s."data"->>'exclude_reason', '') = 'PREPAID'
        )
        THEN true
        ELSE false
      END AS "alreadyExcluded"
    FROM "tbl_ptrs_stage_row" s
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND (
        COALESCE(s."data"->>'payment_terms','') ILIKE '%prepaid%'
        OR COALESCE(s."data"->>'payment_terms','') ILIKE '%pre-pay%'
        OR COALESCE(s."data"->>'payment_terms','') ILIKE '%prepay%'
        OR COALESCE(s."data"->>'description','') ILIKE '%prepaid%'
        OR COALESCE(s."data"->>'description','') ILIKE '%pre-pay%'
        OR COALESCE(s."data"->>'description','') ILIKE '%prepay%'
      )
    ORDER BY s."rowNo" ASC
    LIMIT :limit
  `;

  const [sampleRows] = await sequelize.query(sampleSql, {
    replacements: { customerId, ptrsId, limit: effectiveLimit },
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

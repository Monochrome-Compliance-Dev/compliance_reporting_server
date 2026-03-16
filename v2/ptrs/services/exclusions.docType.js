const {
  jsonText,
  appendJsonbTextArray,
  appendJsonbTextArrayAtPath,
  applyExcludeFlags,
  applyMetaBase,
} = require("./exclusions.shared");

async function applyDocTypeExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
}) {
  const docTypeExpr = jsonText("s", "document_type", "Document Type");
  const clearingDocExpr = jsonText(
    "s",
    "clearing_document",
    "Clearing Document",
  );
  const reasonSql = `'DOC_TYPE'`;
  const commentSql = `
    CASE
      WHEN ${docTypeExpr} = 'K1'
        THEN 'Document type exclusion — K1 document type'
      WHEN ${clearingDocExpr} LIKE '2000%'
        THEN 'Document type exclusion — clearing document begins 2000'
      WHEN ${docTypeExpr} IN ('Z', 'KZ', 'AB')
        AND ${clearingDocExpr} LIKE '5%'
        THEN 'Document type exclusion — internal/adjustment document type with clearing document beginning 5'
      ELSE 'Document type exclusion'
    END
  `;

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
        ${docTypeExpr} = 'K1'
        OR ${clearingDocExpr} LIKE '2000%'
        OR (
          ${docTypeExpr} IN ('Z', 'KZ', 'AB')
          AND ${clearingDocExpr} LIKE '5%'
        )
      )
      AND NOT (
        COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('DOC_TYPE'::text)
        OR COALESCE(s."data"->>'exclude_reason', '') = 'DOC_TYPE'
      )
  `;

  const [, meta] = await sequelize.query(sql, {
    replacements: { customerId, ptrsId },
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
  const countSql = `
    SELECT
      COUNT(*)::int AS "matchedCount",
      SUM(
        CASE
          WHEN (
            COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('DOC_TYPE'::text)
            OR COALESCE(s."data"->>'exclude_reason', '') = 'DOC_TYPE'
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
        COALESCE(s."data"->>'document_type', s."data"->>'Document Type', '') = 'K1'
        OR COALESCE(s."data"->>'clearing_document', s."data"->>'Clearing Document', '') LIKE '2000%'
        OR (
          COALESCE(s."data"->>'document_type', s."data"->>'Document Type', '') IN ('Z', 'KZ', 'AB')
          AND COALESCE(s."data"->>'clearing_document', s."data"->>'Clearing Document', '') LIKE '5%'
        )
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
      (s."data"->>'row_no')::int AS "row_no",
      s."data"->>'payer_entity_abn' AS "payer_entity_abn",
      s."data"->>'payer_entity_name' AS "payer_entity_name",
      s."data"->>'payee_entity_abn' AS "payee_entity_abn",
      s."data"->>'payee_entity_name' AS "payee_entity_name",
      s."data"->>'invoice_reference_number' AS "invoice_reference_number",
      s."data"->>'account_code' AS "account_code",
      s."data"->>'description' AS "description",
      s."data"->>'payment_date' AS "payment_date",
      s."data"->>'payment_amount' AS "payment_amount",
      CASE
        WHEN COALESCE(s."data"->>'document_type', s."data"->>'Document Type', '') = 'K1'
          THEN 'Document type exclusion — K1 document type'
        WHEN COALESCE(s."data"->>'clearing_document', s."data"->>'Clearing Document', '') LIKE '2000%'
          THEN 'Document type exclusion — clearing document begins 2000'
        WHEN COALESCE(s."data"->>'document_type', s."data"->>'Document Type', '') IN ('Z', 'KZ', 'AB')
          AND COALESCE(s."data"->>'clearing_document', s."data"->>'Clearing Document', '') LIKE '5%'
          THEN 'Document type exclusion — internal/adjustment document type with clearing document beginning 5'
        ELSE 'Document type exclusion'
      END AS "exclude_comment",
      CASE
        WHEN (
          COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('DOC_TYPE'::text)
          OR COALESCE(s."data"->>'exclude_reason', '') = 'DOC_TYPE'
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
        COALESCE(s."data"->>'document_type', s."data"->>'Document Type', '') = 'K1'
        OR COALESCE(s."data"->>'clearing_document', s."data"->>'Clearing Document', '') LIKE '2000%'
        OR (
          COALESCE(s."data"->>'document_type', s."data"->>'Document Type', '') IN ('Z', 'KZ', 'AB')
          AND COALESCE(s."data"->>'clearing_document', s."data"->>'Clearing Document', '') LIKE '5%'
        )
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
  applyDocTypeExclusion,
  previewDocTypeExclusion,
};

const {
  jsonText,
  appendJsonbTextArray,
  appendJsonbTextArrayAtPath,
  applyExcludeFlags,
  applyMetaBase,
} = require("./exclusions.shared");

function buildInternationalCommentSql(currencyExpr) {
  return `
    CASE
      WHEN upper(${currencyExpr}) <> 'AUD'
        THEN 'International supplier — non-AUD document currency'
      ELSE 'International supplier'
    END
  `;
}

function buildInternationalPredicate(currencyExpr) {
  return `(
    NULLIF(trim(${currencyExpr}), '') IS NOT NULL
    AND upper(${currencyExpr}) <> 'AUD'
  )`;
}

async function applyInternationalExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
}) {
  const currencyExpr = jsonText("s", "document_currency", "Document Currency");
  const predicateSql = buildInternationalPredicate(currencyExpr);
  const reasonSql = `'INTERNATIONAL'`;
  const commentSql = buildInternationalCommentSql(currencyExpr);

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
      AND ${predicateSql}
      AND NOT (
        COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('INTERNATIONAL'::text)
        OR COALESCE(s."data"->>'exclude_reason', '') = 'INTERNATIONAL'
      )
  `;

  const [, meta] = await sequelize.query(sql, {
    replacements: { customerId, ptrsId },
    transaction,
  });

  return Number(meta?.rowCount ?? 0) || 0;
}

async function previewInternationalExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  effectiveLimit,
}) {
  const currencyExpr = `COALESCE(s."data"->>'document_currency', s."data"->>'Document Currency', '')`;
  const predicateSql = buildInternationalPredicate(currencyExpr);
  const commentSql = buildInternationalCommentSql(currencyExpr);

  const countSql = `
    SELECT
      COUNT(*)::int AS "matchedCount",
      SUM(
        CASE
          WHEN (
            COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('INTERNATIONAL'::text)
            OR COALESCE(s."data"->>'exclude_reason', '') = 'INTERNATIONAL'
          )
          THEN 1 ELSE 0
        END
      )::int AS "alreadyExcludedCount"
    FROM "tbl_ptrs_stage_row" s
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND ${predicateSql}
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
      s."data"->>'payer_entity_name' AS "payer_entity_name",
      s."data"->>'payee_entity_name' AS "payee_entity_name",
      s."data"->>'payee_entity_abn' AS "payee_entity_abn",
      COALESCE(s."data"->>'document_currency', s."data"->>'Document Currency') AS "document_currency",
      s."data"->>'invoice_reference_number' AS "invoice_reference_number",
      s."data"->>'payment_date' AS "payment_date",
      s."data"->>'payment_amount' AS "payment_amount",
      ${commentSql} AS "exclude_comment",
      CASE
        WHEN (
          COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('INTERNATIONAL'::text)
          OR COALESCE(s."data"->>'exclude_reason', '') = 'INTERNATIONAL'
        )
        THEN true
        ELSE false
      END AS "alreadyExcluded"
    FROM "tbl_ptrs_stage_row" s
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND ${predicateSql}
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
  applyInternationalExclusion,
  previewInternationalExclusion,
};

const {
  buildKeywordMatchCondition,
  appendJsonbTextArray,
  appendJsonbTextArrayAtPath,
  applyExcludeFlags,
  applyMetaBase,
} = require("./exclusions.shared");

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
  const reasonSql = `'KEYWORD'`;
  const commentSql = `('Keyword exclusion — ' || k."keyword")`;

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
  const metaWithCommentsSql = appendJsonbTextArrayAtPath(
    "exclusions,comments",
    commentSql,
    metaWithReasonsSql,
  );
  const metaFinalSql = `
    jsonb_set(
      ${metaWithCommentsSql},
      '{exclusions,keyword}',
      to_jsonb(k."keyword"),
      true
    )
  `;

  const sql = `
    UPDATE "tbl_ptrs_stage_row" s
    SET
      "data" = ${dataFinalSql},
      "meta" = ${metaFinalSql},
      "updatedAt" = now()
    FROM "tbl_ptrs_exclusion_keyword_customer_ref" k
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND k."deletedAt" IS NULL
      AND k."customerId" = :customerId
      AND k."profileId" = :profileId
      AND (
        ${keywordMatchCondition}
      )
      AND NOT (
        COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('KEYWORD'::text)
        OR COALESCE(s."data"->>'exclude_reason', '') = 'KEYWORD'
      )
  `;

  const [, meta] = await sequelize.query(sql, {
    replacements: { customerId, ptrsId, profileId },
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

  const countSql = `
    SELECT
      COUNT(*)::int AS "matchedCount",
      SUM(
        CASE
          WHEN (
            COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('KEYWORD'::text)
            OR COALESCE(s."data"->>'exclude_reason', '') = 'KEYWORD'
          )
          THEN 1 ELSE 0
        END
      )::int AS "alreadyExcludedCount"
    FROM "tbl_ptrs_stage_row" s
    JOIN "tbl_ptrs_exclusion_keyword_customer_ref" k
      ON k."customerId" = :customerId
      AND k."profileId" = :profileId
      AND k."deletedAt" IS NULL
      AND (
        ${keywordMatchCondition}
      )
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
  `;

  const [countRows] = await sequelize.query(countSql, {
    replacements: { customerId, ptrsId, profileId },
    transaction,
  });

  const matched = Number(countRows?.[0]?.matchedCount ?? 0) || 0;
  const alreadyExcluded =
    Number(countRows?.[0]?.alreadyExcludedCount ?? 0) || 0;

  const sampleSql = `
    SELECT
      (s."data"->>'row_no')::int AS "row_no",
      s."data"->>'payee_entity_abn' AS "payee_entity_abn",
      s."data"->>'payee_entity_name' AS "payee_entity_name",
      s."data"->>'description' AS "description",
      s."data"->>'invoice_reference_number' AS "invoice_reference_number",
      s."data"->>'account_code' AS "account_code",
      s."data"->>'account_name' AS "account_name",
      s."data"->>'payment_date' AS "payment_date",
      s."data"->>'payment_amount' AS "payment_amount",
      ('Keyword exclusion — ' || k."keyword")::text AS "exclude_comment",
      k."keyword" AS "matched_keyword",
      CASE
        WHEN (
          COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('KEYWORD'::text)
          OR COALESCE(s."data"->>'exclude_reason', '') = 'KEYWORD'
        )
        THEN true
        ELSE false
      END AS "alreadyExcluded"
    FROM "tbl_ptrs_stage_row" s
    JOIN "tbl_ptrs_exclusion_keyword_customer_ref" k
      ON k."customerId" = :customerId
      AND k."profileId" = :profileId
      AND k."deletedAt" IS NULL
      AND (
        ${keywordMatchCondition}
      )
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
    ORDER BY s."rowNo" ASC
    LIMIT :limit
  `;

  const [sampleRows] = await sequelize.query(sampleSql, {
    replacements: { customerId, ptrsId, profileId, limit: effectiveLimit },
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

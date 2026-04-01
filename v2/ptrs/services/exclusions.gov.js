const {
  jsonText,
  appendJsonbTextArray,
  appendJsonbTextArrayAtPath,
  applyExcludeFlags,
  applyMetaBase,
} = require("./exclusions.shared");

async function applyGovExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
}) {
  const payeeAbnExpr = `NULLIF(regexp_replace(${jsonText("s", "payee_entity_abn")}, '\\D', '', 'g'), '')`;
  const govAbnExpr = `NULLIF(regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g'), '')`;
  const reasonSql = `'GOV_ENTITY'`;
  const commentSql = `
    (
      'Government entity' ||
      CASE WHEN COALESCE(g."name",'') <> '' THEN ' — ' || g."name" ELSE '' END ||
      CASE WHEN COALESCE(g."category",'') <> '' THEN ' — ' || g."category" ELSE '' END
    )
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
    FROM "tbl_ptrs_gov_entity_ref" g
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND g."deletedAt" IS NULL
      AND ${payeeAbnExpr} IS NOT NULL
      AND ${govAbnExpr} IS NOT NULL
      AND ${payeeAbnExpr} = ${govAbnExpr}
      AND NOT (
        COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('GOV_ENTITY'::text)
        OR COALESCE(s."data"->>'exclude_reason', '') = 'GOV_ENTITY'
      )
  `;

  const [, meta] = await sequelize.query(sql, {
    replacements: { customerId, ptrsId },
    transaction,
  });

  return Number(meta?.rowCount ?? 0) || 0;
}

async function previewGovExclusion({
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
            COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('GOV_ENTITY'::text)
            OR COALESCE(s."data"->>'exclude_reason', '') = 'GOV_ENTITY'
          )
          THEN 1 ELSE 0
        END
      )::int AS "alreadyExcludedCount"
    FROM "tbl_ptrs_stage_row" s
    JOIN "tbl_ptrs_gov_entity_ref" g
      ON g."deletedAt" IS NULL
      AND NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NOT NULL
      AND NULLIF(regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g'), '') IS NOT NULL
      AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
        = regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g')
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
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
      s."data"->>'payment_date' AS "payment_date",
      s."data"->>'payment_amount' AS "payment_amount",
      (
        'Government entity' ||
        CASE WHEN COALESCE(g."name",'') <> '' THEN ' — ' || g."name" ELSE '' END ||
        CASE WHEN COALESCE(g."category",'') <> '' THEN ' — ' || g."category" ELSE '' END
      )::text AS "exclude_comment",
      CASE
        WHEN (
          COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('GOV_ENTITY'::text)
          OR COALESCE(s."data"->>'exclude_reason', '') = 'GOV_ENTITY'
        )
        THEN true
        ELSE false
      END AS "alreadyExcluded"
    FROM "tbl_ptrs_stage_row" s
    JOIN "tbl_ptrs_gov_entity_ref" g
      ON g."deletedAt" IS NULL
      AND NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NOT NULL
      AND NULLIF(regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g'), '') IS NOT NULL
      AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
        = regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g')
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND g."deletedAt" IS NULL
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
  applyGovExclusion,
  previewGovExclusion,
};

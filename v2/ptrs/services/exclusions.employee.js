const {
  appendJsonbTextArray,
  appendJsonbTextArrayAtPath,
  applyExcludeFlags,
  applyMetaBase,
} = require("./exclusions.shared");

async function applyEmployeeExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  profileId,
}) {
  if (!profileId) throw new Error("profileId is required for employee");

  const reasonSql = `'EMPLOYEE'`;
  const commentSql = `
    (
      'Employee / payroll' ||
      CASE WHEN COALESCE(m.matched_name,'') <> '' THEN ' — ' || m.matched_name ELSE '' END
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
    WITH matches AS (
      SELECT
        s."id" AS stage_id,
        MIN(r."name") AS matched_name
      FROM "tbl_ptrs_stage_row" s
      JOIN "tbl_ptrs_employee_ref" r
        ON r."customerId" = :customerId
        AND r."profileId" = :profileId
        AND r."deletedAt" IS NULL
      WHERE
        s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
        AND (
          (
            NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NOT NULL
            AND NULLIF(regexp_replace(COALESCE(r."abn",''), '\\D', '', 'g'), '') IS NOT NULL
            AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
              = regexp_replace(COALESCE(r."abn",''), '\\D', '', 'g')
          )
          OR
          (
            trim(COALESCE(s."data"->>'payee_entity_name','')) <> ''
            AND trim(COALESCE(r."name",'')) <> ''
            AND lower(s."data"->>'payee_entity_name') LIKE '%' || lower(r."name") || '%'
          )
          OR
          (
            trim(COALESCE(s."data"->>'description','')) <> ''
            AND trim(COALESCE(r."name",'')) <> ''
            AND lower(s."data"->>'description') LIKE '%' || lower(r."name") || '%'
          )
        )
      GROUP BY s."id"
    )
    UPDATE "tbl_ptrs_stage_row" s
    SET
      "data" = ${dataFinalSql},
      "meta" = ${metaFinalSql},
      "updatedAt" = now()
    FROM matches m
    WHERE
      s."id" = m.stage_id
      AND NOT (
        COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('EMPLOYEE'::text)
        OR COALESCE(s."data"->>'exclude_reason', '') = 'EMPLOYEE'
      )
  `;

  const [, meta] = await sequelize.query(sql, {
    replacements: { customerId, ptrsId, profileId },
    transaction,
  });

  return Number(meta?.rowCount ?? 0) || 0;
}

async function previewEmployeeExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  profileId,
  effectiveLimit,
}) {
  if (!profileId) throw new Error("profileId is required for employee");

  const countSql = `
    WITH matched AS (
      SELECT
        s."id" AS stage_id,
        MAX(
          CASE
            WHEN (
              COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('EMPLOYEE'::text)
              OR COALESCE(s."data"->>'exclude_reason', '') = 'EMPLOYEE'
            )
            THEN 1 ELSE 0
          END
        ) AS already_excluded
      FROM "tbl_ptrs_stage_row" s
      JOIN "tbl_ptrs_employee_ref" r
        ON r."customerId" = :customerId
        AND r."profileId" = :profileId
        AND r."deletedAt" IS NULL
      WHERE
        s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
        AND (
          (
            NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NOT NULL
            AND NULLIF(regexp_replace(COALESCE(r."abn",''), '\\D', '', 'g'), '') IS NOT NULL
            AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
              = regexp_replace(COALESCE(r."abn",''), '\\D', '', 'g')
          )
          OR
          (
            trim(COALESCE(s."data"->>'payee_entity_name','')) <> ''
            AND trim(COALESCE(r."name",'')) <> ''
            AND lower(s."data"->>'payee_entity_name') LIKE '%' || lower(r."name") || '%'
          )
          OR
          (
            trim(COALESCE(s."data"->>'description','')) <> ''
            AND trim(COALESCE(r."name",'')) <> ''
            AND lower(s."data"->>'description') LIKE '%' || lower(r."name") || '%'
          )
        )
      GROUP BY s."id"
    )
    SELECT
      COUNT(*)::int AS "matchedCount",
      SUM(already_excluded)::int AS "alreadyExcludedCount"
    FROM matched
  `;

  const [countRows] = await sequelize.query(countSql, {
    replacements: { customerId, ptrsId, profileId },
    transaction,
  });

  const matched = Number(countRows?.[0]?.matchedCount ?? 0) || 0;
  const alreadyExcluded =
    Number(countRows?.[0]?.alreadyExcludedCount ?? 0) || 0;

  const sampleSql = `
    SELECT DISTINCT ON (s."id")
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
      (
        'Employee / payroll' ||
        CASE WHEN COALESCE(r."name",'') <> '' THEN ' — ' || r."name" ELSE '' END
      )::text AS "exclude_comment",
      CASE
        WHEN (
          COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('EMPLOYEE'::text)
          OR COALESCE(s."data"->>'exclude_reason', '') = 'EMPLOYEE'
        )
        THEN true
        ELSE false
      END AS "alreadyExcluded"
    FROM "tbl_ptrs_stage_row" s
    JOIN "tbl_ptrs_employee_ref" r
      ON r."customerId" = :customerId
      AND r."profileId" = :profileId
      AND r."deletedAt" IS NULL
      AND (
        (
          NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NOT NULL
          AND NULLIF(regexp_replace(COALESCE(r."abn",''), '\\D', '', 'g'), '') IS NOT NULL
          AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
            = regexp_replace(COALESCE(r."abn",''), '\\D', '', 'g')
        )
        OR
        (
          trim(COALESCE(s."data"->>'payee_entity_name','')) <> ''
          AND trim(COALESCE(r."name",'')) <> ''
          AND lower(s."data"->>'payee_entity_name') LIKE '%' || lower(r."name") || '%'
        )
        OR
        (
          trim(COALESCE(s."data"->>'description','')) <> ''
          AND trim(COALESCE(r."name",'')) <> ''
          AND lower(s."data"->>'description') LIKE '%' || lower(r."name") || '%'
        )
      )
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
    ORDER BY s."id", r."name" NULLS LAST, s."rowNo" ASC
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
  applyEmployeeExclusion,
  previewEmployeeExclusion,
};

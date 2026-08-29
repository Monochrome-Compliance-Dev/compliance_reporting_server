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

function buildJsonbTextArrayValue(sourceSql, path, valueSql) {
  return `
    CASE
      WHEN jsonb_typeof(COALESCE(${sourceSql}->'${path}', '[]'::jsonb)) = 'array'
        THEN CASE
          WHEN COALESCE(${sourceSql}->'${path}', '[]'::jsonb) @> jsonb_build_array((${valueSql})::text)
            THEN COALESCE(${sourceSql}->'${path}', '[]'::jsonb)
          ELSE COALESCE(${sourceSql}->'${path}', '[]'::jsonb) || to_jsonb((${valueSql})::text)
        END
      WHEN ${sourceSql} ? '${path}'
        THEN CASE
          WHEN ${sourceSql}->'${path}' = to_jsonb((${valueSql})::text)
            THEN jsonb_build_array((${valueSql})::text)
          ELSE jsonb_build_array(${sourceSql}->>'${path}') || to_jsonb((${valueSql})::text)
        END
      ELSE jsonb_build_array((${valueSql})::text)
    END
  `;
}

function buildInternationalDataSql({ dataSql, reasonSql, commentSql }) {
  return `
    ${dataSql} || jsonb_build_object(
      'exclude', true,
      'exclude_from_metrics', true,
      'exclude_reason', CASE
        WHEN trim(COALESCE(${dataSql}->>'exclude_reason', '')) <> ''
          THEN to_jsonb(${dataSql}->>'exclude_reason')
        ELSE to_jsonb((${reasonSql})::text)
      END,
      'exclude_reasons', ${buildJsonbTextArrayValue(
        dataSql,
        "exclude_reasons",
        reasonSql,
      )},
      'exclude_comment', ${buildJsonbTextArrayValue(
        dataSql,
        "exclude_comment",
        commentSql,
      )}
    )
  `;
}

function buildInternationalMetaSql({ metaSql, reasonSql, commentSql }) {
  const sourceSql = `COALESCE(${metaSql}, '{}'::jsonb)`;
  const baseSql = `
    jsonb_set(
      jsonb_set(
        ${sourceSql},
        '{_stage}',
        to_jsonb('ptrs.v2.exclusionsApply'::text),
        true
      ),
      '{at}',
      to_jsonb(now()::text),
      true
    )
  `;
  const exclusionsSql = `${sourceSql}->'exclusions'`;

  return `
    ${baseSql} ||
    CASE
      WHEN jsonb_typeof(${exclusionsSql}) = 'object'
        THEN jsonb_build_object(
          'exclusions',
          ${exclusionsSql} || jsonb_build_object(
            'excluded', true,
            'reason', CASE
              WHEN trim(COALESCE(${exclusionsSql}->>'reason', '')) <> ''
                THEN to_jsonb(${exclusionsSql}->>'reason')
              ELSE to_jsonb((${reasonSql})::text)
            END,
            'reasons', ${buildJsonbTextArrayValue(
              exclusionsSql,
              "reasons",
              reasonSql,
            )},
            'comments', ${buildJsonbTextArrayValue(
              exclusionsSql,
              "comments",
              commentSql,
            )}
          )
        )
      ELSE '{}'::jsonb
    END
  `;
}

async function applyInternationalExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
}) {
  const currencyExpr = `s."documentCurrency"`;
  const predicateSql = buildInternationalPredicate(currencyExpr);
  const reasonSql = `'INTERNATIONAL'`;
  const commentSql = buildInternationalCommentSql(currencyExpr);
  const dataFinalSql = buildInternationalDataSql({
    dataSql: `s."data"`,
    reasonSql,
    commentSql,
  });
  const metaFinalSql = buildInternationalMetaSql({
    metaSql: `s."meta"`,
    reasonSql,
    commentSql,
  });

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
  const currencyExpr = `s."documentCurrency"`;
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
      s."documentCurrency" AS "document_currency",
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

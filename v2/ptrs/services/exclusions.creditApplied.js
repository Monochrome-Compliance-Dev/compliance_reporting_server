const {
  appendJsonbTextArray,
  appendJsonbTextArrayAtPath,
  applyExcludeFlags,
  applyMetaBase,
} = require("./exclusions.shared");

const REASON_SQL = `'CREDIT_APPLIED'`;
const COMMENT_SQL = `'Credit applied — clearing group begins 200'`;

function firstNonEmptyText(...expressions) {
  return `COALESCE(${expressions
    .map((expression) => `NULLIF(BTRIM(COALESCE(${expression}, '')), '')`)
    .join(", ")})`;
}

function buildClearingGroupExpressions(alias) {
  return {
    companyCode: firstNonEmptyText(
      `${alias}."data"->>'company_code'`,
      `${alias}."data"->>'Company Code'`,
    ),
    account: firstNonEmptyText(
      `${alias}."sourceAccountCode"`,
      `${alias}."data"->>'source_account_code'`,
      `${alias}."data"->>'account_code'`,
      `${alias}."data"->>'account'`,
      `${alias}."data"->>'Account'`,
    ),
    clearingDocument: firstNonEmptyText(
      `${alias}."clearingDocument"`,
      `${alias}."data"->>'clearing_document'`,
      `${alias}."data"->>'Clearing Document'`,
    ),
  };
}

function buildMatchedGroupsCte() {
  const candidate = buildClearingGroupExpressions("candidate");

  return `
    matched_credit_groups AS (
      SELECT DISTINCT
        ${candidate.companyCode} AS "companyCode",
        ${candidate.account} AS "account",
        ${candidate.clearingDocument} AS "clearingDocument"
      FROM "tbl_ptrs_stage_row" candidate
      WHERE
        candidate."customerId" = :customerId
        AND candidate."ptrsId" = :ptrsId
        AND candidate."deletedAt" IS NULL
        AND ${candidate.companyCode} IS NOT NULL
        AND ${candidate.account} IS NOT NULL
        AND ${candidate.clearingDocument} LIKE '200%'
    )
  `;
}

function buildGroupMatchCondition(alias) {
  const target = buildClearingGroupExpressions(alias);

  return `
    EXISTS (
      SELECT 1
      FROM matched_credit_groups matched_group
      WHERE matched_group."companyCode" = ${target.companyCode}
        AND matched_group."account" = ${target.account}
        AND matched_group."clearingDocument" = ${target.clearingDocument}
    )
  `;
}

async function applyCreditAppliedExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
}) {
  const dataBaseSql = applyExcludeFlags(`s."data"`, REASON_SQL);
  const dataWithReasonsSql = appendJsonbTextArray(
    "exclude_reasons",
    REASON_SQL,
    dataBaseSql,
  );
  const dataFinalSql = appendJsonbTextArray(
    "exclude_comment",
    COMMENT_SQL,
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
        ELSE to_jsonb((${REASON_SQL})::text)
      END,
      true
    )
  `;
  const metaWithReasonsSql = appendJsonbTextArrayAtPath(
    "exclusions,reasons",
    REASON_SQL,
    metaWithReasonSql,
  );
  const metaFinalSql = appendJsonbTextArrayAtPath(
    "exclusions,comments",
    COMMENT_SQL,
    metaWithReasonsSql,
  );

  const sql = `
    WITH ${buildMatchedGroupsCte()}
    UPDATE "tbl_ptrs_stage_row" s
    SET
      "data" = ${dataFinalSql},
      "meta" = ${metaFinalSql},
      "updatedAt" = now()
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND ${buildGroupMatchCondition("s")}
      AND NOT (
        COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('CREDIT_APPLIED'::text)
        OR COALESCE(s."data"->>'exclude_reason', '') = 'CREDIT_APPLIED'
      )
  `;

  const [, meta] = await sequelize.query(sql, {
    replacements: { customerId, ptrsId },
    transaction,
  });

  return Number(meta?.rowCount ?? 0) || 0;
}

async function previewCreditAppliedExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  effectiveLimit,
}) {
  const countSql = `
    WITH ${buildMatchedGroupsCte()}
    SELECT
      COUNT(*)::int AS "matchedCount",
      SUM(
        CASE
          WHEN (
            COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('CREDIT_APPLIED'::text)
            OR COALESCE(s."data"->>'exclude_reason', '') = 'CREDIT_APPLIED'
          )
          THEN 1 ELSE 0
        END
      )::int AS "alreadyExcludedCount"
    FROM "tbl_ptrs_stage_row" s
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND ${buildGroupMatchCondition("s")}
  `;

  const [countRows] = await sequelize.query(countSql, {
    replacements: { customerId, ptrsId },
    transaction,
  });

  const target = buildClearingGroupExpressions("s");
  const sampleSql = `
    WITH ${buildMatchedGroupsCte()}
    SELECT
      (s."data"->>'row_no')::int AS "row_no",
      s."data"->>'payer_entity_abn' AS "payer_entity_abn",
      s."data"->>'payer_entity_name' AS "payer_entity_name",
      s."data"->>'payee_entity_abn' AS "payee_entity_abn",
      s."data"->>'payee_entity_name' AS "payee_entity_name",
      s."data"->>'invoice_reference_number' AS "invoice_reference_number",
      ${target.companyCode} AS "company_code",
      ${target.account} AS "account",
      ${target.clearingDocument} AS "clearing_document",
      s."data"->>'document_type' AS "document_type",
      ${COMMENT_SQL}::text AS "exclude_comment",
      CASE
        WHEN (
          COALESCE(s."data"->'exclude_reasons', '[]'::jsonb) @> jsonb_build_array('CREDIT_APPLIED'::text)
          OR COALESCE(s."data"->>'exclude_reason', '') = 'CREDIT_APPLIED'
        )
        THEN true
        ELSE false
      END AS "alreadyExcluded"
    FROM "tbl_ptrs_stage_row" s
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND ${buildGroupMatchCondition("s")}
    ORDER BY s."rowNo" ASC
    LIMIT :limit
  `;

  const [sampleRows] = await sequelize.query(sampleSql, {
    replacements: { customerId, ptrsId, limit: effectiveLimit },
    transaction,
  });

  return {
    matched: Number(countRows?.[0]?.matchedCount ?? 0) || 0,
    alreadyExcluded:
      Number(countRows?.[0]?.alreadyExcludedCount ?? 0) || 0,
    sampleRows: Array.isArray(sampleRows) ? sampleRows : [],
  };
}

module.exports = {
  applyCreditAppliedExclusion,
  previewCreditAppliedExclusion,
};

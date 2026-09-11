const {
  appendJsonbTextArray,
  appendJsonbTextArrayAtPath,
  applyExcludeFlags,
  applyMetaBase,
} = require("./exclusions.shared");

const TCP_GROUP_EXCLUSION_REASONS = Object.freeze([
  "GOVERNMENT_ENTITY",
  "NO_ABN",
  "INVALID_ABN",
  "ABN_NOT_CONFIRMED",
  "EMPLOYEE_PAYMENT",
  "INTRA_GROUP",
]);

function groupFieldSql(alias, field) {
  if (field === "scope") {
    return `COALESCE(NULLIF(BTRIM(${alias}."sourceGroupScope"), ''),
      'dataset:' || ${alias}."datasetId")`;
  }
  if (field === "company") {
    return `NULLIF(BTRIM(${alias}."data"->>'company_code'), '')`;
  }
  if (field === "account") {
    return `NULLIF(BTRIM(${alias}."sourceAccountCode"), '')`;
  }
  return `NULLIF(BTRIM(${alias}."clearingDocument"), '')`;
}

function buildTcpGroupExclusionPropagationSql(reason) {
  if (!TCP_GROUP_EXCLUSION_REASONS.includes(reason)) {
    throw new Error(`Unsupported TCP group exclusion reason: ${reason}`);
  }
  const reasonSql = `'${reason}'`;
  const commentSql = `'Payment group inherits ${reason} from an associated Stage row'`;
  const dataSql = appendJsonbTextArray(
    "exclude_comment",
    commentSql,
    appendJsonbTextArray(
      "exclude_reasons",
      reasonSql,
      applyExcludeFlags('target."data"', reasonSql),
    ),
  );
  const metaSql = appendJsonbTextArrayAtPath(
    "exclusions,comments",
    commentSql,
    appendJsonbTextArrayAtPath(
      "exclusions,reasons",
      reasonSql,
      applyMetaBase('target."meta"'),
    ),
  );

  return `
    WITH excluded_groups AS MATERIALIZED (
      SELECT DISTINCT
        ${groupFieldSql("source", "scope")} AS source_scope,
        ${groupFieldSql("source", "company")} AS company_code,
        ${groupFieldSql("source", "account")} AS source_account_code,
        ${groupFieldSql("source", "clearing")} AS clearing_document
      FROM "tbl_ptrs_stage_row" source
      WHERE source."customerId" = :customerId
        AND source."ptrsId" = :ptrsId
        AND source."deletedAt" IS NULL
        AND source."semanticKind" = 'accounting_event'
        AND COALESCE(source."data"->'exclude_reasons', '[]'::jsonb)
          @> jsonb_build_array(${reasonSql}::text)
        AND ${groupFieldSql("source", "company")} IS NOT NULL
        AND ${groupFieldSql("source", "account")} IS NOT NULL
        AND ${groupFieldSql("source", "clearing")} IS NOT NULL
    )
    UPDATE "tbl_ptrs_stage_row" target
    SET "data" = ${dataSql}, "meta" = ${metaSql}, "updatedAt" = now()
    FROM excluded_groups excluded_group
    WHERE target."customerId" = :customerId
      AND target."ptrsId" = :ptrsId
      AND target."deletedAt" IS NULL
      AND target."semanticKind" = 'accounting_event'
      AND ${groupFieldSql("target", "scope")} = excluded_group.source_scope
      AND ${groupFieldSql("target", "company")} = excluded_group.company_code
      AND ${groupFieldSql("target", "account")} = excluded_group.source_account_code
      AND ${groupFieldSql("target", "clearing")} = excluded_group.clearing_document
      AND NOT COALESCE(target."data"->'exclude_reasons', '[]'::jsonb)
        @> jsonb_build_array(${reasonSql}::text)
  `;
}

async function applyTcpGroupExclusionPropagation({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  reasons = TCP_GROUP_EXCLUSION_REASONS,
}) {
  let updated = 0;
  for (const reason of reasons) {
    const [, metadata] = await sequelize.query(
      buildTcpGroupExclusionPropagationSql(reason),
      {
        replacements: { customerId, ptrsId },
        transaction,
      },
    );
    updated += Number(metadata?.rowCount || 0);
  }
  return updated;
}

module.exports = {
  TCP_GROUP_EXCLUSION_REASONS,
  applyTcpGroupExclusionPropagation,
  buildTcpGroupExclusionPropagationSql,
};

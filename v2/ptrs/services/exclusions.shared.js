const KEYWORD_MATCH_FIELDS = {
  any: [
    "payee_entity_name",
    "description",
    "invoice_reference_number",
    "account_name",
    "account_code",
  ],
  payee_entity_name: ["payee_entity_name"],
  description: ["description"],
  invoice_reference_number: ["invoice_reference_number"],
  account_name: ["account_name"],
  account_code: ["account_code"],
};

function normaliseKeyword(raw) {
  return String(raw || "").trim();
}

function normaliseKeywordField(raw) {
  const value = String(raw || "any").trim();
  return KEYWORD_MATCH_FIELDS[value] ? value : "any";
}

function normaliseKeywordMatchType(raw) {
  const value = String(raw || "contains").trim();
  return value === "equals" ? "equals" : "contains";
}

function buildKeywordMatchCondition({ stageAlias, keywordAlias }) {
  const fieldExpr = `COALESCE(${keywordAlias}."field", 'any')`;
  const matchTypeExpr = `COALESCE(${keywordAlias}."matchType", 'contains')`;

  return `
    CASE
      WHEN ${fieldExpr} = 'payee_entity_name' THEN
        CASE
          WHEN ${matchTypeExpr} = 'equals'
            THEN lower(trim(COALESCE(${stageAlias}."data"->>'payee_entity_name', ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
          ELSE COALESCE(${stageAlias}."data"->>'payee_entity_name', '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
        END
      WHEN ${fieldExpr} = 'description' THEN
        CASE
          WHEN ${matchTypeExpr} = 'equals'
            THEN lower(trim(COALESCE(${stageAlias}."data"->>'description', ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
          ELSE COALESCE(${stageAlias}."data"->>'description', '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
        END
      WHEN ${fieldExpr} = 'invoice_reference_number' THEN
        CASE
          WHEN ${matchTypeExpr} = 'equals'
            THEN lower(trim(COALESCE(${stageAlias}."data"->>'invoice_reference_number', ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
          ELSE COALESCE(${stageAlias}."data"->>'invoice_reference_number', '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
        END
      WHEN ${fieldExpr} = 'account_name' THEN
        CASE
          WHEN ${matchTypeExpr} = 'equals'
            THEN lower(trim(COALESCE(${stageAlias}."data"->>'account_name', ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
          ELSE COALESCE(${stageAlias}."data"->>'account_name', '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
        END
      WHEN ${fieldExpr} = 'account_code' THEN
        CASE
          WHEN ${matchTypeExpr} = 'equals'
            THEN lower(trim(COALESCE(${stageAlias}."data"->>'account_code', ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
          ELSE COALESCE(${stageAlias}."data"->>'account_code', '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
        END
      ELSE
        CASE
          WHEN ${matchTypeExpr} = 'equals' THEN
            lower(trim(COALESCE(${stageAlias}."data"->>'payee_entity_name', ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
            OR lower(trim(COALESCE(${stageAlias}."data"->>'description', ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
            OR lower(trim(COALESCE(${stageAlias}."data"->>'invoice_reference_number', ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
            OR lower(trim(COALESCE(${stageAlias}."data"->>'account_name', ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
            OR lower(trim(COALESCE(${stageAlias}."data"->>'account_code', ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
          ELSE
            COALESCE(${stageAlias}."data"->>'payee_entity_name', '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
            OR COALESCE(${stageAlias}."data"->>'description', '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
            OR COALESCE(${stageAlias}."data"->>'invoice_reference_number', '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
            OR COALESCE(${stageAlias}."data"->>'account_name', '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
            OR COALESCE(${stageAlias}."data"->>'account_code', '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
        END
    END
  `;
}

function jsonText(dataAlias, ...keys) {
  if (!keys.length) return "''";
  return `COALESCE(${keys.map((key) => `${dataAlias}."data"->>'${key}'`).join(", ")}, '')`;
}

function appendJsonbTextArray(path, valueSql, sourceSql) {
  return `
    jsonb_set(
      ${sourceSql},
      '{${path}}',
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
      END,
      true
    )
  `;
}

function appendJsonbTextArrayAtPath(pathLiteral, valueSql, sourceSql) {
  return `
    jsonb_set(
      ${sourceSql},
      '{${pathLiteral}}',
      CASE
        WHEN jsonb_typeof(COALESCE(${sourceSql}#>'{${pathLiteral}}', '[]'::jsonb)) = 'array'
          THEN CASE
            WHEN COALESCE(${sourceSql}#>'{${pathLiteral}}', '[]'::jsonb) @> jsonb_build_array((${valueSql})::text)
              THEN COALESCE(${sourceSql}#>'{${pathLiteral}}', '[]'::jsonb)
            ELSE COALESCE(${sourceSql}#>'{${pathLiteral}}', '[]'::jsonb) || to_jsonb((${valueSql})::text)
          END
        WHEN ${sourceSql}#>'{${pathLiteral}}' IS NOT NULL
          THEN CASE
            WHEN ${sourceSql}#>'{${pathLiteral}}' = to_jsonb((${valueSql})::text)
              THEN jsonb_build_array((${valueSql})::text)
            ELSE jsonb_build_array(${sourceSql}#>>'{${pathLiteral}}') || to_jsonb((${valueSql})::text)
          END
        ELSE jsonb_build_array((${valueSql})::text)
      END,
      true
    )
  `;
}

function applyExcludeFlags(baseSql, reasonSql) {
  return `
    jsonb_set(
      jsonb_set(
        jsonb_set(
          ${baseSql},
          '{exclude}',
          'true'::jsonb,
          true
        ),
        '{exclude_from_metrics}',
        'true'::jsonb,
        true
      ),
      '{exclude_reason}',
      CASE
        WHEN trim(COALESCE(${baseSql}->>'exclude_reason', '')) <> ''
          THEN to_jsonb(${baseSql}->>'exclude_reason')
        ELSE to_jsonb((${reasonSql})::text)
      END,
      true
    )
  `;
}

function applyMetaBase(baseSql) {
  return `
    jsonb_set(
      jsonb_set(
        jsonb_set(
          COALESCE(${baseSql}, '{}'::jsonb),
          '{_stage}',
          to_jsonb('ptrs.v2.exclusionsApply'::text),
          true
        ),
        '{at}',
        to_jsonb(now()::text),
        true
      ),
      '{exclusions,excluded}',
      'true'::jsonb,
      true
    )
  `;
}

module.exports = {
  KEYWORD_MATCH_FIELDS,
  normaliseKeyword,
  normaliseKeywordField,
  normaliseKeywordMatchType,
  buildKeywordMatchCondition,
  jsonText,
  appendJsonbTextArray,
  appendJsonbTextArrayAtPath,
  applyExcludeFlags,
  applyMetaBase,
};

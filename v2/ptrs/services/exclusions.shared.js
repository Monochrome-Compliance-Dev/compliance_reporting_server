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
            THEN lower(trim(COALESCE(${stageAlias}."payeeEntityName", ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
          ELSE COALESCE(${stageAlias}."payeeEntityName", '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
        END
      WHEN ${fieldExpr} = 'description' THEN
        CASE
          WHEN ${matchTypeExpr} = 'equals'
            THEN lower(trim(COALESCE(${stageAlias}."description", ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
          ELSE COALESCE(${stageAlias}."description", '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
        END
      WHEN ${fieldExpr} = 'invoice_reference_number' THEN
        CASE
          WHEN ${matchTypeExpr} = 'equals'
            THEN lower(trim(COALESCE(${stageAlias}."invoiceReferenceNumber", ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
          ELSE COALESCE(${stageAlias}."invoiceReferenceNumber", '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
        END
      WHEN ${fieldExpr} = 'account_name' THEN
        CASE
          WHEN ${matchTypeExpr} = 'equals'
            THEN lower(trim(COALESCE(${stageAlias}."sourceAccountName", ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
          ELSE COALESCE(${stageAlias}."sourceAccountName", '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
        END
      WHEN ${fieldExpr} = 'account_code' THEN
        CASE
          WHEN ${matchTypeExpr} = 'equals'
            THEN lower(trim(COALESCE(${stageAlias}."sourceAccountCode", ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
          ELSE COALESCE(${stageAlias}."sourceAccountCode", '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
        END
      ELSE
        CASE
          WHEN ${matchTypeExpr} = 'equals' THEN
            lower(trim(COALESCE(${stageAlias}."payeeEntityName", ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
            OR lower(trim(COALESCE(${stageAlias}."description", ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
            OR lower(trim(COALESCE(${stageAlias}."invoiceReferenceNumber", ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
            OR lower(trim(COALESCE(${stageAlias}."sourceAccountName", ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
            OR lower(trim(COALESCE(${stageAlias}."sourceAccountCode", ''))) = lower(trim(COALESCE(${keywordAlias}."keyword", '')))
          ELSE
            COALESCE(${stageAlias}."payeeEntityName", '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
            OR COALESCE(${stageAlias}."description", '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
            OR COALESCE(${stageAlias}."invoiceReferenceNumber", '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
            OR COALESCE(${stageAlias}."sourceAccountName", '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
            OR COALESCE(${stageAlias}."sourceAccountCode", '') ILIKE '%' || COALESCE(${keywordAlias}."keyword", '') || '%'
        END
    END
  `;
}

module.exports = {
  KEYWORD_MATCH_FIELDS,
  normaliseKeyword,
  normaliseKeywordField,
  normaliseKeywordMatchType,
  buildKeywordMatchCondition,
};

const { toSnake } = require("@/v2/ptrs/services/ptrs.service");
const {
  CANONICAL_DATE_FIELDS,
} = require("@/v2/ptrs/services/canonical.date-normalisation.ptrs.service");

const COMMON_PAYMENT_FIELDS = Object.freeze([
  "payer_entity_name",
  "payer_entity_abn",
  "payee_entity_name",
  "payee_entity_abn",
  "invoice_reference_number",
  "payment_amount",
  "payment_date",
]);

const PAYMENT_CLOCK_FIELDS = Object.freeze([
  "invoice_issue_date",
  "invoice_receipt_date",
  "notice_for_payment_issue_date",
  "supply_date",
]);

const ADAPTER_CONTRACTS = Object.freeze({
  sap_accounting_event: Object.freeze({
    adapterType: "sap_accounting_event",
    defaultVersion: "1",
    supportedVersions: Object.freeze(["1"]),
    semanticKind: "accounting_event",
    canonicalProjection: "mapped_canonical_row",
    sourceGroupSemantics: "scoped_accounting_ledger",
    requiredFields: Object.freeze([
      ...COMMON_PAYMENT_FIELDS,
      "document_type",
      "company_code",
      "source_account_code",
      "clearing_document",
    ]),
    requiredAnyGroups: Object.freeze([
      Object.freeze({
        id: "payment_clock_start",
        fields: PAYMENT_CLOCK_FIELDS,
      }),
    ]),
    optionalFields: Object.freeze(["supplier_has_no_abn"]),
    validateRows: false,
  }),
  xero_accounting_event: Object.freeze({
    adapterType: "xero_accounting_event",
    defaultVersion: "1",
    supportedVersions: Object.freeze(["1"]),
    semanticKind: "accounting_event",
    canonicalProjection: "mapped_canonical_row",
    sourceGroupSemantics: "scoped_accounting_ledger",
    requiredFields: COMMON_PAYMENT_FIELDS,
    requiredAnyGroups: Object.freeze([
      Object.freeze({
        id: "payment_clock_start",
        fields: PAYMENT_CLOCK_FIELDS,
      }),
    ]),
    optionalFields: Object.freeze(["supplier_has_no_abn"]),
    validateRows: false,
  }),
  direct_payment: Object.freeze({
    adapterType: "direct_payment",
    defaultVersion: "1",
    supportedVersions: Object.freeze(["1"]),
    semanticKind: "direct_payment",
    canonicalProjection: "mapped_canonical_row",
    sourceGroupSemantics: "provenance_only_no_event_reconstruction",
    requiredFields: COMMON_PAYMENT_FIELDS,
    requiredAnyGroups: Object.freeze([
      Object.freeze({
        id: "payment_clock_start",
        fields: PAYMENT_CLOCK_FIELDS,
      }),
    ]),
    optionalFields: Object.freeze([
      "payer_entity_acn_arbn",
      "payee_entity_acn_arbn",
      "supply_date",
      "notice_for_payment_issue_date",
      "invoice_issue_date",
      "invoice_receipt_date",
      "invoice_due_date",
      "contract_po_reference_number",
      "contract_po_payment_terms",
      "notice_for_payment_terms",
      "invoice_payment_terms",
      "payment_term",
      "payment_term_days",
      "description",
      "company_code",
      "document_type",
      "document_currency",
      "source_account_code",
      "supplier_has_no_abn",
      "clearing_document",
      "invoice_created_date",
      "entry_date",
      "reconciliation_status",
      "source_user",
      "trade_credit_payment",
      "excluded_trade_credit_payment",
      "peppol_einvoice_enabled",
      "rcti",
      "credit_card_payment",
      "credit_card_no",
      "partial_payment",
    ]),
    paymentAmountSemantic: "actual_settlement_amount",
    validateRows: true,
  }),
});

function getCanonicalAdapterContract(adapterType) {
  return ADAPTER_CONTRACTS[String(adapterType || "").trim()] || null;
}

function normaliseMappedFields(fieldMap) {
  return new Set(
    (Array.isArray(fieldMap) ? fieldMap : [])
      .map((row) => toSnake(row?.canonicalField))
      .filter(Boolean),
  );
}

function validateAdapterMappings({
  contract,
  fieldMap,
  datasetId,
  systemSuppliedFields = [],
}) {
  const mappedFields = normaliseMappedFields(fieldMap);
  for (const field of systemSuppliedFields) mappedFields.add(toSnake(field));
  const missingFields = contract.requiredFields.filter(
    (field) => !mappedFields.has(field),
  );
  const missingGroups = contract.requiredAnyGroups
    .filter((group) => !group.fields.some((field) => mappedFields.has(field)))
    .map((group) => ({ id: group.id, fields: [...group.fields] }));

  if (!missingFields.length && !missingGroups.length) return;

  const error = new Error(
    `Canonical mappings are incomplete for ${contract.adapterType} dataset ${datasetId}`,
  );
  error.statusCode = 400;
  error.code = "CANONICAL_MAPPING_INCOMPLETE";
  error.details = { datasetId, missingFields, missingGroups };
  throw error;
}

function hasValue(value) {
  return value != null && String(value).trim() !== "";
}

function isSupportedDate(value) {
  if (!hasValue(value)) return false;
  if (value instanceof Date) return !Number.isNaN(value.getTime());
  const text = String(value).trim();
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(text);
  if (!match) return false;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const date = new Date(Date.UTC(year, month - 1, day));
  return (
    date.getUTCFullYear() === year &&
    date.getUTCMonth() === month - 1 &&
    date.getUTCDate() === day
  );
}

function isNumericPaymentAmount(value) {
  if (!hasValue(value)) return false;
  const text = String(value).trim();
  return (
    /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(text) ||
    /^[+-]?\d{1,3}(?:,\d{3})+(?:\.\d+)?$/.test(text)
  );
}

function validateCanonicalRowForAdapter({
  contract,
  row,
  datasetId,
  sourceRowNo,
}) {
  if (!contract.validateRows) return;

  const missingFields = contract.requiredFields.filter(
    (field) => !hasValue(row?.[field]),
  );
  const missingGroups = contract.requiredAnyGroups
    .filter((group) => !group.fields.some((field) => hasValue(row?.[field])))
    .map((group) => ({ id: group.id, fields: [...group.fields] }));
  const invalidFields = [];
  if (
    hasValue(row?.payment_amount) &&
    !isNumericPaymentAmount(row.payment_amount)
  ) {
    invalidFields.push("payment_amount");
  }
  for (const field of CANONICAL_DATE_FIELDS) {
    if (hasValue(row?.[field]) && !isSupportedDate(row[field])) {
      invalidFields.push(field);
    }
  }

  if (!missingFields.length && !missingGroups.length && !invalidFields.length) {
    return;
  }

  const error = new Error(
    `Invalid direct-payment canonical row ${sourceRowNo} in dataset ${datasetId}: ${[
      missingFields.length
        ? `missing fields ${missingFields.join(", ")}`
        : null,
      missingGroups.length
        ? `missing field groups ${missingGroups.map((group) => group.id).join(", ")}`
        : null,
      invalidFields.length
        ? `invalid fields ${Array.from(new Set(invalidFields)).join(", ")}`
        : null,
    ]
      .filter(Boolean)
      .join("; ")}`,
  );
  error.statusCode = 422;
  error.code = "DIRECT_PAYMENT_CANONICAL_ROW_INVALID";
  error.details = {
    datasetId,
    sourceRowNo,
    missingFields,
    missingGroups,
    invalidFields: Array.from(new Set(invalidFields)),
    invalidFieldDetails: Array.from(new Set(invalidFields)).map((field) => ({
      field,
      reason:
        row?._ptrsMeta?.canonicalSources?.[field]?.normalisation?.error ||
        (field === "payment_amount"
          ? "INVALID_CANONICAL_NUMBER"
          : "INVALID_CANONICAL_DATE"),
    })),
  };
  throw error;
}

module.exports = {
  ADAPTER_CONTRACTS,
  COMMON_PAYMENT_FIELDS,
  PAYMENT_CLOCK_FIELDS,
  getCanonicalAdapterContract,
  validateAdapterMappings,
  validateCanonicalRowForAdapter,
};

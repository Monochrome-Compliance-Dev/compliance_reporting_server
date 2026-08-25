function snakeToCamel(value) {
  if (!value) return "";
  return String(value).replace(/_([a-z])/g, (_, ch) => ch.toUpperCase());
}

function toSnakeCase(value) {
  if (!value) return "";
  return String(value)
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/[^a-zA-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function collectCanonicalContractFields(contract) {
  if (!contract || typeof contract !== "object") return [];

  const sections = [
    contract.identity,
    contract.transaction,
    contract.dates,
    contract.terms,
    contract.operational_source_fields,
    contract.regulator_flags,
  ];

  const out = [];
  const seen = new Set();

  for (const section of sections) {
    if (!section || typeof section !== "object") continue;
    for (const key of Object.keys(section)) {
      const field = toSnakeCase(key);
      if (!field || seen.has(field)) continue;
      seen.add(field);
      out.push(field);
    }
  }

  return out;
}

function readStageFieldValue(row, field) {
  if (!row || typeof row !== "object" || !field) return undefined;

  if (Object.prototype.hasOwnProperty.call(row, field)) {
    return row[field];
  }

  const camel = snakeToCamel(field);
  if (camel && Object.prototype.hasOwnProperty.call(row, camel)) {
    return row[camel];
  }

  return undefined;
}

function buildPersistedStageRow(row, allowedFields) {
  const out = {};
  const fields = Array.isArray(allowedFields) ? allowedFields : [];

  for (const field of fields) {
    if (!field) continue;
    const value = readStageFieldValue(row, field);
    out[field] = typeof value === "undefined" ? null : value;
  }

  return out;
}

const STAGE_COLUMN_SOURCES = Object.freeze({
  payerEntityName: ["payer_entity_name"],
  payerEntityAbn: ["payer_entity_abn"],
  payeeEntityName: ["payee_entity_name"],
  payeeEntityAbn: ["payee_entity_abn"],
  payeeEntityAbnValid: ["payee_entity_abn_valid"],
  invoiceReferenceNumber: ["invoice_reference_number"],
  sourceAccountCode: ["source_account_code"],
  description: ["description"],
  documentType: ["document_type"],
  documentCurrency: ["document_currency"],
  clearingDocument: ["clearing_document"],
  reconciliationStatus: ["reconciliation_status"],
  sourceUser: ["source_user"],
  paymentAmount: ["payment_amount"],
  paymentDate: ["payment_date"],
  invoiceIssueDate: ["invoice_issue_date"],
  invoiceReceiptDate: ["invoice_receipt_date"],
  invoiceDueDate: ["invoice_due_date"],
  invoiceCreatedDate: ["invoice_created_date"],
  entryDate: ["entry_date"],
  paymentTermRaw: [
    "contract_po_payment_terms_effective",
    "invoice_payment_terms_effective",
    "invoice_payment_terms_raw",
    "invoice_payment_terms",
    "payment_term",
    "contract_po_payment_terms",
    "notice_for_payment_terms",
  ],
  paymentTermDays: ["payment_term_days"],
  paymentTimeDays: ["payment_time_days"],
  tradeCreditPayment: ["trade_credit_payment"],
  excludedTradeCreditPayment: ["excluded_trade_credit_payment"],
  excludeReason: ["exclude_reason"],
});

function getStageColumnForCanonicalField(canonicalField) {
  const field = String(canonicalField || "").trim();
  if (!field) return null;

  for (const [physicalField, canonicalFields] of Object.entries(
    STAGE_COLUMN_SOURCES,
  )) {
    if (canonicalFields.includes(field)) return physicalField;
  }

  return null;
}

const STAGE_DATE_COLUMNS = new Set([
  "paymentDate",
  "invoiceIssueDate",
  "invoiceReceiptDate",
  "invoiceDueDate",
  "invoiceCreatedDate",
  "entryDate",
]);

const INTEGER_TYPE_KEYS = new Set([
  "TINYINT",
  "SMALLINT",
  "MEDIUMINT",
  "INTEGER",
  "BIGINT",
]);
const DECIMAL_TYPE_KEYS = new Set([
  "DECIMAL",
  "NUMERIC",
  "FLOAT",
  "REAL",
  "DOUBLE",
]);

function getNumericStageColumnKind(attribute) {
  const typeKey = String(
    attribute?.type?.key || attribute?.type?.constructor?.key || "",
  ).toUpperCase();

  if (INTEGER_TYPE_KEYS.has(typeKey)) return "integer";
  if (DECIMAL_TYPE_KEYS.has(typeKey)) return "decimal";
  return null;
}

function normaliseNumericStageValue(value, { physicalField, kind }) {
  if (value == null || String(value).trim() === "") return null;

  if (typeof value === "number") {
    if (
      Number.isFinite(value) &&
      (kind !== "integer" || Number.isSafeInteger(value))
    ) {
      return value;
    }
  } else if (typeof value === "string") {
    const trimmed = value.trim();
    const plainNumber = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/;
    const groupedNumber = /^[+-]?\d{1,3}(?:,\d{3})+(?:\.\d+)?$/;

    if (plainNumber.test(trimmed) || groupedNumber.test(trimmed)) {
      const normalised = trimmed.replace(/,/g, "");
      if (kind !== "integer") return normalised;

      const integer = Number(normalised);
      if (Number.isSafeInteger(integer)) return integer;
    }
  }

  throw new TypeError(
    `Invalid numeric value for PTRS stage column "${physicalField}": ${JSON.stringify(
      value,
    )}`,
  );
}

function readFirstStageFieldValue(row, fields) {
  for (const field of fields || []) {
    const value = readStageFieldValue(row, field);
    if (value == null || String(value).trim() === "") continue;
    return value;
  }
  return null;
}

function buildStageColumnProjection(row, model) {
  const out = {};
  const modelAttributes = model?.rawAttributes || {};

  for (const [physicalField, canonicalFields] of Object.entries(
    STAGE_COLUMN_SOURCES,
  )) {
    if (!Object.prototype.hasOwnProperty.call(modelAttributes, physicalField)) {
      continue;
    }

    const value = readFirstStageFieldValue(row, canonicalFields);
    const numericKind = getNumericStageColumnKind(
      modelAttributes[physicalField],
    );
    if (STAGE_DATE_COLUMNS.has(physicalField)) {
      out[physicalField] = parseISODateOnly(value)?.iso || null;
    } else if (numericKind) {
      out[physicalField] = normaliseNumericStageValue(value, {
        physicalField,
        kind: numericKind,
      });
    } else {
      out[physicalField] = value;
    }
  }

  return out;
}

function getFirstRowValue(row, keys) {
  if (!row || typeof row !== "object" || !Array.isArray(keys)) return null;
  for (const k of keys) {
    if (!k) continue;
    if (!Object.prototype.hasOwnProperty.call(row, k)) continue;
    const v = row[k];
    if (v == null) continue;
    const s = String(v).trim();
    if (s) return v;
  }
  return null;
}

function parseISODateOnly(value) {
  if (!value) return null;
  const s = String(value).trim();
  if (!s) return null;

  const au = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(s);
  const datePart = au
    ? `${au[3]}-${String(au[2]).padStart(2, "0")}-${String(au[1]).padStart(2, "0")}`
    : s.includes("T")
    ? s.split("T")[0]
    : s.includes(" ")
      ? s.split(" ")[0]
      : s;
  const m = /^\d{4}-\d{2}-\d{2}$/.test(datePart) ? datePart : null;
  if (!m) return null;

  const [y, mo, d] = datePart.split("-").map((x) => Number(x));
  if (!Number.isFinite(y) || !Number.isFinite(mo) || !Number.isFinite(d)) {
    return null;
  }

  const ms = Date.UTC(y, mo - 1, d);
  if (!Number.isFinite(ms)) return null;
  const parsed = new Date(ms);
  if (
    parsed.getUTCFullYear() !== y ||
    parsed.getUTCMonth() !== mo - 1 ||
    parsed.getUTCDate() !== d
  ) {
    return null;
  }
  return { y, mo, d, ms, iso: datePart };
}

function diffDaysUTC(later, earlier) {
  if (!later || !earlier) return null;
  const ms = later.ms - earlier.ms;
  if (!Number.isFinite(ms)) return null;
  return Math.floor(ms / (24 * 60 * 60 * 1000));
}

function isRctiYes(value) {
  if (value == null) return false;
  const s = String(value).trim().toLowerCase();
  return s === "yes" || s === "y" || s === "true";
}

function computePaymentTimeRegulator(row) {
  if (!row || typeof row !== "object") {
    return { days: null, referenceDate: null, referenceKind: null };
  }

  const paymentRaw = getFirstRowValue(row, [
    "payment_date",
    "paymentDate",
    "payment_date_iso",
    "paymentDateIso",
  ]);
  const payment = parseISODateOnly(paymentRaw);
  if (!payment) {
    return { days: null, referenceDate: null, referenceKind: null };
  }

  const issueRaw = getFirstRowValue(row, [
    "invoice_issue_date",
    "invoiceIssueDate",
  ]);
  const issue = parseISODateOnly(issueRaw);

  const receiptRaw = getFirstRowValue(row, [
    "invoice_receipt_date",
    "invoiceReceiptDate",
  ]);
  const receipt = parseISODateOnly(receiptRaw);

  const noticeRaw = getFirstRowValue(row, [
    "notice_for_payment_issue_date",
    "noticeForPaymentIssueDate",
  ]);
  const notice = parseISODateOnly(noticeRaw);

  const supplyRaw = getFirstRowValue(row, ["supply_date", "supplyDate"]);
  const supply = parseISODateOnly(supplyRaw);

  const dueRaw = getFirstRowValue(row, [
    "invoice_due_date",
    "invoiceDueDate",
    "due_date",
    "dueDate",
  ]);
  const due = parseISODateOnly(dueRaw);

  const rctiRaw = getFirstRowValue(row, ["rcti", "RCTI"]);
  const rcti = isRctiYes(rctiRaw);

  let calc = null;
  let ref = null;

  if (rcti) {
    if (!issue) {
      return { days: null, referenceDate: null, referenceKind: null };
    }
    calc = diffDaysUTC(payment, issue);
    ref = { referenceDate: issue.iso, referenceKind: "invoice_issue" };
  } else if (!issue && !notice) {
    if (supply) {
      calc = diffDaysUTC(payment, supply);
      ref = { referenceDate: supply.iso, referenceKind: "supply" };
    } else if (due) {
      calc = diffDaysUTC(payment, due);
      ref = { referenceDate: due.iso, referenceKind: "invoice_due" };
    } else {
      return { days: null, referenceDate: null, referenceKind: null };
    }
  } else if (!issue) {
    if (!notice) {
      return { days: null, referenceDate: null, referenceKind: null };
    }
    calc = diffDaysUTC(payment, notice);
    ref = { referenceDate: notice.iso, referenceKind: "notice_for_payment" };
  } else {
    const dIssue = diffDaysUTC(payment, issue);
    const dReceipt = receipt ? diffDaysUTC(payment, receipt) : null;

    if (dReceipt == null || !Number.isFinite(dReceipt)) {
      calc = dIssue;
      ref = { referenceDate: issue.iso, referenceKind: "invoice_issue" };
    } else {
      if (dIssue == null || !Number.isFinite(dIssue)) {
        calc = dReceipt;
        ref = { referenceDate: receipt.iso, referenceKind: "invoice_receipt" };
      } else if (dIssue <= dReceipt) {
        calc = dIssue;
        ref = { referenceDate: issue.iso, referenceKind: "invoice_issue" };
      } else {
        calc = dReceipt;
        ref = { referenceDate: receipt.iso, referenceKind: "invoice_receipt" };
      }
    }
  }

  if (calc == null || !Number.isFinite(calc)) {
    return { days: null, referenceDate: null, referenceKind: null };
  }

  const days = calc <= 0 ? 0 : calc + 1;

  return {
    days,
    referenceDate: ref?.referenceDate || null,
    referenceKind: ref?.referenceKind || null,
  };
}

module.exports = {
  snakeToCamel,
  toSnakeCase,
  collectCanonicalContractFields,
  readStageFieldValue,
  buildPersistedStageRow,
  buildStageColumnProjection,
  STAGE_COLUMN_SOURCES,
  getStageColumnForCanonicalField,
  getNumericStageColumnKind,
  normaliseNumericStageValue,
  getFirstRowValue,
  parseISODateOnly,
  diffDaysUTC,
  isRctiYes,
  computePaymentTimeRegulator,
};

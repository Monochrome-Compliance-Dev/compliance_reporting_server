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

  const datePart = s.includes("T")
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
  getFirstRowValue,
  parseISODateOnly,
  diffDaysUTC,
  isRctiYes,
  computePaymentTimeRegulator,
};

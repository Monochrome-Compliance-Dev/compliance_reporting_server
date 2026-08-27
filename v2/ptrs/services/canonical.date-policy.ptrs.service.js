const SAP_ACCOUNTING_EVENT = "sap_accounting_event";
const INVOICE_SOURCE_ROLE = "invoices";
const SAP_INVOICE_DATE_POLICY_VERSION = "sap-invoice-created-preferred-v1";

function hasDateValue(value) {
  return value != null && String(value).trim() !== "";
}

function applyCanonicalInvoiceDatePolicy({ row, adapterType }) {
  if (!row || typeof row !== "object") return row;
  if (String(adapterType || "").trim() !== SAP_ACCOUNTING_EVENT) return row;

  const out = { ...row };
  const meta =
    out._ptrsMeta && typeof out._ptrsMeta === "object"
      ? { ...out._ptrsMeta }
      : {};
  const canonicalSources = {
    ...(meta.canonicalSources || {}),
  };
  const receiptSource = canonicalSources.invoice_receipt_date || null;
  const hasInvoiceSourceReceipt =
    hasDateValue(out.invoice_receipt_date) &&
    String(receiptSource?.sourceRole || "")
      .trim()
      .toLowerCase() === INVOICE_SOURCE_ROLE;

  if (hasInvoiceSourceReceipt) {
    out.invoice_issue_date = out.invoice_receipt_date;
    canonicalSources.invoice_issue_date = { ...receiptSource };
  } else if (hasDateValue(out.invoice_issue_date)) {
    out.invoice_receipt_date = out.invoice_issue_date;
    const issueSource = canonicalSources.invoice_issue_date || null;
    if (issueSource) {
      canonicalSources.invoice_receipt_date = { ...issueSource };
    }
  }

  out._ptrsMeta = {
    ...meta,
    canonicalSources,
    invoiceDatePolicy: {
      policy: "sap_invoice_created_preferred",
      effectiveDate:
        out.invoice_receipt_date || out.invoice_issue_date || null,
      usedInvoiceSourceReceipt: hasInvoiceSourceReceipt,
    },
  };

  return out;
}

module.exports = {
  SAP_INVOICE_DATE_POLICY_VERSION,
  applyCanonicalInvoiceDatePolicy,
};

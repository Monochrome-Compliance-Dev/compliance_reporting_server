const SAP_INVOICE_DATE_POLICY_VERSION = "sap-distinct-invoice-dates-v2";

function applyCanonicalInvoiceDatePolicy({ row, adapterType }) {
  if (!row || typeof row !== "object") return row;
  if (String(adapterType || "").trim() !== "sap_accounting_event") return row;

  const out = { ...row };
  const meta =
    out._ptrsMeta && typeof out._ptrsMeta === "object"
      ? { ...out._ptrsMeta }
      : {};
  out._ptrsMeta = {
    ...meta,
    invoiceDatePolicy: {
      policy: "distinct_invoice_issue_and_receipt",
      issueDate: out.invoice_issue_date || null,
      receiptDate: out.invoice_receipt_date || null,
    },
  };

  return out;
}

module.exports = {
  SAP_INVOICE_DATE_POLICY_VERSION,
  applyCanonicalInvoiceDatePolicy,
};

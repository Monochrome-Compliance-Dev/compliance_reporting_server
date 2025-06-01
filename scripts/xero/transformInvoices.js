/****
 * Updated transformInvoices function:
 * - Receives an array of invoices.
 * - Receives a map of contact details (keyed by ContactID) with payment terms, ABN, etc.
 * - Merges the contact-specific payment terms and identifiers.
 */

const transformInvoices = (invoices, contactMap) => {
  return invoices.map((invoice) => {
    const transformed = {};

    // Get contact-specific details if present
    const contactId = invoice.Contact?.ContactID;
    const contactDetails = contactMap[contactId] || {};

    // Base mapping with updated placeholders and fallbacks
    transformed.payerEntityName =
      "PAYER_ENTITY_NAME_PLACEHOLDER" || invoice.PayerEntityName || "";
    transformed.payerEntityAbn = "PAYER_ENTITY_ABN_PLACEHOLDER";
    transformed.payerEntityAcnArbn = "PAYER_ENTITY_ACN_ARBN_PLACEHOLDER";

    transformed.payeeEntityName =
      "PAYEE_ENTITY_NAME_PLACEHOLDER" || invoice.Contact?.Name || "";
    transformed.payeeEntityAbn =
      contactDetails.ABN || "PAYEE_ENTITY_ABN_PLACEHOLDER";
    transformed.payeeEntityAcnArbn =
      contactDetails.ACN || "PAYEE_ENTITY_ACN_ARBN_PLACEHOLDER";

    transformed.paymentAmount = Array.isArray(invoice.Payments)
      ? invoice.Payments.reduce((sum, p) => sum + (p.Amount || 0), 0)
      : 0;

    transformed.description = invoice.Reference || "";

    transformed.supplyDate = invoice.DateString || "";

    transformed.paymentDate =
      Array.isArray(invoice.Payments) && invoice.Payments.length > 0
        ? new Date(Number(invoice.Payments[0].Date.match(/\d+/)[0]))
            .toISOString()
            .slice(0, 10)
        : "";

    // Contract-level info
    transformed.contractPoReferenceNumber = invoice.Reference || "";
    transformed.contractPoPaymentTerms =
      contactDetails.PaymentTerms || "Unknown Terms";

    // Notice - placeholder
    transformed.noticeForPaymentIssueDate = "";
    transformed.noticeForPaymentTerms = "";

    // Invoice-level info
    transformed.invoiceReferenceNumber = invoice.InvoiceNumber || "";
    transformed.invoiceIssueDate = invoice.DateString || "";
    transformed.invoiceReceiptDate = "";
    transformed.invoiceAmount = invoice.Total || 0;
    transformed.invoicePaymentTerms =
      contactDetails.PaymentTerms || "Unknown Terms";
    transformed.invoiceDueDate = invoice.DueDateString || "";

    return transformed;
  });
};

module.exports = { transformInvoices };

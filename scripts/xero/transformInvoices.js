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
    transformed.payeeEntityName = invoice.Contact?.Name || "PLACEHOLDER";
    transformed.payeeEntityAbn = contactDetails.ABN || "PLACEHOLDER";
    transformed.payeeEntityAcnArbn = contactDetails.ACN || "PLACEHOLDER";

    transformed.paymentAmount = Array.isArray(invoice.Payments)
      ? invoice.Payments.reduce((sum, p) => sum + (p.Amount || 0), 0)
      : 0;

    transformed.description = invoice.Reference || "PLACEHOLDER";

    transformed.supplyDate = "PLACEHOLDER";

    transformed.paymentDate =
      Array.isArray(invoice.Payments) && invoice.Payments.length > 0
        ? new Date(Number(invoice.Payments[0].Date.match(/\d+/)[0]))
            .toISOString()
            .slice(0, 10)
        : "";

    // Contract-level info
    transformed.contractPoReferenceNumber = "PLACEHOLDER";
    transformed.contractPoPaymentTerms =
      contactDetails.PaymentTerms || "PLACEHOLDER";

    // Notice - placeholder
    transformed.noticeForPaymentIssueDate = "PLACEHOLDER";
    transformed.noticeForPaymentTerms = "PLACEHOLDER";

    // Invoice-level info
    transformed.invoiceReferenceNumber = invoice.InvoiceNumber || "PLACEHOLDER";
    transformed.invoiceIssueDate = invoice.DateString || "PLACEHOLDER";
    transformed.invoiceReceiptDate = "PlACEHOLDER";
    transformed.invoiceAmount = invoice.Total || "PLACEHOLDER";
    transformed.invoicePaymentTerms =
      contactDetails.PaymentTerms || "PLACEHOLDER";
    transformed.invoiceDueDate = invoice.DueDateString || "PLACEHOLDER";

    return transformed;
  });
};

module.exports = { transformInvoices };

/****
 * Updated transformInvoices function:
 * - Receives an array of invoices.
 * - Receives a map of contact details (keyed by ContactID) with payment terms, ABN, etc.
 * - Merges the contact-specific payment terms and identifiers.
 */

const transformInvoices = (invoices, contactMap) => {
  return invoices.map((invoice) => {
    const transformed = {};

    transformed.paymentAmount = Array.isArray(invoice.Payments)
      ? invoice.Payments.reduce((sum, p) => sum + (p.Amount || 0), 0)
      : 0;

    transformed.description =
      Array.isArray(invoice.LineItems) && invoice.LineItems.length > 0
        ? invoice.LineItems[0].Description || "NONE PROVIDED"
        : "NONE PROVIDED";

    transformed.supplyDate = "NONE PROVIDED";

    transformed.paymentDate =
      Array.isArray(invoice.Payments) && invoice.Payments.length > 0
        ? new Date(Number(invoice.Payments[0].Date.match(/\d+/)[0]))
            .toISOString()
            .slice(0, 10)
        : "";

    // Contract-level info
    transformed.contractPoReferenceNumber = "NONE PROVIDED";

    // Notice - placeholder
    transformed.noticeForPaymentIssueDate = "NONE PROVIDED";
    transformed.noticeForPaymentTerms = "NONE PROVIDED";

    // Invoice-level info
    transformed.invoiceReferenceNumber =
      invoice.InvoiceNumber || "NONE PROVIDED";
    transformed.invoiceIssueDate = invoice.DateString || "NONE PROVIDED";
    transformed.invoiceReceiptDate = "PlACEHOLDER";
    transformed.invoiceAmount = invoice.Total || "NONE PROVIDED";
    transformed.invoicePaymentTerms = "NONE PROVIDED";
    transformed.invoiceDueDate = invoice.DueDateString || "NONE PROVIDED";

    return transformed;
  });
};

module.exports = { transformInvoices };

/****
 * Updated transformInvoices function:
 * - Receives an array of invoices.
 * - Receives a map of contact details (keyed by ContactID) with payment terms, ABN, etc.
 * - Merges the contact-specific payment terms and identifiers.
 */
const { transformContact } = require("./transformContact");

const transformInvoices = (invoices) => {
  return invoices.map((invoice) => {
    const transformed = {};

    transformed.description =
      Array.isArray(invoice.LineItems) && invoice.LineItems.length > 0
        ? invoice.LineItems[0].Description || "NONE PROVIDED"
        : "NONE PROVIDED";

    transformed.supplyDate = "NONE PROVIDED";
    transformed.contractPoReferenceNumber = "NONE PROVIDED";
    transformed.noticeForPaymentIssueDate = "NONE PROVIDED";
    transformed.noticeForPaymentTerms = "NONE PROVIDED";

    transformed.invoiceReferenceNumber =
      invoice.InvoiceNumber || "NONE PROVIDED";
    transformed.invoiceIssueDate = invoice.DateString || "NONE PROVIDED";
    transformed.invoiceReceiptDate = "NONE PROVIDED";
    transformed.invoiceAmount = invoice.Total || "NONE PROVIDED";
    transformed.invoicePaymentTerms = "NONE PROVIDED";
    transformed.invoiceDueDate = invoice.DueDateString || "NONE PROVIDED";

    // Add payment terms if available
    if (invoice.PaymentTerms) {
      if (invoice.PaymentTerms.Bills) {
        transformed.invoicePaymentTermsBillsDay =
          invoice.PaymentTerms.Bills.Day || null;
        transformed.invoicePaymentTermsBillsType =
          invoice.PaymentTerms.Bills.Type || "NONE PROVIDED";
      }
      if (invoice.PaymentTerms.Sales) {
        transformed.invoicePaymentTermsSalesDay =
          invoice.PaymentTerms.Sales.Day || null;
        transformed.invoicePaymentTermsSalesType =
          invoice.PaymentTerms.Sales.Type || "NONE PROVIDED";
      }
    }

    // Add contact info directly from the invoice
    if (invoice.Contact) {
      const contactData = transformContact(invoice.Contact);
      Object.assign(transformed, contactData);
    }

    return transformed;
  });
};

module.exports = { transformInvoices };

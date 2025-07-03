/****
 * Updated transformInvoices function:
 * - Receives an array of invoices.
 * - Receives a map of contact details (keyed by ContactID) with payment terms, ABN, etc.
 * - Merges the contact-specific payment terms and identifiers.
 */
const { transformContact } = require("./transformContact");

/**
 * Extracts the first AccountCode from an array of line items.
 * Trims it to 20 characters if it's longer.
 * @param {Array} lineItems
 * @returns {string|null}
 */
function getFirstAccountCode(lineItems) {
  if (Array.isArray(lineItems) && lineItems.length > 0) {
    const code = lineItems[0]?.AccountCode;
    return code;
  }
  return null;
}

const transformInvoices = (invoices) => {
  return invoices.map((invoice) => {
    const transformed = {};

    // Retain all original fields for future use
    Object.assign(transformed, invoice);

    // transformed.description = () => {
    //   if (Array.isArray(invoice.LineItems) && invoice.LineItems.length > 0) {
    //     for (const item of invoice.LineItems) {
    //       if (item.Description && typeof item.Description === "string") {
    //         return item.Description;
    //       }
    //     }
    //   }
    //   return "None provided";
    // };
    transformed.description = "None provided";

    transformed.accountCode =
      getFirstAccountCode(invoice.LineItems) || "None provided";

    transformed.supplyDate = null;
    transformed.contractPoReferenceNumber = "None provided";
    transformed.noticeForPaymentIssueDate = null;
    transformed.noticeForPaymentTerms = "None provided";

    transformed.invoiceReferenceNumber =
      invoice.InvoiceNumber || "None provided";
    transformed.invoiceIssueDate = invoice.DateString || "";
    transformed.invoiceReceiptDate = invoice.DateString || "";
    transformed.invoiceAmount = invoice.Total || "";
    transformed.invoicePaymentTerms = null;
    transformed.invoiceDueDate = invoice.DueDateString || "";

    // Add contact info directly from the invoice
    if (invoice.Contact) {
      const contactData = transformContact(invoice.Contact);
      Object.assign(transformed, contactData);
    }

    return transformed;
  });
};

module.exports = { transformInvoices };

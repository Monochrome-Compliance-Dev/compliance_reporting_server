// const xeroData = require("../../docs/xeroApiDump.json");

const { logger } = require("../../helpers/logger");
const { transformOrganisations } = require("./transformOrganisations");
const { transformContact } = require("./transformContact");
const { transformPayments } = require("./transformPayments");
const { transformInvoices } = require("./transformInvoices");

async function transformXeroData(xeroData) {
  logger.logEvent("info", "Starting Xero data transformation");

  const transformedData = {
    organisations: [],
    contacts: [],
    invoices: [],
    payments: [],
  };

  if (xeroData.organisations) {
    transformedData.organisations = transformOrganisations(
      xeroData.organisations
    );
    logger.logEvent(
      "info",
      `Transformed ${transformedData.organisations.length} organisations`
    );
  }

  if (xeroData.contacts) {
    transformedData.contacts = xeroData.contacts.map((c) =>
      transformContact(c)
    );
    logger.logEvent(
      "info",
      `Transformed ${transformedData.contacts.length} contacts`
    );
  }

  if (xeroData.invoices) {
    transformedData.invoices = transformInvoices(xeroData.invoices);
    logger.logEvent(
      "info",
      `Transformed ${transformedData.invoices.length} invoices`
    );
  }

  if (xeroData.payments) {
    transformedData.payments = transformPayments(xeroData.payments);
    logger.logEvent(
      "info",
      `Transformed ${transformedData.payments.length} payments`
    );
  }

  const mergedRecords = [];

  const org = Array.isArray(transformedData.organisations)
    ? transformedData.organisations[0]
    : transformedData.organisations;
  // console.log("Organisation Data:", org);

  transformedData.payments.forEach((payment) => {
    const invoice = transformedData.invoices.find(
      (inv) => inv.InvoiceID === payment.Invoice.InvoiceID
    );
    if (!invoice) return;
    // console.log("Invoice Data:", invoice);

    const contact = transformedData.contacts.find(
      (c) => c.ContactID === invoice.Contact.ContactID
    );

    const rawRecord = {
      payerEntityName: org?.payerEntityName || null,
      payerEntityAbn: org?.payerEntityAbn || null,
      payerEntityAcnArbn: org?.payerEntityAcnArbn || null,

      payeeEntityName: contact?.payeeEntityName || null,
      payeeEntityAbn: contact?.payeeEntityAbn || null,
      payeeEntityAcnArbn: contact?.payeeEntityAcnArbn || null,

      paymentAmount: payment?.paymentAmount || null,
      description: invoice?.Reference || null,
      supplyDate: invoice?.SupplyDate || null,
      paymentDate: payment?.paymentDate || null,

      contractPoReferenceNumber: null,
      contractPoPaymentTerms: contact?.contractPoPaymentTerms || null,

      noticeForPaymentIssueDate: null,
      noticeForPaymentTerms: null,

      invoiceReferenceNumber: invoice?.InvoiceNumber || null,
      invoiceIssueDate: invoice?.invoiceIssueDate || null,
      invoiceReceiptDate: invoice?.DateString || null,
      invoiceAmount: invoice?.Total || null,
      invoicePaymentTerms: invoice?.PaymentTerms || null,
      invoiceDueDate: invoice?.DueDateString || null,
    };

    // console.log("Raw Record:", rawRecord);
    mergedRecords.push(rawRecord);
  });

  logger.logEvent("info", "Xero data transformation complete");
  // console.log("Transformed Xero Data:", mergedRecords.slice(0, 20));
  return mergedRecords;
}

module.exports = { transformXeroData };

// transformXeroData(xeroData).then(() => console.log("Transform complete."));

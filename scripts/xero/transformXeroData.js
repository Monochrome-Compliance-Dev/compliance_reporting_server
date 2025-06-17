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
      `Transformed organisations ${transformedData.organisations}`
      // `Transformed ${transformedData.organisations.length} organisations`
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
    if (payment.Status === "DELETED") return;

    const invoice = transformedData.invoices.find(
      (inv) => inv.InvoiceID === payment.Invoice.InvoiceID
    );
    if (!invoice || invoice.Type === "ACCREC") return;

    const contact = transformedData.contacts.find(
      (c) => c.ContactID === invoice.Contact.ContactID
    );

    // console.log("-----------org:", org);
    // console.log("invoice:", invoice);
    // console.log("contact:", contact);
    // console.log("payment:", payment);

    const rawRecord = {
      payerEntityAbn: org.payerEntityAbn,
      payerEntityAcnArbn: org.payerEntityAcnArbn,
      payerEntityName: org.payerEntityName,
      payeeEntityAbn: contact.payeeEntityAbn,
      payeeEntityAcnArbn: contact.payeeEntityAcnArbn,
      payeeEntityName: contact.payeeEntityName,
      paymentAmount: payment.Amount,
      description: invoice.description,
      supplyDate: invoice.supplyDate,
      paymentDate: payment.paymentDate,
      contractPoReferenceNumber: invoice.contractPoReferenceNumber,
      contractPoPaymentTerms: contact.contractPoPaymentTerms,
      noticeForPaymentIssueDate: invoice.noticeForPaymentIssueDate,
      noticeForPaymentTerms: invoice.noticeForPaymentTerms,
      invoiceReferenceNumber: invoice.invoiceReferenceNumber,
      invoiceIssueDate: invoice.invoiceIssueDate,
      invoiceReceiptDate: invoice.invoiceReceiptDate,
      invoiceAmount: invoice.invoiceAmount,
      invoicePaymentTerms: invoice.invoicePaymentTerms,
      invoiceDueDate: invoice.invoiceDueDate,
    };

    mergedRecords.push(rawRecord);
  });

  logger.logEvent("info", "Xero data transformation complete");
  // console.log("Transformed Xero Data:", mergedRecords.slice(0, 20));
  return mergedRecords;
}

module.exports = { transformXeroData };

// transformXeroData(xeroData).then(() => console.log("Transform complete."));

// const xeroData = require("../../docs/xeroApiDump.json");

const { logger } = require("../../helpers/logger");
const { transformOrganisations } = require("./transformOrganisations");
const { transformContact } = require("./transformContact");
const { transformPayments } = require("./transformPayments");
const { transformInvoices } = require("./transformInvoices");
const { transformBankTransactions } = require("./transformBankTransactions");

async function transformXeroData(xeroData) {
  logger.logEvent("info", "Starting Xero data transformation");

  const transformedData = {
    organisations: [],
    contacts: [],
    invoices: [],
    payments: [],
    bankTransactions: [],
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

  if (xeroData.bankTransactions) {
    transformedData.bankTransactions = transformBankTransactions(
      xeroData.bankTransactions
    );
    logger.logEvent(
      "info",
      `Transformed ${transformedData.bankTransactions.length} bank transactions`
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
      (inv) => inv?.InvoiceID === payment?.Invoice?.InvoiceID
    );
    if (!invoice || invoice.Type === "ACCREC") return;

    const contact = transformedData.contacts.find(
      (c) => c?.ContactID === invoice.Contact?.ContactID
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
      description: "invoice.description",
      transactionType: payment.PaymentType,
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
      isReconciled: payment.isReconciled,
      accountCode: invoice.accountCode || null,
    };

    mergedRecords.push(rawRecord);
  });
  console.log(
    "Payment merged records: ",
    mergedRecords.length,
    mergedRecords.slice(0, 5)
  );

  transformedData.bankTransactions.forEach((txn) => {
    if (txn.Status === "DELETED") return;

    const contact = transformedData.contacts.find(
      (c) => c?.ContactID === txn.Contact?.ContactID
    );
    if (!contact) {
      logger.logEvent("warn", "No matching contact for bank transaction", {
        contactId: txn.Contact?.ContactID,
        bankTransactionId: txn.BankTransactionID,
      });
      return;
    }

    const rawRecord = {
      payerEntityAbn: org.payerEntityAbn,
      payerEntityAcnArbn: org.payerEntityAcnArbn,
      payerEntityName: org.payerEntityName,
      payeeEntityAbn: contact.payeeEntityAbn || null,
      payeeEntityAcnArbn: contact.payeeEntityAcnArbn || null,
      payeeEntityName: contact.payeeEntityName || null,
      paymentAmount: txn.paymentAmount,
      description: txn.description || "No description available",
      transactionType: txn.Type,
      supplyDate: txn.supplyDate,
      paymentDate: txn.paymentDate,
      contractPoReferenceNumber: txn.contractPoReferenceNumber,
      contractPoPaymentTerms: contact.contractPoPaymentTerms || null,
      noticeForPaymentIssueDate: txn.noticeForPaymentIssueDate,
      noticeForPaymentTerms: txn.noticeForPaymentTerms,
      invoiceReferenceNumber: txn.invoiceReferenceNumber,
      invoiceIssueDate: txn.invoiceIssueDate,
      invoiceReceiptDate: txn.invoiceReceiptDate,
      invoiceAmount: txn.invoiceAmount,
      invoicePaymentTerms: txn.invoicePaymentTerms,
      invoiceDueDate: txn.invoiceDueDate,
      isReconciled: txn.isReconciled,
      accountCode: txn.accountCode || null,
    };
    mergedRecords.push(rawRecord);
  });
  console.log(
    "Txn merged records: ",
    mergedRecords.length,
    mergedRecords.slice(0, 5)
  );

  logger.logEvent("info", "Xero data transformation complete");
  // console.log("Transformed Xero Data:", mergedRecords.slice(0, 20));
  return mergedRecords;
}

module.exports = { transformXeroData };

// transformXeroData(xeroData).then(() => console.log("Transform complete."));

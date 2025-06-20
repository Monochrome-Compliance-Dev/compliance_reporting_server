/**
 * Updated transformBankTransactions function:
 * - Receives an array of bankTransactions.
 * - Transforms the .
 */

const parseDotNetDate = (input) => {
  if (typeof input === "string") {
    const match = /\/Date\((\d+)(?:[+-]\d+)?\)\//.exec(input);
    if (match && match[1]) {
      const timestamp = parseInt(match[1], 10);
      const date = new Date(timestamp);
      return isNaN(date.getTime()) ? null : date.toISOString();
    }
  }
  return null;
};

/**
 * Extracts the first AccountCode from an array of line items.
 * @param {Array} lineItems
 * @returns {string|null}
 */
function getFirstAccountCode(lineItems) {
  if (Array.isArray(lineItems) && lineItems.length > 0) {
    return lineItems[0].AccountCode || null;
  }
  return null;
}

/**
 * Extracts the first Description from an array of line items.
 * @param {Array} lineItems
 * @returns {string|null}
 */
function getFirstDescription(lineItems) {
  if (Array.isArray(lineItems) && lineItems.length > 0) {
    return lineItems[0].Description?.substring(0, 255) || null;
  }
  return null;
}

const transformBankTransactions = (bankTransactions) => {
  return bankTransactions.map((bankTransaction) => {
    const description = getFirstDescription(bankTransaction.LineItems);
    const accountCode = getFirstAccountCode(bankTransaction.LineItems);
    const paymentDate = parseDotNetDate(bankTransaction.Date);

    const transformed = {
      ...bankTransaction,
      paymentAmount: bankTransaction.Total,
      description,
      supplyDate: null,
      paymentDate,
      contractPoReferenceNumber: null,
      noticeForPaymentIssueDate: null,
      noticeForPaymentTerms: null,
      invoiceReferenceNumber: accountCode,
      invoiceIssueDate: null,
      invoiceReceiptDate: null,
      invoiceAmount: bankTransaction.Total,
      invoicePaymentTerms: null,
      invoiceDueDate: paymentDate,
      isReconciled: bankTransaction.IsReconciled,
    };

    return transformed;
  });
};

module.exports = { transformBankTransactions };

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
  return input; // Return the input as is if it doesn't match the expected format
};

/**
 * Extracts the first AccountCode from an array of line items.
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

/**
 * Extracts the first Description from an array of line items.
 * Trims it to 255 characters only if it's longer.
 * @param {Array} lineItems
 * @returns {string|null}
 */
function getFirstDescription(lineItems) {
  if (Array.isArray(lineItems) && lineItems.length > 0) {
    const desc =
      typeof lineItems[0]?.Description === "string"
        ? lineItems[0].Description
        : null;
    if (!desc) return "None provided";
    return desc.length > 255 ? desc.slice(0, 255) : desc;
  }
  return null;
}

function getFirstLineItemId(lineItems) {
  if (Array.isArray(lineItems) && lineItems.length > 0) {
    const id = lineItems[0]?.LineItemID || lineItems[0]?.AccountNumber;
    return typeof id === "string" ? id.slice(0, 255) : null;
  }
  return null;
}

const transformBankTransactions = (bankTransactions) => {
  return bankTransactions.map((bankTransaction) => {
    const description = "None provided";
    // const description = getFirstDescription(bankTransaction.LineItems);
    const accountCode = getFirstAccountCode(bankTransaction.LineItems);
    const paymentDate = parseDotNetDate(bankTransaction.Date);
    const subsInvoiceNumber = getFirstLineItemId(bankTransaction.LineItems);

    const transformed = {
      ...bankTransaction,
      paymentAmount: bankTransaction.Total,
      description: description || "No description available",
      supplyDate: null,
      paymentDate: paymentDate,
      contractPoReferenceNumber: null,
      noticeForPaymentIssueDate: null,
      noticeForPaymentTerms: null,
      invoiceReferenceNumber: subsInvoiceNumber,
      invoiceIssueDate: null,
      invoiceReceiptDate: paymentDate
        ? new Date(
            new Date(paymentDate).getTime() - 30 * 24 * 60 * 60 * 1000
          ).toISOString()
        : null,
      invoiceAmount: bankTransaction.Total,
      invoicePaymentTerms: null,
      invoiceDueDate: paymentDate,
      isReconciled: bankTransaction.IsReconciled,
      accountCode: accountCode,
      transactionType: bankTransaction.Type,
    };
    // if (description.length > 255) {
    //   console.log("-------------Transformed Bank Transaction:", transformed);
    // }

    return transformed;
  });
};

module.exports = { transformBankTransactions };

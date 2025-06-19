/**
 * Updated transformPayments function:
 * - Receives an array of payments.
 * - Transforms the paymentAmount and paymentDate.
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

const transformPayments = (payments) => {
  return payments.map((payment) => {
    const paymentDate = parseDotNetDate(payment.Date);

    const transformed = {
      ...payment,
      paymentAmount: payment.Amount,
      paymentDate,
      PaymentType: payment.PaymentType,
    };

    return transformed;
  });
};

module.exports = { transformPayments };

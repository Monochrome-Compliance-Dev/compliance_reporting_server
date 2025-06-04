/**
 * Updated transformPayments function:
 * - Receives an array of payments.
 * - Transforms the paymentAmount and paymentDate.
 */
const transformPayments = (payments) => {
  return payments.map((payment) => {
    const transformed = {};

    // paymentAmount is directly from Amount
    transformed.paymentAmount = payment.Amount || 0;

    // paymentDate is from Date (converted to ISO 8601)
    if (payment.Date) {
      const dateMatch = payment.Date.match(/\d+/);
      transformed.paymentDate = dateMatch
        ? new Date(Number(dateMatch[0])).toISOString().slice(0, 10)
        : "";
    } else {
      transformed.paymentDate = "";
    }

    return transformed;
  });
};

module.exports = { transformPayments };

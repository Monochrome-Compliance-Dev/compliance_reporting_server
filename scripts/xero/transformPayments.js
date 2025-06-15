/**
 * Updated transformPayments function:
 * - Receives an array of payments.
 * - Transforms the paymentAmount and paymentDate.
 */
const transformPayments = (payments) => {
  return payments.map((payment) => {
    const transformed = {
      ...payment,
      paymentAmount: payment.Amount || 0,
      paymentDate: payment.Date
        ? payment.Date.match(/\d+/)
          ? new Date(Number(payment.Date.match(/\d+/)[0]))
              .toISOString()
              .slice(0, 10)
          : ""
        : "",
    };

    return transformed;
  });
};

module.exports = { transformPayments };

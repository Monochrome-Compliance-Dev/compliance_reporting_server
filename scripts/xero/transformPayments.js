/**
 * Updated transformPayments function:
 * - Receives an array of payments.
 * - Transforms the paymentAmount and paymentDate.
 */

const parseDotNetDate = (input) => {
  if (typeof input === "string") {
    const match = /\/Date\((\d+)(?:[+-]\d+)?\)\//.exec(input);
    console.log("Parsing .NET date:", input, "Match:", match);
    if (match && match[1]) {
      const timestamp = parseInt(match[1], 10);
      console.log("Extracted timestamp:", timestamp);
      const date = new Date(timestamp);
      console.log("Converted date:", date);
      console.log("Is valid date:", !isNaN(date.getTime()));
      console.log("ISO String:", date.toISOString());
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
    };
    console.log(
      "Transformed Payment:",
      transformed.PaymentID,
      "Amount:",
      transformed.paymentAmount,
      "Date:",
      transformed.paymentDate
    );

    return transformed;
  });
};

module.exports = { transformPayments };

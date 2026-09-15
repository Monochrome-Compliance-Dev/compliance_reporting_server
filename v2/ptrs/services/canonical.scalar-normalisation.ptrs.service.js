const DIRECT_PAYMENT_SCALAR_NORMALISATION_VERSION =
  "direct-payment-scalar-normalisation-v1";

const UNSIGNED_PLAIN_NUMBER = /^(?:\d+(?:\.\d*)?|\.\d+)$/;
const UNSIGNED_GROUPED_NUMBER = /^\d{1,3}(?:,\d{3})+(?:\.\d+)?$/;
const SIGNED_PLAIN_NUMBER = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/;
const SIGNED_GROUPED_NUMBER = /^[+-]?\d{1,3}(?:,\d{3})+(?:\.\d+)?$/;

function normaliseCanonicalNumber(value) {
  if (value == null || String(value).trim() === "") {
    return { value: null, error: null };
  }
  if (typeof value === "number") {
    return Number.isFinite(value)
      ? { value: String(value), error: null }
      : { value, error: "INVALID_NUMBER" };
  }

  const text = String(value).trim();
  const accountingMatch = /^\((.*)\)$/.exec(text);
  if (accountingMatch) {
    const magnitude = accountingMatch[1].trim();
    if (
      !UNSIGNED_PLAIN_NUMBER.test(magnitude) &&
      !UNSIGNED_GROUPED_NUMBER.test(magnitude)
    ) {
      return { value: text, error: "UNSUPPORTED_NUMERIC_FORMAT" };
    }
    return { value: `-${magnitude.replace(/,/g, "")}`, error: null };
  }

  if (!SIGNED_PLAIN_NUMBER.test(text) && !SIGNED_GROUPED_NUMBER.test(text)) {
    return { value: text, error: "UNSUPPORTED_NUMERIC_FORMAT" };
  }

  return { value: text.replace(/^\+/, "").replace(/,/g, ""), error: null };
}

module.exports = {
  DIRECT_PAYMENT_SCALAR_NORMALISATION_VERSION,
  normaliseCanonicalNumber,
};

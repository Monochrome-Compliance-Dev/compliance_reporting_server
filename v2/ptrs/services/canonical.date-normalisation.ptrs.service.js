const DIRECT_PAYMENT_DATE_NORMALISATION_VERSION =
  "direct-payment-dataset-date-format-v1";

const CANONICAL_DATE_FIELDS = Object.freeze([
  "payment_date",
  "invoice_issue_date",
  "invoice_receipt_date",
  "notice_for_payment_issue_date",
  "supply_date",
  "invoice_due_date",
  "invoice_created_date",
  "entry_date",
]);

const SUPPORTED_DATASET_DATE_FORMATS = Object.freeze(["ISO", "MDY", "DMY"]);

function normaliseDatasetDateFormat(value) {
  const format = String(value || "")
    .trim()
    .toUpperCase();
  return SUPPORTED_DATASET_DATE_FORMATS.includes(format) ? format : null;
}

function isoDate(year, month, day) {
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return null;
  }
  return `${String(year).padStart(4, "0")}-${String(month).padStart(
    2,
    "0",
  )}-${String(day).padStart(2, "0")}`;
}

function normaliseCanonicalDate(value, dateFormat) {
  if (value == null || String(value).trim() === "") {
    return { value: null, error: null };
  }
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      return { value, error: "INVALID_DATE" };
    }
    return { value: value.toISOString().slice(0, 10), error: null };
  }

  const text = String(value).trim();
  const isoMatch = /^(\d{4})-(\d{2})-(\d{2})$/.exec(text);
  if (isoMatch) {
    const normalised = isoDate(
      Number(isoMatch[1]),
      Number(isoMatch[2]),
      Number(isoMatch[3]),
    );
    return normalised
      ? { value: normalised, error: null }
      : { value: text, error: "INVALID_DATE" };
  }

  const slashMatch = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(text);
  if (!slashMatch) return { value: text, error: "UNSUPPORTED_DATE_FORMAT" };

  const format = normaliseDatasetDateFormat(dateFormat);
  if (!format) {
    return { value: text, error: "DATASET_DATE_FORMAT_REQUIRED" };
  }
  if (format === "ISO") {
    return { value: text, error: "DATASET_DATE_FORMAT_MISMATCH" };
  }
  const first = Number(slashMatch[1]);
  const second = Number(slashMatch[2]);
  const year = Number(slashMatch[3]);
  const month = format === "MDY" ? first : second;
  const day = format === "MDY" ? second : first;
  const normalised = isoDate(year, month, day);
  return normalised
    ? { value: normalised, error: null }
    : { value: text, error: "INVALID_DATE" };
}

module.exports = {
  CANONICAL_DATE_FIELDS,
  DIRECT_PAYMENT_DATE_NORMALISATION_VERSION,
  SUPPORTED_DATASET_DATE_FORMATS,
  normaliseCanonicalDate,
  normaliseDatasetDateFormat,
};

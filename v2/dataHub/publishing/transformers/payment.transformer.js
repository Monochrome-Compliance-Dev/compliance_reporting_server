function parseDate(value) {
  if (!value) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;

  const d = new Date(trimmed);
  if (!Number.isNaN(d.getTime())) return d;

  const match = trimmed.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (!match) return null;

  const day = Number(match[1]);
  const month = Number(match[2]) - 1;
  const year = Number(match[3].length === 2 ? `20${match[3]}` : match[3]);
  const parsed = new Date(year, month, day);

  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function parseAmount(value) {
  if (value === null || value === undefined) return null;
  const cleaned = String(value)
    .trim()
    .replace(/,/g, "")
    .replace(/^\((.*)\)$/, "-$1");

  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function cleanText(value) {
  if (value === null || value === undefined) return null;
  const trimmed = String(value).trim();
  return trimmed || null;
}

function cleanAbn(value) {
  if (value === null || value === undefined) return null;
  const digits = String(value).replace(/\D/g, "");
  return digits || null;
}

function mappedValue(rawRecord, fieldMapping, fieldId) {
  const header = fieldMapping[fieldId];
  if (!header) return null;
  return rawRecord[header];
}

function buildRows({
  customerId,
  profileId,
  datasetId,
  rows,
  fieldMapping,
  userId,
}) {
  return rows.map(({ sourceRowNumber, rawRecord }) => ({
    customerId,
    profileId,
    datasetId,
    sourceRowNumber,
    paymentDate: parseDate(mappedValue(rawRecord, fieldMapping, "paymentDate")),
    paymentAmount: parseAmount(
      mappedValue(rawRecord, fieldMapping, "paymentAmount"),
    ),
    currency: cleanText(mappedValue(rawRecord, fieldMapping, "currency")),
    payeeName: cleanText(mappedValue(rawRecord, fieldMapping, "payeeName")),
    payeeAbn: cleanAbn(mappedValue(rawRecord, fieldMapping, "payeeAbn")),
    payerName: cleanText(mappedValue(rawRecord, fieldMapping, "payerName")),
    payerAbn: cleanAbn(mappedValue(rawRecord, fieldMapping, "payerAbn")),
    invoiceReference: cleanText(
      mappedValue(rawRecord, fieldMapping, "invoiceReference"),
    ),
    documentType: cleanText(
      mappedValue(rawRecord, fieldMapping, "documentType"),
    ),
    paymentTerms: cleanText(
      mappedValue(rawRecord, fieldMapping, "paymentTerms"),
    ),
    purchasingDocument: cleanText(
      mappedValue(rawRecord, fieldMapping, "purchasingDocument"),
    ),
    rawRecord,
    meta: null,
    createdBy: userId || null,
    updatedBy: userId || null,
  }));
}

module.exports = {
  datasetType: "payment",
  modelName: "DataHubPayment",
  buildRows,
};

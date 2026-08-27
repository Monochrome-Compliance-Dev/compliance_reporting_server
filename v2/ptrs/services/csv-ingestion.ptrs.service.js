const fs = require("fs");
const csv = require("fast-csv");

const DEFAULT_BATCH_SIZE = 1000;

function repairCsvHeaders(rawHeaders) {
  if (!Array.isArray(rawHeaders) || rawHeaders.length === 0) {
    const error = new Error("CSV appears to have no header row");
    error.statusCode = 400;
    throw error;
  }

  const seen = new Map();
  return rawHeaders.map((header, index) => {
    const trimmed = String(header == null ? "" : header).trim();
    const label = trimmed || `column_${index + 1}`;
    const occurrence = (seen.get(label) || 0) + 1;
    seen.set(label, occurrence);
    return occurrence === 1 ? label : `${label}_${occurrence}`;
  });
}

async function ingestCsvFileInBatches({
  filePath,
  batchSize = DEFAULT_BATCH_SIZE,
  createRow,
  persistBatch,
  onProgress = null,
}) {
  if (!filePath) throw new Error("filePath is required");
  if (!Number.isInteger(batchSize) || batchSize <= 0) {
    throw new Error("batchSize must be a positive integer");
  }
  if (typeof createRow !== "function") {
    throw new Error("createRow is required");
  }
  if (typeof persistBatch !== "function") {
    throw new Error("persistBatch is required");
  }

  let headers = null;
  let rowsProcessed = 0;
  let rowsInserted = 0;
  let batch = [];

  const source = fs.createReadStream(filePath);
  const parser = csv.parse({
    headers: (rawHeaders) => {
      headers = repairCsvHeaders(rawHeaders);
      return headers;
    },
    ignoreEmpty: true,
    trim: true,
    strictColumnHandling: false,
    discardUnmappedColumns: true,
  });

  source.on("error", (error) => parser.destroy(error));
  source.pipe(parser);

  const flush = async () => {
    if (batch.length === 0) return;
    const pending = batch;
    batch = [];
    await persistBatch(pending);
    rowsInserted += pending.length;
    if (onProgress) {
      await onProgress({
        rowsProcessed,
        rowsInserted,
        bytesProcessed: source.bytesRead,
      });
    }
  };

  try {
    for await (const parsedRow of parser) {
      rowsProcessed += 1;
      batch.push(createRow(parsedRow, rowsProcessed));
      if (batch.length >= batchSize) {
        await flush();
      }
    }
    await flush();
  } finally {
    source.destroy();
    parser.destroy();
  }

  if (!headers) {
    const error = new Error("CSV appears to have no header row");
    error.statusCode = 400;
    throw error;
  }

  return {
    headers,
    rowsProcessed,
    rowsInserted,
    bytesProcessed: source.bytesRead,
  };
}

module.exports = {
  DEFAULT_BATCH_SIZE,
  ingestCsvFileInBatches,
  repairCsvHeaders,
};

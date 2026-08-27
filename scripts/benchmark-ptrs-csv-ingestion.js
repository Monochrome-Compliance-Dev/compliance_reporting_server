const fs = require("fs");
const os = require("os");
const path = require("path");
const { once } = require("events");
const { performance } = require("perf_hooks");

require("module-alias/register");

const {
  DEFAULT_BATCH_SIZE,
  ingestCsvFileInBatches,
} = require("@/v2/ptrs/services/csv-ingestion.ptrs.service");

const requestedRows = Number.parseInt(
  process.argv.find((arg) => arg.startsWith("--rows="))?.split("=")[1] ||
    "250000",
  10,
);

if (!Number.isInteger(requestedRows) || requestedRows <= 0) {
  throw new Error("--rows must be a positive integer");
}

function csvValue(value) {
  const text = String(value);
  return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

async function generateCsv(filePath, rows) {
  const output = fs.createWriteStream(filePath, { flags: "wx" });
  const headers = Array.from({ length: 32 }, (_, index) => `field_${index + 1}`);
  output.write(`${headers.join(",")}\n`);

  for (let rowNo = 1; rowNo <= rows; rowNo += 1) {
    const values = headers.map((_, columnNo) => {
      if (columnNo === 5) return `Supplier ${rowNo}, Australia`;
      if (columnNo === 11 && rowNo % 10000 === 0) return `quoted "value" ${rowNo}`;
      return `${columnNo + 1}-${rowNo}-${"x".repeat(8)}`;
    });
    if (!output.write(`${values.map(csvValue).join(",")}\n`)) {
      await once(output, "drain");
    }
  }

  output.end();
  await once(output, "finish");
}

async function main() {
  const temporaryDirectory = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "ptrs-csv-benchmark-"),
  );
  const filePath = path.join(temporaryDirectory, `${requestedRows}.csv`);

  try {
    await generateCsv(filePath, requestedRows);
    const fileSizeBytes = (await fs.promises.stat(filePath)).size;
    let insertedRows = 0;
    let peakRssBytes = 0;
    let peakHeapUsedBytes = 0;
    let peakHeapTotalBytes = 0;

    const sampleMemory = () => {
      const memory = process.memoryUsage();
      peakRssBytes = Math.max(peakRssBytes, memory.rss);
      peakHeapUsedBytes = Math.max(peakHeapUsedBytes, memory.heapUsed);
      peakHeapTotalBytes = Math.max(peakHeapTotalBytes, memory.heapTotal);
    };

    sampleMemory();
    const startedAt = performance.now();
    const result = await ingestCsvFileInBatches({
      filePath,
      batchSize: DEFAULT_BATCH_SIZE,
      createRow: (data, rowNo) => ({ rowNo, data }),
      persistBatch: async (batch) => {
        insertedRows += batch.length;
        sampleMemory();
      },
      onProgress: sampleMemory,
    });
    const elapsedMs = Math.round(performance.now() - startedAt);
    sampleMemory();

    process.stdout.write(
      `${JSON.stringify(
        {
          rowsRequested: requestedRows,
          rowsProcessed: result.rowsProcessed,
          insertedRows,
          columns: result.headers.length,
          batchSize: DEFAULT_BATCH_SIZE,
          fileSizeBytes,
          elapsedMs,
          peakRssBytes,
          peakHeapUsedBytes,
          peakHeapTotalBytes,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await fs.promises.rm(temporaryDirectory, { recursive: true, force: true });
  }
}

main().catch((error) => {
  process.stderr.write(`${error.stack || error.message}\n`);
  process.exitCode = 1;
});

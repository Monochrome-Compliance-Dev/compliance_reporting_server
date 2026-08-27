const fs = require("fs");
const os = require("os");
const path = require("path");

const {
  ingestCsvFileInBatches,
} = require("@/v2/ptrs/services/csv-ingestion.ptrs.service");

describe("bounded PTRS CSV ingestion", () => {
  let temporaryDirectory;

  beforeEach(async () => {
    temporaryDirectory = await fs.promises.mkdtemp(
      path.join(os.tmpdir(), "ptrs-csv-ingestion-"),
    );
  });

  afterEach(async () => {
    await fs.promises.rm(temporaryDirectory, { recursive: true, force: true });
  });

  async function writeCsv(contents) {
    const filePath = path.join(temporaryDirectory, "input.csv");
    await fs.promises.writeFile(filePath, contents, "utf8");
    return filePath;
  }

  test("repairs headers and parses quoted, multiline, and non-empty rows once", async () => {
    const filePath = await writeCsv(
      '\uFEFF\n\n Name,,Name,Comment\n"Alpha, Pty",x,first,"line one\nline two"\n,,,\nBeta,y,second,plain\nGamma,z,third,last\n',
    );
    const batches = [];
    const progress = [];

    const result = await ingestCsvFileInBatches({
      filePath,
      batchSize: 2,
      createRow: (data, rowNo) => ({ rowNo, data }),
      persistBatch: async (batch) => batches.push(batch),
      onProgress: async (value) => progress.push(value),
    });

    expect(result.headers).toEqual([
      "Name",
      "column_2",
      "Name_2",
      "Comment",
    ]);
    expect(result.rowsProcessed).toBe(3);
    expect(result.rowsInserted).toBe(3);
    expect(batches.map((batch) => batch.length)).toEqual([2, 1]);
    expect(batches.flat().map((row) => row.rowNo)).toEqual([1, 2, 3]);
    expect(batches[0][0].data).toEqual({
      Name: "Alpha, Pty",
      column_2: "x",
      Name_2: "first",
      Comment: "line one\nline two",
    });
    expect(progress.map((value) => value.rowsInserted)).toEqual([2, 3]);
  });

  test("rejects a malformed CSV without requiring a row pre-count", async () => {
    const filePath = await writeCsv('a,b\nvalid,row\n"missing,quote\n');
    const persisted = [];

    await expect(
      ingestCsvFileInBatches({
        filePath,
        batchSize: 1,
        createRow: (data, rowNo) => ({ rowNo, data }),
        persistBatch: async (batch) => persisted.push(...batch),
      }),
    ).rejects.toThrow("missing closing");

    expect(persisted).toHaveLength(1);
  });

  test("flushes the production batch boundary without changing row order", async () => {
    const records = Array.from(
      { length: 1001 },
      (_, index) => `${index + 1},value-${index + 1}`,
    );
    const filePath = await writeCsv(`id,value\n${records.join("\n")}\n`);
    const batchSizes = [];
    const finalRowNumbers = [];

    const result = await ingestCsvFileInBatches({
      filePath,
      createRow: (data, rowNo) => ({ rowNo, data }),
      persistBatch: async (batch) => {
        batchSizes.push(batch.length);
        finalRowNumbers.push(batch.at(-1).rowNo);
      },
    });

    expect(batchSizes).toEqual([1000, 1]);
    expect(finalRowNumbers).toEqual([1000, 1001]);
    expect(result.rowsInserted).toBe(1001);
  });

  test("rejects an empty file as having no header row", async () => {
    const filePath = await writeCsv("");

    await expect(
      ingestCsvFileInBatches({
        filePath,
        createRow: (data, rowNo) => ({ rowNo, data }),
        persistBatch: jest.fn(),
      }),
    ).rejects.toMatchObject({
      message: "CSV appears to have no header row",
      statusCode: 400,
    });
  });
});

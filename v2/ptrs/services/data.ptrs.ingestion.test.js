const fs = require("fs");
const os = require("os");
const path = require("path");

const mockTransactions = [];
const mockMakeTransaction = () => {
  const transaction = {
    finished: false,
    commit: jest.fn(async () => {
      transaction.finished = "commit";
    }),
    rollback: jest.fn(async () => {
      transaction.finished = "rollback";
    }),
  };
  mockTransactions.push(transaction);
  return transaction;
};

const mockDataset = {
  values: {
    role: "transaction",
    sourceType: "csv",
    meta: { originalName: "input.csv" },
  },
  get: jest.fn((field) => mockDataset.values[field]),
  update: jest.fn(async (values) => {
    Object.assign(mockDataset.values, values);
  }),
};

const mockDb = {
  PtrsImportRaw: {
    bulkCreate: jest.fn(async () => {}),
    destroy: jest.fn(async () => 0),
  },
  PtrsDataset: {
    findOne: jest.fn(async () => mockDataset),
  },
};

jest.mock("@/db/database", () => mockDb);
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(async () => mockMakeTransaction()),
}));
jest.mock("@/helpers/logger", () => ({
  logger: { info: jest.fn(), error: jest.fn(), warn: jest.fn() },
}));

const {
  importDatasetCsvStreamToImportRaw,
} = require("@/v2/ptrs/services/data.ptrs.service");

describe("PTRS dataset ingestion persistence", () => {
  let temporaryDirectory;
  let filePath;
  let emit;

  beforeEach(async () => {
    jest.clearAllMocks();
    mockTransactions.length = 0;
    mockDataset.values = {
      role: "transaction",
      sourceType: "csv",
      meta: { originalName: "input.csv" },
    };
    temporaryDirectory = await fs.promises.mkdtemp(
      path.join(os.tmpdir(), "ptrs-data-ingestion-"),
    );
    filePath = path.join(temporaryDirectory, "input.csv");
    emit = jest.fn();
    global.__socketio = { to: jest.fn(() => ({ emit })) };
  });

  afterEach(async () => {
    delete global.__socketio;
    await fs.promises.rm(temporaryDirectory, { recursive: true, force: true });
  });

  test("persists exact row numbers and finalises dataset metadata only at EOF", async () => {
    await fs.promises.writeFile(filePath, "a,b\n1,2\n3,4\n", "utf8");

    const result = await importDatasetCsvStreamToImportRaw({
      customerId: "customer1",
      ptrsId: "ptrs000001",
      datasetId: "dataset001",
      role: "transaction",
      sourceType: "csv",
      filePath,
      fileSize: 16,
    });

    expect(result.rowsInserted).toBe(2);
    expect(mockDb.PtrsImportRaw.bulkCreate).toHaveBeenCalledTimes(1);
    expect(mockDb.PtrsImportRaw.bulkCreate.mock.calls[0][0]).toEqual([
      expect.objectContaining({ rowNo: 1, data: { a: "1", b: "2" } }),
      expect.objectContaining({ rowNo: 2, data: { a: "3", b: "4" } }),
    ]);
    expect(mockDataset.update).toHaveBeenCalledWith(
      expect.objectContaining({
        status: "parsed",
        rowsCount: 2,
        meta: expect.objectContaining({ headers: ["a", "b"], rowsCount: 2 }),
      }),
      expect.any(Object),
    );
    const statusPayloads = emit.mock.calls.map((call) => call[1]);
    expect(statusPayloads[0]).toEqual(
      expect.objectContaining({ status: "uploading", totalRows: null }),
    );
    expect(statusPayloads.at(-1)).toEqual(
      expect.objectContaining({ status: "complete", totalRows: 2 }),
    );
  });

  test("cleans partial rows, marks the dataset failed, and removes source after a DB failure", async () => {
    await fs.promises.writeFile(filePath, "a,b\n1,2\n", "utf8");
    mockDb.PtrsImportRaw.bulkCreate.mockRejectedValueOnce(new Error("write failed"));

    await expect(
      importDatasetCsvStreamToImportRaw({
        customerId: "customer1",
        ptrsId: "ptrs000001",
        datasetId: "dataset001",
        role: "transaction",
        sourceType: "csv",
        filePath,
      }),
    ).rejects.toThrow("write failed");

    expect(mockDb.PtrsImportRaw.destroy).toHaveBeenCalledWith(
      expect.objectContaining({
        where: {
          customerId: "customer1",
          ptrsId: "ptrs000001",
          datasetId: "dataset001",
        },
      }),
    );
    expect(mockDataset.update).toHaveBeenCalledWith(
      expect.objectContaining({
        status: "failed",
        storageRef: null,
        rowsCount: null,
      }),
      expect.any(Object),
    );
    await expect(fs.promises.stat(filePath)).rejects.toMatchObject({ code: "ENOENT" });
    expect(emit.mock.calls.at(-1)[1]).toEqual(
      expect.objectContaining({ status: "failed", totalRows: null }),
    );
  });
});

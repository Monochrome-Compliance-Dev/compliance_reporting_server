const mockTransaction = {
  finished: false,
  commit: jest.fn(async function commit() {
    this.finished = "commit";
  }),
  rollback: jest.fn(async function rollback() {
    this.finished = "rollback";
  }),
};

const transactionDatasets = [
  {
    id: "dataset-a",
    ptrsId: "ptrs-1",
    purpose: "transaction",
    sourceFormat: "csv",
  },
  {
    id: "dataset-b",
    ptrsId: "ptrs-1",
    purpose: "transaction",
    sourceFormat: "csv",
  },
];

const mockDb = {
  PtrsDataset: {
    rawAttributes: { deletedAt: {} },
    findAll: jest.fn(async () => transactionDatasets),
    findOne: jest.fn(async ({ where }) =>
      transactionDatasets.find((dataset) => dataset.id === where.id),
    ),
  },
  PtrsImportRaw: {
    count: jest.fn(async () => 1),
    findAll: jest.fn(async () => [{ rowNo: 1, data: { amount: "10" } }]),
  },
};

jest.mock("@/db/database", () => mockDb);
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(async () => {
    mockTransaction.finished = false;
    return mockTransaction;
  }),
}));
jest.mock("@/helpers/logger", () => ({
  logger: { info: jest.fn(), error: jest.fn(), warn: jest.fn() },
}));

const {
  getDatasetSample,
  listDatasets,
} = require("./data.ptrs.service");

describe("PTRS dataset listing and raw-row scope", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockTransaction.finished = false;
  });

  test("returns every transaction dataset without collapsing a primary dataset", async () => {
    const result = await listDatasets({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(result.map((dataset) => dataset.id)).toEqual([
      "dataset-a",
      "dataset-b",
    ]);
    expect(mockDb.PtrsDataset.findAll).toHaveBeenCalledWith(
      expect.objectContaining({
        where: {
          customerId: "customer-1",
          ptrsId: "ptrs-1",
          deletedAt: null,
        },
      }),
    );
  });

  test("samples raw rows only from the concrete dataset", async () => {
    await getDatasetSample({
      customerId: "customer-1",
      datasetId: "dataset-b",
      limit: 10,
    });

    expect(mockDb.PtrsImportRaw.count).toHaveBeenCalledWith(
      expect.objectContaining({
        where: {
          customerId: "customer-1",
          ptrsId: "ptrs-1",
          datasetId: "dataset-b",
        },
      }),
    );
    expect(mockDb.PtrsImportRaw.findAll).toHaveBeenCalledWith(
      expect.objectContaining({
        where: {
          customerId: "customer-1",
          ptrsId: "ptrs-1",
          datasetId: "dataset-b",
        },
      }),
    );
  });
});

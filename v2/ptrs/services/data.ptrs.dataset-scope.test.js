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

const directDatasetValues = {
  id: "dataset-direct",
  customerId: "customer-1",
  ptrsId: "ptrs-1",
  purpose: "transaction",
  adapterType: "direct_payment",
  dateFormat: null,
};
const directDataset = {
  get: jest.fn((key) =>
    typeof key === "string"
      ? directDatasetValues[key]
      : { ...directDatasetValues },
  ),
  update: jest.fn(async (values) => {
    Object.assign(directDatasetValues, values);
    return directDataset;
  }),
};
const createdSnapshot = {
  id: "snapshot-1",
  datasetId: "dataset-direct",
  entityName: "ORONTIDE GROUP PTY LTD",
  abn: "40115288492",
  get: jest.fn(function get() {
    return {
      id: this.id,
      datasetId: this.datasetId,
      entityName: this.entityName,
      abn: this.abn,
    };
  }),
};

const mockDb = {
  PtrsDataset: {
    rawAttributes: { deletedAt: {} },
    findAll: jest.fn(async () => transactionDatasets),
    findOne: jest.fn(async ({ where }) =>
      where.id === "dataset-direct"
        ? directDataset
        : transactionDatasets.find((dataset) => dataset.id === where.id),
    ),
  },
  PtrsDatasetReportingEntitySnapshot: {
    findAll: jest.fn(async () => []),
    findOne: jest.fn(async () => null),
    create: jest.fn(async () => createdSnapshot),
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
  updateDatasetSettings,
} = require("./data.ptrs.service");

describe("PTRS dataset listing and raw-row scope", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockTransaction.finished = false;
    directDatasetValues.dateFormat = null;
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

  test("saves direct dataset identity and date format atomically", async () => {
    const result = await updateDatasetSettings({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      datasetId: "dataset-direct",
      dateFormat: "MDY",
      reportingEntityName: "ORONTIDE GROUP PTY LTD",
      reportingEntityAbn: "40 115 288 492",
      userId: "user-1",
    });

    expect(directDataset.update).toHaveBeenCalledWith(
      { dateFormat: "MDY", updatedBy: "user-1" },
      { transaction: mockTransaction },
    );
    expect(
      mockDb.PtrsDatasetReportingEntitySnapshot.create,
    ).toHaveBeenCalledWith(
      expect.objectContaining({
        datasetId: "dataset-direct",
        entityName: "ORONTIDE GROUP PTY LTD",
        abn: "40 115 288 492",
      }),
      { transaction: mockTransaction },
    );
    expect(result).toMatchObject({
      dateFormat: "MDY",
      reportingEntity: {
        id: "snapshot-1",
        entityName: "ORONTIDE GROUP PTY LTD",
      },
    });
    expect(mockTransaction.commit).toHaveBeenCalledTimes(1);
  });

  test("saves a supplied malformed ABN unchanged for downstream validation", async () => {
    await updateDatasetSettings({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      datasetId: "dataset-direct",
      dateFormat: "DMY",
      reportingEntityName: "Supplied Entity",
      reportingEntityAbn: "not-an-abn",
      userId: "user-1",
    });

    expect(
      mockDb.PtrsDatasetReportingEntitySnapshot.create,
    ).toHaveBeenCalledWith(expect.objectContaining({ abn: "not-an-abn" }), {
      transaction: mockTransaction,
    });
    expect(mockTransaction.commit).toHaveBeenCalledTimes(1);
  });

  test.each([
    ["missing supplied ABN", "MDY", ""],
    ["invalid date convention", "guess", "not-an-abn"],
  ])(
    "rejects direct dataset settings with %s",
    async (_label, dateFormat, abn) => {
      await expect(
        updateDatasetSettings({
          customerId: "customer-1",
          ptrsId: "ptrs-1",
          datasetId: "dataset-direct",
          dateFormat,
          reportingEntityName: "ORONTIDE GROUP PTY LTD",
          reportingEntityAbn: abn,
          userId: "user-1",
        }),
      ).rejects.toMatchObject({ statusCode: 400 });

      expect(directDataset.update).not.toHaveBeenCalled();
      expect(
        mockDb.PtrsDatasetReportingEntitySnapshot.create,
      ).not.toHaveBeenCalled();
    },
  );
});

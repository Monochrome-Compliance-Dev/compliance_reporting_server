const mockTransaction = {
  finished: false,
  commit: jest.fn(async function commit() {
    this.finished = "commit";
  }),
  rollback: jest.fn(async function rollback() {
    this.finished = "rollback";
  }),
};

const mockDb = {
  PtrsColumnMap: { findAll: jest.fn() },
  PtrsFieldMap: { findAll: jest.fn() },
  Ptrs: { findAll: jest.fn() },
  PtrsDataset: { findAll: jest.fn() },
};

jest.mock("@/db/database", () => mockDb);
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(async () => mockTransaction),
}));
jest.mock("@/v2/ptrs/services/ptrs.service", () => ({
  safeMeta: (value) => value,
  slog: { info: jest.fn(), debug: jest.fn(), error: jest.fn() },
  toSnake: (value) => String(value || "").trim().toLowerCase(),
}));
jest.mock("@/v2/ptrs/services/maps.staleness.ptrs.service", () => ({
  extractMapMetaFromExtras: jest.fn(() => null),
  buildMaterialMapSignature: jest.fn(),
  safeParseJsonObject: jest.fn(),
  buildMapMetaFromMappings: jest.fn(),
}));
jest.mock("@/v2/ptrs/contracts/ptrs.canonical.contract", () => ({
  PTRS_CANONICAL_CONTRACT: { fields: {} },
}));

const { listCompatibleJoins } = require("./joins.ptrs.service");
const { listCompatibleMaps } = require("./maps.config.ptrs.service");

describe("PTRS saved configuration ordering", () => {
  beforeEach(() => {
    mockTransaction.finished = false;
    mockTransaction.commit.mockClear();
    mockTransaction.rollback.mockClear();
    for (const model of Object.values(mockDb)) {
      model.findAll.mockReset();
    }
  });

  test("returns compatible joins newest updated first", async () => {
    mockDb.PtrsColumnMap.findAll.mockResolvedValueOnce([
      {
        ptrsId: "older-ptrs",
        joins: { conditions: [{ id: "one" }, { id: "two" }] },
        customFields: [],
        updatedAt: "2026-01-01T00:00:00.000Z",
        createdAt: "2026-01-01T00:00:00.000Z",
      },
      {
        ptrsId: "newer-ptrs",
        joins: { conditions: [{ id: "one" }] },
        customFields: [],
        updatedAt: "2026-02-01T00:00:00.000Z",
        createdAt: "2026-01-15T00:00:00.000Z",
      },
    ]);
    mockDb.PtrsDataset.findAll.mockResolvedValueOnce([
      {
        id: "older-dataset",
        ptrsId: "older-ptrs",
        purpose: "transaction",
        fileName: "older.csv",
        meta: {},
      },
      {
        id: "newer-dataset",
        ptrsId: "newer-ptrs",
        purpose: "transaction",
        fileName: "newer.csv",
        meta: {},
      },
    ]);

    const result = await listCompatibleJoins({
      customerId: "customer-1",
      ptrsId: "current-ptrs",
    });

    expect(result.items.map((item) => item.ptrsId)).toEqual([
      "newer-ptrs",
      "older-ptrs",
    ]);
  });

  test("returns compatible maps by field-map update date before map size", async () => {
    mockDb.PtrsColumnMap.findAll.mockResolvedValueOnce([]);
    mockDb.PtrsFieldMap.findAll.mockResolvedValueOnce([
      {
        ptrsId: "older-ptrs",
        datasetId: "older-dataset",
        canonicalField: "field-a",
        updatedAt: "2026-01-01T00:00:00.000Z",
        createdAt: "2026-01-01T00:00:00.000Z",
      },
      {
        ptrsId: "older-ptrs",
        datasetId: "older-dataset",
        canonicalField: "field-b",
        updatedAt: "2026-01-01T00:00:00.000Z",
        createdAt: "2026-01-01T00:00:00.000Z",
      },
      {
        ptrsId: "newer-ptrs",
        datasetId: "newer-dataset",
        canonicalField: "field-a",
        updatedAt: "2026-02-01T00:00:00.000Z",
        createdAt: "2026-01-15T00:00:00.000Z",
      },
    ]);
    mockDb.Ptrs.findAll.mockResolvedValueOnce([
      { id: "older-ptrs", createdAt: "2026-01-01T00:00:00.000Z" },
      { id: "newer-ptrs", createdAt: "2026-01-15T00:00:00.000Z" },
    ]);
    mockDb.PtrsDataset.findAll.mockResolvedValueOnce([
      {
        id: "older-dataset",
        ptrsId: "older-ptrs",
        purpose: "transaction",
        fileName: "older.csv",
      },
      {
        id: "newer-dataset",
        ptrsId: "newer-ptrs",
        purpose: "transaction",
        fileName: "newer.csv",
      },
    ]);

    const result = await listCompatibleMaps({ customerId: "customer-1" });

    expect(result.items.map((item) => item.datasetId)).toEqual([
      "newer-dataset",
      "older-dataset",
    ]);
    expect(result.items.map((item) => item.mappedFieldsCount)).toEqual([1, 2]);
  });
});

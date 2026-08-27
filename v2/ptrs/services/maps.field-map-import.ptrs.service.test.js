const transactions = [];

function mockMakeTransaction() {
  const transaction = {
    finished: false,
    commit: jest.fn(async function commit() {
      this.finished = "commit";
    }),
    rollback: jest.fn(async function rollback() {
      this.finished = "rollback";
    }),
  };
  transactions.push(transaction);
  return transaction;
}

const sourceDatasets = [
  { id: "old-transaction", role: "transaction", purpose: "transaction" },
  { id: "old-vendor", role: "vendormaster", purpose: "reference", referenceKind: "vendormaster" },
  { id: "old-terms", role: "termschanges", purpose: "reference", referenceKind: "termschanges" },
  { id: "old-entity", role: "entitystructure", purpose: "reference", referenceKind: "entitystructure" },
  { id: "old-invoices", role: "invoices", purpose: "reference", referenceKind: "invoices" },
];

let targetDatasets = [];
let targetRows = [];

function getOperatorValues(value) {
  return Object.getOwnPropertySymbols(value || {}).flatMap(
    (symbol) => value[symbol] || [],
  );
}

const sourceRows = [
  {
    canonicalField: "payment_amount",
    sourceRole: "transaction",
    sourceColumn: "Amount",
    transformType: "number",
    transformConfig: { decimal: "." },
    meta: { sourceDatasetId: "old-transaction", retained: true },
  },
  {
    canonicalField: "payee_entity_abn",
    sourceRole: "vendormaster",
    sourceColumn: "Tax number",
    meta: { sourceDatasetId: "old-vendor" },
  },
  {
    canonicalField: "invoice_payment_terms",
    sourceRole: "termschanges",
    sourceColumn: "Terms",
  },
  {
    canonicalField: "payer_entity_abn",
    sourceRole: "entitystructure",
    sourceColumn: "ABN",
  },
  {
    canonicalField: "invoice_receipt_date",
    sourceRole: "invoices",
    sourceColumn: "Invoice Date Created - Date",
    transformType: "date",
    transformConfig: { format: "DD/MM/YYYY" },
  },
];

const joins = {
  conditions: [
    { from: { datasetId: "old-transaction" }, to: { datasetId: "old-vendor" } },
    { from: { datasetId: "old-transaction" }, to: { datasetId: "old-terms" } },
    { from: { datasetId: "old-transaction" }, to: { datasetId: "old-entity" } },
    { from: { datasetId: "old-transaction" }, to: { datasetId: "old-invoices" } },
  ],
};

const mockDb = {
  PtrsColumnMap: {
    findOne: jest.fn(async () => ({ joins })),
  },
  PtrsDataset: {
    findAll: jest.fn(async ({ where }) =>
      where.ptrsId === "source-ptrs" ? sourceDatasets : targetDatasets,
    ),
  },
  PtrsFieldMap: {
    findAll: jest.fn(async ({ where }) => {
      if (where.ptrsId === "source-ptrs") return sourceRows;
      const canonicalFields = getOperatorValues(where.canonicalField);
      return canonicalFields.length
        ? targetRows.filter((row) =>
            canonicalFields.includes(String(row.canonicalField)),
          )
        : targetRows;
    }),
    destroy: jest.fn(async ({ where }) => {
      const canonicalFields = getOperatorValues(where.canonicalField);
      const initialCount = targetRows.length;
      targetRows = targetRows.filter(
        (row) => !canonicalFields.includes(String(row.canonicalField)),
      );
      return initialCount - targetRows.length;
    }),
    bulkCreate: jest.fn(async (rows) => {
      targetRows.push(...rows);
      return rows;
    }),
  },
};

jest.mock("@/db/database", () => mockDb);
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(async () => mockMakeTransaction()),
}));
jest.mock("@/v2/ptrs/services/ptrs.service", () => ({
  safeMeta: (value) => value,
  slog: { info: jest.fn(), debug: jest.fn(), error: jest.fn() },
  toSnake: (value) => String(value || "").trim().toLowerCase(),
}));
jest.mock("@/v2/ptrs/services/maps.staleness.ptrs.service", () => ({
  extractMapMetaFromExtras: jest.fn(),
  buildMaterialMapSignature: jest.fn(),
  safeParseJsonObject: (value) =>
    typeof value === "string" ? JSON.parse(value) : value,
  buildMapMetaFromMappings: jest.fn(),
  getMapStaleness: jest.fn(),
}));
jest.mock("@/v2/ptrs/contracts/ptrs.canonical.contract", () => ({
  PTRS_CANONICAL_CONTRACT: { fields: {} },
}));

const { importFieldMap } = require("./maps.config.ptrs.service");

const baseArgs = {
  customerId: "customer-1",
  sourcePtrsId: "source-ptrs",
  sourceDatasetId: "old-transaction",
  targetPtrsId: "target-ptrs",
  targetDatasetId: "new-transaction",
  profileId: "profile-1",
  userId: "user-1",
};

describe("PTRS compatible field-map import dataset resolution", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    transactions.length = 0;
    targetRows = [
      {
        customerId: "customer-1",
        ptrsId: "target-ptrs",
        profileId: "profile-1",
        datasetId: "new-vendor",
        canonicalField: "payee_entity_abn",
        sourceRole: "vendormaster",
        sourceColumn: "Old tax number",
      },
    ];
    targetDatasets = [
      { id: "new-transaction", role: "transaction", purpose: "transaction" },
      { id: "new-vendor", role: "vendormaster", purpose: "reference", referenceKind: "vendormaster" },
      { id: "new-terms", role: "termschanges", purpose: "reference", referenceKind: "termschanges" },
      { id: "new-entity", role: "entitystructure", purpose: "reference", referenceKind: "entitystructure" },
      { id: "new-invoices", role: "invoices", purpose: "reference", referenceKind: "invoices" },
    ];
  });

  test("persists resolved reference and transaction dataset IDs", async () => {
    const result = await importFieldMap(baseArgs);

    expect(Object.fromEntries(result.map((row) => [row.sourceRole, row.datasetId])))
      .toEqual({
        transaction: "new-transaction",
        vendormaster: "new-vendor",
        termschanges: "new-terms",
        entitystructure: "new-entity",
        invoices: "new-invoices",
      });
    expect(result.find((row) => row.sourceRole === "transaction")).toMatchObject({
      datasetId: "new-transaction",
      canonicalField: "payment_amount",
      sourceColumn: "Amount",
      transformType: "number",
      transformConfig: { decimal: "." },
      meta: { retained: true },
    });
    expect(result.find((row) => row.sourceRole === "invoices")).toMatchObject({
      datasetId: "new-invoices",
      canonicalField: "invoice_receipt_date",
      sourceColumn: "Invoice Date Created - Date",
      transformType: "date",
      transformConfig: { format: "DD/MM/YYYY" },
    });
    expect(result.find((row) => row.sourceRole === "vendormaster")).toMatchObject({
      datasetId: "new-vendor",
      canonicalField: "payee_entity_abn",
      meta: null,
    });
    expect(mockDb.PtrsFieldMap.destroy).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          customerId: "customer-1",
          ptrsId: "target-ptrs",
          profileId: "profile-1",
          canonicalField: expect.any(Object),
        }),
        force: true,
        transaction: transactions[0],
      }),
    );
    expect(transactions[0].commit).toHaveBeenCalledTimes(1);
  });

  test("replaces an existing active mapping for the same canonical field", async () => {
    await importFieldMap(baseArgs);

    const payeeAbnRows = targetRows.filter(
      (row) => row.canonicalField === "payee_entity_abn",
    );
    expect(payeeAbnRows).toHaveLength(1);
    expect(payeeAbnRows[0]).toMatchObject({
      datasetId: "new-vendor",
      sourceRole: "vendormaster",
      sourceColumn: "Tax number",
    });
  });

  test("repeated imports leave one active row per canonical field", async () => {
    await importFieldMap(baseArgs);
    await importFieldMap(baseArgs);

    const counts = targetRows.reduce((result, row) => {
      result[row.canonicalField] = (result[row.canonicalField] || 0) + 1;
      return result;
    }, {});
    expect(counts).toEqual({
      payment_amount: 1,
      payee_entity_abn: 1,
      invoice_payment_terms: 1,
      payer_entity_abn: 1,
      invoice_receipt_date: 1,
    });
    expect(mockDb.PtrsFieldMap.bulkCreate).toHaveBeenCalledTimes(2);
  });

  test("fails instead of falling back when a source role has no target dataset", async () => {
    targetDatasets = targetDatasets.filter((dataset) => dataset.role !== "invoices");

    await expect(importFieldMap(baseArgs)).rejects.toMatchObject({
      code: "FIELD_MAP_SOURCE_ROLE_UNRESOLVED",
      statusCode: 400,
    });
    expect(mockDb.PtrsFieldMap.bulkCreate).not.toHaveBeenCalled();
    expect(transactions[0].rollback).toHaveBeenCalledTimes(1);
  });

  test("fails when a source role is ambiguous in the current PTRS", async () => {
    targetDatasets.push({
      id: "new-invoices-2",
      role: "invoices",
      purpose: "reference",
      referenceKind: "invoices",
    });

    await expect(importFieldMap(baseArgs)).rejects.toMatchObject({
      code: "FIELD_MAP_SOURCE_ROLE_UNRESOLVED",
      statusCode: 400,
    });
    expect(mockDb.PtrsFieldMap.bulkCreate).not.toHaveBeenCalled();
    expect(transactions[0].rollback).toHaveBeenCalledTimes(1);
  });
});

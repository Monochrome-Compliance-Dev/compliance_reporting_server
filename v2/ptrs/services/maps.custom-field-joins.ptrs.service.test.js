let mockConfiguredJoins = [];
let mockConfiguredCustomFields = [];
let mockTransactionRows = [];
let mockReferenceRows = [];

const mockDb = {
  PtrsDataset: {
    findAll: jest.fn(async () => [
      {
        id: "transaction-1",
        role: "transaction",
        purpose: "transaction",
        referenceKind: null,
      },
      {
        id: "reference-1",
        role: "invoices",
        purpose: "reference",
        referenceKind: "invoices",
      },
    ]),
  },
  PtrsImportRaw: {
    findAll: jest.fn(async ({ where }) =>
      where.datasetId === "reference-1" ? mockReferenceRows : [],
    ),
  },
};

jest.mock("@/db/database", () => mockDb);
jest.mock("@/helpers/logger", () => ({
  logger: { info: jest.fn(), debug: jest.fn(), error: jest.fn() },
}));
jest.mock("@/v2/ptrs/services/ptrs.service", () => ({
  safeMeta: (value) => value,
  slog: { info: jest.fn(), debug: jest.fn(), error: jest.fn() },
  toSnake: (value) =>
    String(value || "")
      .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
      .replace(/[^a-zA-Z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "")
      .toLowerCase(),
  normalizeJoinKeyValue: (value, transform) => {
    if (value == null) return "";
    const text = String(value).trim();
    return transform?.op === "trim_upper" ? text.toUpperCase() : text;
  },
}));
jest.mock("@/v2/ptrs/services/data.ptrs.service", () => ({
  pickFromRowLoose: (row, key) => {
    if (!row || key == null) return undefined;
    if (Object.prototype.hasOwnProperty.call(row, key)) return row[key];
    const target = String(key).trim().toLowerCase();
    const matchedKey = Object.keys(row).find(
      (candidate) => String(candidate).trim().toLowerCase() === target,
    );
    return matchedKey == null ? undefined : row[matchedKey];
  },
}));
jest.mock("@/v2/ptrs/services/maps.joins.ptrs.service", () => ({
  logComposeJoinProbeOnce: jest.fn(),
}));
jest.mock("@/v2/ptrs/services/maps.dependencies.ptrs.service", () => ({
  loadComposeDependencies: jest.fn(async () => ({
    supportConfig: {},
    fieldMapRows: [
      {
        datasetId: "transaction-1",
        canonicalField: "invoice_issue_date",
        sourceRole: "transaction",
        sourceColumn: "Document Date",
        transformType: null,
        transformConfig: null,
      },
      {
        datasetId: "transaction-1",
        canonicalField: "payment_date",
        sourceRole: "transaction",
        sourceColumn: "Payment Date",
        transformType: null,
        transformConfig: null,
      },
      {
        datasetId: "transaction-1",
        canonicalField: "invoice_due_date",
        sourceRole: "transaction",
        sourceColumn: "Due Date",
        transformType: null,
        transformConfig: null,
      },
      {
        datasetId: "reference-1",
        canonicalField: "invoice_receipt_date",
        sourceRole: "invoices",
        sourceColumn: "Receipt Date",
        transformType: null,
        transformConfig: null,
        meta: { sourceDatasetId: "reference-1" },
      },
    ],
  })),
  normaliseConfiguredJoins: jest.fn(() => ({
    normalisedJoins: mockConfiguredJoins,
  })),
  normaliseConfiguredCustomFields: jest.fn(() => mockConfiguredCustomFields),
  resolveTransactionDatasetForCompose: jest.fn(async () => ({
    id: "transaction-1",
    purpose: "transaction",
    adapterType: "sap_accounting_event",
    sourceFormat: "csv",
    status: "parsed",
  })),
  loadTransactionRowsForCompose: jest.fn(async () => mockTransactionRows),
  buildHeadersFromComposedRows: jest.fn((rows) => Object.keys(rows?.[0] || {})),
}));

const {
  applyCustomFields,
  composeMappedRowsForPtrs,
  prepareMappedRowsContext,
} = require("./maps.compose.ptrs.service");

const transform = { op: "trim_upper" };

function customField({ datasetId, key, fields }) {
  return {
    datasetId,
    role: datasetId === "transaction-1" ? "transaction" : "invoices",
    key,
    type: "concat",
    segments: fields.flatMap((name, index) => [
      ...(index ? [{ kind: "literal", value: "|" }] : []),
      { kind: "field", name },
    ]),
  };
}

function join(fromColumn, toColumn) {
  return {
    fromRole: "transaction",
    fromDatasetId: "transaction-1",
    fromColumn,
    fromTransform: transform,
    toRole: "invoices",
    toDatasetId: "reference-1",
    toColumn,
    toTransform: transform,
  };
}

async function compose() {
  return composeMappedRowsForPtrs({
    customerId: "customer-1",
    ptrsId: "ptrs-1",
    datasetId: "transaction-1",
    limit: 50,
    transaction: { id: "transaction" },
    hrMsSince: () => 0,
    parseDateFlexible: () => null,
  });
}

describe("PTRS custom-field join composition", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockConfiguredJoins = [];
    mockConfiguredCustomFields = [];
    mockTransactionRows = [
      {
        id: "raw-transaction-1",
        rowNo: 1,
        data: {
          "Company Code": "R362",
          Account: "1010004055",
          Reference: "1858893",
          physical_transaction_key: "R362|1010004055|1858893",
          "Document Date": "2026-01-16",
          "Payment Date": "2026-02-15",
          "Due Date": "2026-02-14",
        },
      },
    ];
    mockReferenceRows = [
      {
        id: "raw-reference-1",
        rowNo: 1,
        data: {
          "Company Code - Company Code": "R362",
          "Supplier - ERP Supplier ID": "1010004055",
          "Invoice Number": "1858893",
          physical_reference_key: "R362|1010004055|1858893",
          "Receipt Date": "2026-01-15",
        },
      },
    ];
  });

  test("keeps physical-column to physical-column joins unchanged", async () => {
    mockConfiguredJoins = [
      join("physical_transaction_key", "physical_reference_key"),
    ];

    const result = await compose();

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].invoice_receipt_date).toBe("2026-01-15");
  });

  test("one prepared context loads references/indexes once across batches and retains the first duplicate and provenance", async () => {
    mockConfiguredCustomFields = [
      customField({
        datasetId: "reference-1",
        key: "invoice_key",
        fields: [
          "Company Code - Company Code",
          "Supplier - ERP Supplier ID",
          "Invoice Number",
        ],
      }),
    ];
    mockConfiguredJoins = [join("physical_transaction_key", "invoice_key")];
    mockReferenceRows.push({
      ...mockReferenceRows[0],
      id: "raw-reference-2",
      rowNo: 2,
      data: { ...mockReferenceRows[0].data, "Receipt Date": "2026-01-20" },
    });
    const trace = { write: jest.fn() };
    const options = {
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      datasetId: "transaction-1",
      transaction: { id: "build" },
      hrMsSince: () => 1,
      parseDateFlexible: () => null,
      trace,
    };
    const preparedContext = await prepareMappedRowsContext(options);
    const first = await composeMappedRowsForPtrs({
      ...options,
      preparedContext,
      limit: 2000,
    });
    mockTransactionRows = [
      { ...mockTransactionRows[0], id: "raw-transaction-2", rowNo: 2001 },
    ];
    const second = await composeMappedRowsForPtrs({
      ...options,
      preparedContext,
      limit: 2000,
      afterRowNo: 2000,
    });
    const dependencies = require("./maps.dependencies.ptrs.service");
    expect(dependencies.loadComposeDependencies).toHaveBeenCalledTimes(1);
    expect(dependencies.normaliseConfiguredJoins).toHaveBeenCalledTimes(1);
    expect(mockDb.PtrsDataset.findAll).toHaveBeenCalledTimes(1);
    expect(mockDb.PtrsImportRaw.findAll).toHaveBeenCalledTimes(1);
    expect(mockDb.PtrsImportRaw.findAll).toHaveBeenCalledWith(
      expect.objectContaining({
        where: {
          customerId: "customer-1",
          ptrsId: "ptrs-1",
          datasetId: "reference-1",
        },
        order: [["rowNo", "ASC"]],
        transaction: options.transaction,
      }),
    );
    expect(dependencies.loadTransactionRowsForCompose).toHaveBeenLastCalledWith(
      expect.objectContaining({ limit: 2000, afterRowNo: 2000 }),
    );
    for (const result of [first, second]) {
      expect(result.rows[0]).toMatchObject({
        invoice_issue_date: "2026-01-15",
        invoice_receipt_date: "2026-01-15",
        _ptrsMeta: {
          joinedReferences: {
            "reference-1": {
              sourceRawRowId: "raw-reference-1",
              sourceRowNo: 1,
            },
          },
        },
      });
    }
    expect(second.rows[0]._ptrsMeta.sourceRowNo).toBe(2001);
    expect(
      trace.write.mock.calls.filter(
        ([event, detail]) =>
          event === "compose_stage_end" && detail.stage === "build_join_index",
      ),
    ).toHaveLength(1);
    await expect(
      composeMappedRowsForPtrs({
        ...options,
        preparedContext,
        customerId: "another-customer",
      }),
    ).rejects.toThrow("scope/transaction mismatch");
    await expect(
      composeMappedRowsForPtrs({
        ...options,
        preparedContext,
        transaction: {},
      }),
    ).rejects.toThrow("scope/transaction mismatch");
  });

  test("canonical prepared inputs bypass configuration and dataset reloads", async () => {
    const dependencies = require("./maps.dependencies.ptrs.service");
    const { fieldMapRows } = await dependencies.loadComposeDependencies();
    const datasets = await mockDb.PtrsDataset.findAll();
    jest.clearAllMocks();
    const options = {
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      datasetId: "transaction-1",
      transaction: {},
      hrMsSince: () => 1,
      parseDateFlexible: () => null,
      preparedInput: {
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        profileId: "requested-profile",
        transactionDataset: {
          id: "transaction-1",
          adapterType: "sap_accounting_event",
        },
        supportConfig: { profileId: "requested-profile" },
        fieldMapRows,
        datasets,
      },
    };
    const preparedContext = await prepareMappedRowsContext(options);
    await composeMappedRowsForPtrs({ ...options, preparedContext });
    expect(dependencies.loadComposeDependencies).not.toHaveBeenCalled();
    expect(
      dependencies.resolveTransactionDatasetForCompose,
    ).not.toHaveBeenCalled();
    expect(mockDb.PtrsDataset.findAll).not.toHaveBeenCalled();
  });

  test("normalises the one-day SAP and invoice-source discrepancy canonically", async () => {
    mockConfiguredJoins = [
      join("physical_transaction_key", "physical_reference_key"),
    ];

    const result = await compose();

    expect(result.rows[0]).toMatchObject({
      invoice_issue_date: "2026-01-15",
      invoice_receipt_date: "2026-01-15",
      payment_date: "2026-02-15",
      invoice_due_date: "2026-02-14",
    });
  });

  test("joins a transaction custom field to a reference physical column", async () => {
    mockConfiguredCustomFields = [
      customField({
        datasetId: "transaction-1",
        key: "s4_invoice_key",
        fields: ["Company Code", "Account", "Reference"],
      }),
    ];
    mockConfiguredJoins = [join("s4_invoice_key", "physical_reference_key")];

    const result = await compose();

    expect(result.rows[0].s4_invoice_key).toBe("R362|1010004055|1858893");
    expect(result.rows[0].invoice_receipt_date).toBe("2026-01-15");
  });

  test("joins a transaction physical column to a reference custom field", async () => {
    mockConfiguredCustomFields = [
      customField({
        datasetId: "reference-1",
        key: "ariba_invoice_key",
        fields: [
          "Company Code - Company Code",
          "Supplier - ERP Supplier ID",
          "Invoice Number",
        ],
      }),
    ];
    mockConfiguredJoins = [
      join("physical_transaction_key", "ariba_invoice_key"),
    ];

    const result = await compose();

    expect(result.rows[0]["reference-1__ariba_invoice_key"]).toBe(
      "R362|1010004055|1858893",
    );
    expect(result.rows[0].invoice_receipt_date).toBe("2026-01-15");
  });

  test("joins custom fields on both sides using each field's dataset scope", async () => {
    mockConfiguredCustomFields = [
      customField({
        datasetId: "transaction-1",
        key: "invoice_key",
        fields: ["Company Code", "Account", "Reference"],
      }),
      customField({
        datasetId: "reference-1",
        key: "invoice_key",
        fields: [
          "Company Code - Company Code",
          "Supplier - ERP Supplier ID",
          "Invoice Number",
        ],
      }),
    ];
    mockConfiguredJoins = [join("invoice_key", "invoice_key")];

    const result = await compose();

    expect(result.rows[0].invoice_key).toBe("R362|1010004055|1858893");
    expect(result.rows[0]["reference-1__invoice_key"]).toBe(
      "R362|1010004055|1858893",
    );
    expect(result.rows[0].invoice_receipt_date).toBe("2026-01-15");
  });

  test("does not evaluate a custom field against a row owned by another dataset", () => {
    const fields = [
      customField({
        datasetId: "transaction-1",
        key: "scoped_key",
        fields: ["Company Code", "Account"],
      }),
      customField({
        datasetId: "reference-1",
        key: "scoped_key",
        fields: ["Company Code - Company Code", "Invoice Number"],
      }),
    ];

    const result = applyCustomFields({
      row: mockReferenceRows[0].data,
      rawRow: mockReferenceRows[0].data,
      customFields: fields,
      rowDatasetId: "reference-1",
    });

    expect(result.scoped_key).toBe("R362|1858893");
  });
});

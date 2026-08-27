const transactions = [];
const makeTransaction = () => {
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
};

const fieldMapRows = [
  { canonicalField: "payment_amount", datasetId: "dataset-a" },
  { canonicalField: "payee_entity_abn", datasetId: "vendor-a" },
];

const mockDb = {
  PtrsColumnMap: {
    findOne: jest.fn(async () => null),
    create: jest.fn(async (values) => ({
      get: () => values,
    })),
  },
  PtrsFieldMap: {
    destroy: jest.fn(async () => 0),
    bulkCreate: jest.fn(async (rows) => rows),
    findAll: jest.fn(async ({ where }) =>
      where.datasetId
        ? fieldMapRows.filter((row) => row.datasetId === where.datasetId)
        : fieldMapRows,
    ),
  },
  PtrsDataset: {
    findOne: jest.fn(async ({ where }) => ({
      id: where.id,
      purpose: "transaction",
      sourceFormat: "csv",
      adapterType: null,
      status: "parsed",
    })),
    findAll: jest.fn(async () => []),
  },
  PtrsImportRaw: {
    findAll: jest.fn(async () => [{ rowNo: 1, data: { amount: "10" } }]),
  },
  sequelize: {
    query: jest.fn(async (sql) => {
      if (sql.includes('WITH fields("canonicalField")')) {
        return [
          {
            canonicalField: "payment_amount",
            rowCount: 1,
            populatedCount: 1,
            missingCount: 0,
            completenessPct: 100,
            sampleMissingRowNos: [],
          },
          {
            canonicalField: "payment_date",
            rowCount: 1,
            populatedCount: 1,
            missingCount: 0,
            completenessPct: 100,
            sampleMissingRowNos: [],
          },
        ];
      }
      return [{ rowCount: 1 }];
    }),
  },
};

jest.mock("@/db/database", () => mockDb);
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(async () => makeTransaction()),
}));
jest.mock("@/helpers/logger", () => ({
  logger: { info: jest.fn(), debug: jest.fn(), error: jest.fn() },
}));
jest.mock("@/v2/ptrs/contracts/ptrs.canonical.contract", () => ({
  PTRS_CANONICAL_CONTRACT: {
    fields: {
      payment_amount: { required: true },
      payment_date: { required: true },
    },
  },
}));
jest.mock("@/v2/ptrs/services/ptrs.service", () => ({
  safeMeta: (value) => value,
  slog: { info: jest.fn(), debug: jest.fn(), error: jest.fn() },
  toSnake: (value) =>
    String(value || "")
      .trim()
      .toLowerCase(),
  normalizeJoinKeyValue: (value) => String(value || "").trim().toLowerCase(),
  createExecutionRun: jest.fn(),
  updateExecutionRun: jest.fn(),
}));
jest.mock("@/v2/ptrs/services/maps.staleness.ptrs.service", () => ({
  extractMapMetaFromExtras: jest.fn(),
  buildMaterialMapSignature: jest.fn(),
  safeParseJsonObject: jest.fn(),
  buildMapMetaFromMappings: jest.fn(),
  getMapStaleness: jest.fn(),
}));

const {
  getFieldMap,
  saveFieldMap,
} = require("./maps.config.ptrs.service");
const {
  loadTransactionRowsForCompose,
  normaliseConfiguredJoins,
  resolveTransactionDatasetForCompose,
} = require("./maps.dependencies.ptrs.service");
const {
  composeSingleMappedRow,
  orderJoinsForTransactionDataset,
} = require("./maps.compose.ptrs.service");
const { saveJoins } = require("./joins.ptrs.service");

const scope = {
  customerId: "customer-1",
  ptrsId: "ptrs-1",
  profileId: "profile-1",
};

describe("PTRS dataset-scoped maps and composition", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    transactions.length = 0;
  });

  test("saves the same canonical field independently for two transaction datasets", async () => {
    const fieldMap = [
      {
        canonicalField: "payment_amount",
        sourceRole: "transaction",
        sourceColumn: "Amount",
      },
    ];

    await saveFieldMap({ ...scope, datasetId: "dataset-a", fieldMap });
    await saveFieldMap({ ...scope, datasetId: "dataset-b", fieldMap });

    expect(mockDb.PtrsFieldMap.destroy.mock.calls[0][0].where.datasetId).toBe(
      "dataset-a",
    );
    expect(mockDb.PtrsFieldMap.destroy.mock.calls[1][0].where.datasetId).toBe(
      "dataset-b",
    );
    expect(mockDb.PtrsFieldMap.bulkCreate.mock.calls[0][0][0]).toMatchObject({
      datasetId: "dataset-a",
      canonicalField: "payment_amount",
    });
    expect(mockDb.PtrsFieldMap.bulkCreate.mock.calls[1][0][0]).toMatchObject({
      datasetId: "dataset-b",
      canonicalField: "payment_amount",
    });
  });

  test("mapping reads return the complete profile map across datasets", async () => {
    const rows = await getFieldMap(scope);

    expect(rows).toEqual(fieldMapRows);
    expect(rows.map((row) => row.datasetId)).toEqual([
      "dataset-a",
      "vendor-a",
    ]);
    expect(mockDb.PtrsFieldMap.findAll).toHaveBeenLastCalledWith(
      expect.objectContaining({
        where: scope,
      }),
    );
  });

  test("requires explicit join dataset IDs and preserves duplicate reference kinds", () => {
    const conditions = [
      {
        from: { datasetId: "dataset-a", role: "transaction", column: "Code" },
        to: { datasetId: "vendor-1", role: "vendormaster", column: "Code" },
      },
      {
        from: { datasetId: "dataset-b", role: "transaction", column: "Code" },
        to: { datasetId: "vendor-2", role: "vendormaster", column: "Code" },
      },
    ];

    const result = normaliseConfiguredJoins({
      supportConfig: { joins: { conditions } },
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(result.normalisedJoins.map((join) => join.toDatasetId)).toEqual([
      "vendor-1",
      "vendor-2",
    ]);
    expect(() =>
      normaliseConfiguredJoins({
        supportConfig: {
          joins: {
            conditions: [
              {
                from: { role: "transaction", column: "Code" },
                to: {
                  datasetId: "vendor-1",
                  role: "vendormaster",
                  column: "Code",
                },
              },
            ],
          },
        },
        customerId: "customer-1",
        ptrsId: "ptrs-1",
      }),
    ).toThrow("Every join endpoint requires datasetId");
  });

  test("saveJoins validates concrete endpoint IDs without selecting the first reference kind", async () => {
    const conditions = [
      {
        from: { datasetId: "dataset-a", role: "transaction", column: "Code" },
        to: { datasetId: "vendor-1", role: "vendormaster", column: "Code" },
      },
      {
        from: { datasetId: "dataset-b", role: "transaction", column: "Code" },
        to: { datasetId: "vendor-2", role: "vendormaster", column: "Code" },
      },
    ];
    mockDb.PtrsDataset.findAll.mockResolvedValueOnce(
      ["dataset-a", "dataset-b", "vendor-1", "vendor-2"].map((id) => ({ id })),
    );

    await saveJoins({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      joins: { conditions },
      customFields: [],
    });

    const idPredicate = mockDb.PtrsDataset.findAll.mock.calls[0][0].where.id;
    const requestedIds = Reflect.ownKeys(idPredicate)
      .map((key) => idPredicate[key])
      .flat();
    expect(requestedIds).toEqual(
      expect.arrayContaining(["dataset-a", "dataset-b", "vendor-1", "vendor-2"]),
    );
    expect(mockDb.PtrsColumnMap.create).toHaveBeenCalledWith(
      expect.objectContaining({
        joins: { conditions },
        rowRules: [],
      }),
      expect.any(Object),
    );
  });

  test("executes only the join graph reachable from the selected transaction dataset", () => {
    const joins = [
      { fromDatasetId: "dataset-b", toDatasetId: "vendor-2" },
      { fromDatasetId: "vendor-1", toDatasetId: "terms-1" },
      { fromDatasetId: "dataset-a", toDatasetId: "vendor-1" },
    ];

    expect(
      orderJoinsForTransactionDataset(joins, "dataset-a").map(
        (join) => `${join.fromDatasetId}:${join.toDatasetId}`,
      ),
    ).toEqual(["dataset-a:vendor-1", "vendor-1:terms-1"]);
  });

  test("composes against the explicitly joined reference dataset when kinds repeat", async () => {
    const counters = {
      joinAttempts: 0,
      joinSkippedMissingFromRole: 0,
      joinNoKey: 0,
      joinIndexLookups: 0,
      joinMatched: 0,
      joinNoMatch: 0,
      customFieldsApplied: 0,
      canonicalProjectionApplied: 0,
      canonicalSourceMetaApplied: 0,
    };
    const result = await composeSingleMappedRow({
      rawRow: { rowNo: 1, data: { VendorCode: "V1" } },
      orderedJoins: [
        {
          fromRole: "transaction",
          fromDatasetId: "dataset-a",
          fromColumn: "VendorCode",
          toRole: "vendormaster",
          toDatasetId: "vendor-2",
          toColumn: "Code",
        },
      ],
      customFields: [],
      fieldMapRows: [
        {
          datasetId: "vendor-2",
          canonicalField: "payee_entity_name",
          sourceRole: "vendormaster",
          sourceColumn: "Name",
          meta: { sourceDatasetId: "vendor-2" },
        },
      ],
      preparedJoinIndexes: new Map([
        ["vendor-2|Code", new Map([["v1", { Code: "V1", Name: "Vendor Two" }]])],
      ]),
      counters,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      logger: null,
      loggedJoinProbeRef: { current: false },
      transactionDatasetId: "dataset-a",
      getJoinLhsValue: (row, datasetId, column) =>
        datasetId === "dataset-a"
          ? row[column]
          : row[`${datasetId}__${column}`],
      mergeRoleRowNamespaced: (row, datasetId, joined) => ({
        ...row,
        ...Object.fromEntries(
          Object.entries(joined).map(([key, value]) => [
            `${datasetId}__${key}`,
            value,
          ]),
        ),
      }),
      joinIndexKey: (datasetId, column) => `${datasetId}|${column}`,
      resolveCanonicalValue: ({ sourceDatasetId, sourceColumn, srcRow }) =>
        srcRow[`${sourceDatasetId}__${sourceColumn}`],
      applyTransform: ({ value }) => value,
      setCanonicalSourceMeta: ({ outRow }) => outRow,
      normalizeJoinKeyValue: (value) => String(value || "").toLowerCase(),
      logComposeJoinProbeOnce: jest.fn(),
    });

    expect(result.payee_entity_name).toBe("Vendor Two");
    expect(result["vendor-2__Name"]).toBe("Vendor Two");
    expect(result["vendor-1__Name"]).toBeUndefined();
  });

  test("resolves and loads one concrete transaction dataset", async () => {
    const stageStart = jest.fn(() => ({}));
    const stageEnd = jest.fn();

    await resolveTransactionDatasetForCompose({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      datasetId: "dataset-b",
      stageStart,
      stageEnd,
    });
    expect(mockDb.PtrsDataset.findOne).toHaveBeenCalledWith(
      expect.objectContaining({
        where: {
          id: "dataset-b",
          customerId: "customer-1",
          ptrsId: "ptrs-1",
          purpose: "transaction",
        },
      }),
    );

    await loadTransactionRowsForCompose({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      datasetId: "dataset-b",
      stageStart,
      stageEnd,
    });
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

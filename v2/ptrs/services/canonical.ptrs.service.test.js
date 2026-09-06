const revisions = [];
const canonicalRows = [];
const transactions = [];

const datasets = {
  "dataset-a": {
    id: "dataset-a",
    customerId: "customer-1",
    ptrsId: "ptrs-1",
    purpose: "transaction",
    sourceFormat: "csv",
    adapterType: "sap_accounting_event",
    adapterVersion: "1",
    status: "parsed",
    rowsCount: 1,
    fileName: "A.csv",
    createdAt: "2026-01-01",
    updatedAt: "2026-01-01",
  },
  "dataset-b": {
    id: "dataset-b",
    customerId: "customer-1",
    ptrsId: "ptrs-1",
    purpose: "transaction",
    sourceFormat: "csv",
    adapterType: "sap_accounting_event",
    adapterVersion: "1",
    status: "parsed",
    rowsCount: 1,
    fileName: "B.csv",
    createdAt: "2026-01-02",
    updatedAt: "2026-01-02",
  },
  "dataset-direct": {
    id: "dataset-direct",
    customerId: "customer-1",
    ptrsId: "ptrs-1",
    purpose: "transaction",
    sourceFormat: "csv",
    adapterType: "direct_payment",
    adapterVersion: "1",
    status: "parsed",
    rowsCount: 1,
    fileName: "Direct.csv",
    createdAt: "2026-01-04",
    updatedAt: "2026-01-04",
  },
  "reference-a": {
    id: "reference-a",
    customerId: "customer-1",
    ptrsId: "ptrs-1",
    purpose: "reference",
    referenceKind: "vendormaster",
    sourceFormat: "csv",
    status: "parsed",
    rowsCount: 1,
    fileName: "vendors.csv",
    createdAt: "2026-01-03",
    updatedAt: "2026-01-03",
  },
};

const commonMappings = () =>
  [
    "payer_entity_name",
    "payer_entity_abn",
    "payee_entity_name",
    "payee_entity_abn",
    "invoice_reference_number",
    "payment_amount",
    "payment_date",
    "invoice_issue_date",
  ].map((canonicalField) => ({
    canonicalField,
    sourceRole: "transaction",
    sourceColumn: canonicalField,
  }));

const accountingMappings = () =>
  [
    ...commonMappings(),
    "document_type",
    "company_code",
    "source_account_code",
    "clearing_document",
  ].map((mapping) =>
    typeof mapping === "string"
      ? {
          canonicalField: mapping,
          sourceRole: "transaction",
          sourceColumn: mapping,
        }
      : mapping,
  );

let mappings = {
  "dataset-a": accountingMappings(),
  "dataset-b": accountingMappings(),
  "dataset-direct": commonMappings(),
};
let joins = {
  conditions: [
    {
      from: { datasetId: "dataset-a", role: "transaction", column: "Vendor" },
      to: { datasetId: "reference-a", role: "vendormaster", column: "Vendor" },
    },
  ],
};
let referenceVersion = "2026-01-03";
let revisionSequence = 0;

function plainRevision(values) {
  const revision = {
    id: `revision-${++revisionSequence}`,
    createdAt: `2026-02-${String(revisionSequence).padStart(2, "0")}`,
    ...values,
    previous(field) {
      return this[field];
    },
    async update(updates) {
      Object.assign(this, updates);
      return this;
    },
    get() {
      return {
        ...this,
        update: undefined,
        get: undefined,
        previous: undefined,
      };
    },
  };
  revisions.push(revision);
  return revision;
}

const mockDb = {
  sequelize: { query: jest.fn(async () => [[{ pid: 1234 }]]) },
  PtrsDataset: {
    findOne: jest.fn(async ({ where }) => datasets[where.id] || null),
    findAll: jest.fn(async ({ where }) => {
      if (where.purpose === "transaction") {
        return [datasets["dataset-a"], datasets["dataset-b"]];
      }
      const ids = Object.getOwnPropertySymbols(where.id).length
        ? where.id[Object.getOwnPropertySymbols(where.id)[0]]
        : [];
      return ids.map((id) => ({
        ...datasets[id],
        updatedAt:
          id === "reference-a" ? referenceVersion : datasets[id].updatedAt,
      }));
    }),
  },
  PtrsFieldMap: {
    findAll: jest.fn(async ({ where }) => {
      const datasetFilter = where.datasetId;
      const datasetIds =
        typeof datasetFilter === "string"
          ? [datasetFilter]
          : Object.getOwnPropertySymbols(datasetFilter || {}).flatMap(
              (symbol) => datasetFilter[symbol] || [],
            );
      return datasetIds.flatMap((datasetId) =>
        (mappings[datasetId] || []).map((mapping) => ({
          ...mapping,
          datasetId,
        })),
      );
    }),
  },
  PtrsColumnMap: {
    findOne: jest.fn(async () => ({ joins, customFields: [] })),
  },
  PtrsImportRaw: {
    count: jest.fn(
      async ({ where }) => datasets[where.datasetId]?.rowsCount || 0,
    ),
    max: jest.fn(async (_field, { where }) =>
      where.datasetId === "reference-a"
        ? referenceVersion
        : datasets[where.datasetId]?.updatedAt,
    ),
  },
  PtrsCanonicalRevision: {
    create: jest.fn(async (values) => {
      if (
        revisions.some(
          (revision) =>
            ["building", "succeeded"].includes(revision.status) &&
            ["customerId", "ptrsId", "datasetId", "materialSignature"].every(
              (key) => revision[key] === values[key],
            ),
        )
      ) {
        const error = new Error("active material already claimed");
        error.original = {
          constraint: "ptrs_canonical_revision_active_material_ux",
        };
        throw error;
      }
      return plainRevision(values);
    }),
    findOne: jest.fn(async ({ where }) => {
      const matches = revisions.filter((revision) =>
        Object.entries(where).every(([key, value]) => {
          if (value && typeof value === "object") {
            const choices = value[Object.getOwnPropertySymbols(value)[0]];
            return choices.includes(revision[key]);
          }
          return revision[key] === value;
        }),
      );
      return matches.at(-1) || null;
    }),
  },
  PtrsCanonicalSourceRow: {
    rawAttributes: {},
    bulkCreate: jest.fn(async (rows) => canonicalRows.push(...rows)),
    findAll: jest.fn(async () => []),
  },
};

jest.mock("@/db/database", () => mockDb);
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(async () => {
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
  }),
}));
jest.mock("@/helpers/logger", () => ({
  logger: { info: jest.fn(), error: jest.fn() },
}));
jest.mock("@/v2/ptrs/services/stage.payment-time.ptrs.service", () => ({
  buildStageColumnProjection: jest.fn(() => ({})),
}));
jest.mock("@/v2/ptrs/services/maps.compose.ptrs.service", () => ({
  composeMappedRowsForPtrs: jest.fn(),
  prepareMappedRowsContext: jest.fn(async (options) => ({ ...options })),
}));
jest.mock("@/v2/ptrs/services/ptrs.service", () => ({
  toSnake: (value) =>
    String(value || "")
      .replace(/[^a-zA-Z0-9]+/g, "_")
      .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
      .toLowerCase()
      .replace(/^_+|_+$/g, "")
      .replace(/_{2,}/g, "_"),
  buildStableInputHash: (value) => {
    const sort = (item) => {
      if (Array.isArray(item)) return item.map(sort);
      if (!item || typeof item !== "object") return item;
      return Object.fromEntries(
        Object.keys(item)
          .sort()
          .map((key) => [key, sort(item[key])]),
      );
    };
    return JSON.stringify(sort(value));
  },
}));

const {
  buildCanonicalInputSnapshot,
  getReachableDatasetIds,
  materializeCanonicalRevision,
  resolveCanonicalAdapter,
  resolveCurrentCanonicalRevisions,
} = require("./canonical.ptrs.service");

const scope = {
  customerId: "customer-1",
  ptrsId: "ptrs-1",
  profileId: "profile-1",
};

function deferred() {
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function composeRow(datasetId) {
  return {
    rows: [
      {
        row_no: 1,
        payment_amount: "10",
        _ptrsMeta: {
          sourceRawRowId: `raw-${datasetId}`,
          sourceRowNo: 1,
          joinedReferences:
            datasetId === "dataset-a" ? { "reference-a": [1] } : {},
        },
      },
    ],
  };
}

function composeDirectRow(overrides = {}) {
  return {
    rows: [
      {
        payer_entity_name: "Reporting Entity",
        payer_entity_abn: "11111111111",
        payee_entity_name: "Small Supplier",
        payee_entity_abn: "22222222222",
        invoice_reference_number: "INV-1",
        invoice_issue_date: "2026-01-01",
        payment_date: "2026-01-20",
        payment_amount: "343.20",
        ...overrides,
        _ptrsMeta: {
          sourceRawRowId: "raw-direct-1",
          sourceRowNo: 1,
          joinedReferences: { "reference-a": [1] },
        },
      },
    ],
  };
}

describe("PTRS canonical revisions", () => {
  beforeEach(() => {
    revisions.length = 0;
    canonicalRows.length = 0;
    transactions.length = 0;
    revisionSequence = 0;
    referenceVersion = "2026-01-03";
    mappings = {
      "dataset-a": accountingMappings(),
      "dataset-b": accountingMappings(),
      "dataset-direct": commonMappings(),
    };
  });

  test("creates one successful immutable accounting-event revision with source provenance", async () => {
    const result = await materializeCanonicalRevision({
      ...scope,
      datasetId: "dataset-a",
      compose: jest.fn(async ({ offset }) =>
        offset ? { rows: [] } : composeRow("dataset-a"),
      ),
    });
    expect(result.reused).toBe(false);
    expect(result.revision).toMatchObject({
      status: "succeeded",
      rowCount: 1,
      semanticKind: "accounting_event",
      canonicalVersion: "ptrs-canonical-v3",
      inputSnapshot: {
        datePolicyVersion: "sap-invoice-created-preferred-v1",
      },
    });
    expect(canonicalRows[0]).toMatchObject({
      datasetId: "dataset-a",
      sourceRawRowId: "raw-dataset-a",
      sourceRowNo: 1,
      semanticKind: "accounting_event",
      provenance: {
        sourceDatasetId: "dataset-a",
        joinedReferences: { "reference-a": [1] },
      },
    });
  });

  test("reuses identical inputs and creates a new revision after a mapping change", async () => {
    const compose = jest.fn(async ({ offset }) =>
      offset ? { rows: [] } : composeRow("dataset-a"),
    );
    const first = await materializeCanonicalRevision({
      ...scope,
      datasetId: "dataset-a",
      compose,
    });
    const identical = await materializeCanonicalRevision({
      ...scope,
      datasetId: "dataset-a",
      compose,
    });
    expect(identical.reused).toBe(true);
    mappings["dataset-a"] = mappings["dataset-a"].map((mapping, index) =>
      index === 0 ? { ...mapping, sourceColumn: "Gross Amount" } : mapping,
    );
    const changed = await materializeCanonicalRevision({
      ...scope,
      datasetId: "dataset-a",
      compose,
    });
    expect(changed.revision.id).not.toBe(first.revision.id);
    expect(
      revisions.find((revision) => revision.id === first.revision.id).status,
    ).toBe("succeeded");
  });

  test("equivalent concurrent requests resolve the unique claim race without two composers", async () => {
    const entered = deferred();
    const finish = deferred();
    const compose = jest.fn(async () => {
      entered.resolve();
      await finish.promise;
      return composeRow("dataset-a");
    });
    const requests = [0, 1].map(() =>
      materializeCanonicalRevision({
        ...scope,
        datasetId: "dataset-a",
        compose,
      }),
    );
    await entered.promise;
    const active = await Promise.race(requests);
    expect(active).toMatchObject({
      reused: true,
      revision: { status: "building" },
    });
    expect(compose).toHaveBeenCalledTimes(1);
    expect(revisions).toHaveLength(1);
    finish.resolve();
    const results = await Promise.all(requests);
    expect(results.map((result) => result.revision.id)).toEqual([
      revisions[0].id,
      revisions[0].id,
    ]);
    expect(revisions[0].status).toBe("succeeded");
  });

  test("an already-building claim returns immediately without preparation or compose", async () => {
    const entered = deferred();
    const finish = deferred();
    const compose = jest.fn(async () => {
      entered.resolve();
      await finish.promise;
      return composeRow("dataset-a");
    });
    const owner = materializeCanonicalRevision({
      ...scope,
      datasetId: "dataset-a",
      compose,
    });
    await entered.promise;
    const prepare = jest.fn();
    const active = await materializeCanonicalRevision({
      ...scope,
      datasetId: "dataset-a",
      prepare,
      compose,
    });
    expect(active.revision.status).toBe("building");
    expect(prepare).not.toHaveBeenCalled();
    expect(compose).toHaveBeenCalledTimes(1);
    finish.resolve();
    await owner;
  });

  test("prepares once and passes the same frozen inputs/context through bounded batches", async () => {
    const context = {};
    const prepare = jest.fn(async ({ preparedInput }) => {
      expect(preparedInput.profileId).toBe(scope.profileId);
      expect(preparedInput.supportConfig.profileId).toBe(scope.profileId);
      expect(Object.isFrozen(preparedInput.fieldMapRows[0])).toBe(true);
      return context;
    });
    const compose = jest.fn(async ({ afterRowNo, limit, preparedContext }) => {
      expect(limit).toBe(2000);
      expect(preparedContext).toBe(context);
      const count = afterRowNo == null ? 2000 : 1;
      return {
        rows: Array.from({ length: count }, (_, index) => ({
          payment_amount: "10",
          _ptrsMeta: { sourceRowNo: (afterRowNo || 0) + index + 1 },
        })),
      };
    });
    const result = await materializeCanonicalRevision({
      ...scope,
      datasetId: "dataset-a",
      prepare,
      compose,
    });
    expect(result.revision.rowCount).toBe(2001);
    expect(prepare).toHaveBeenCalledTimes(1);
    expect(compose).toHaveBeenCalledTimes(2);
    expect(mockDb.PtrsColumnMap.findOne).toHaveBeenCalledTimes(1);
    expect(mockDb.PtrsFieldMap.findAll).toHaveBeenCalledTimes(1);
    expect(
      mockDb.sequelize.query.mock.calls.filter(([sql]) =>
        sql.includes("REPEATABLE READ"),
      ),
    ).toHaveLength(2);
  });

  test("rollback failure preserves the build error, records failure separately and permits retry", async () => {
    const original = new Error("original adapter failure");
    const rollback = new Error("Connection terminated unexpectedly");
    const compose = jest.fn(async ({ transaction }) => {
      transaction.rollback.mockRejectedValueOnce(rollback);
      throw original;
    });
    await expect(
      materializeCanonicalRevision({
        ...scope,
        datasetId: "dataset-a",
        compose,
      }),
    ).rejects.toBe(original);
    expect(transactions).toHaveLength(3);
    expect(transactions[1].rollback).toHaveBeenCalledTimes(1);
    expect(transactions[2].commit).toHaveBeenCalledTimes(1);
    expect(revisions[0]).toMatchObject({
      status: "failed",
      failure: { message: original.message },
    });
    const { logger } = require("@/helpers/logger");
    expect(logger.error).toHaveBeenCalledWith(
      "PTRS canonical rollback failed",
      expect.objectContaining({ rollbackError: rollback.message }),
    );
    const retried = await materializeCanonicalRevision({
      ...scope,
      datasetId: "dataset-a",
      compose: async () => composeRow("dataset-a"),
    });
    expect(retried.revision.status).toBe("succeeded");
    expect(revisions[0].status).toBe("failed");
  });

  test("failure to obtain a build connection still records failure through a new transaction", async () => {
    const {
      beginTransactionWithCustomerContext,
    } = require("@/helpers/setCustomerIdRLS");
    const begin = beginTransactionWithCustomerContext.getMockImplementation();
    beginTransactionWithCustomerContext
      .mockImplementationOnce(begin)
      .mockRejectedValueOnce(new Error("pool unavailable"));
    const compose = jest.fn();
    await expect(
      materializeCanonicalRevision({
        ...scope,
        datasetId: "dataset-a",
        compose,
      }),
    ).rejects.toThrow("pool unavailable");
    expect(revisions[0].status).toBe("failed");
    expect(compose).not.toHaveBeenCalled();
  });

  test("raw changes between input capture and build fail before index preparation", async () => {
    mockDb.sequelize.query
      .mockImplementationOnce(async () => [])
      .mockImplementationOnce(async () => {
        referenceVersion = "2026-04-01";
        return [];
      });
    const prepare = jest.fn();
    await expect(
      materializeCanonicalRevision({
        ...scope,
        datasetId: "dataset-a",
        prepare,
      }),
    ).rejects.toMatchObject({ code: "CANONICAL_INPUT_CHANGED" });
    expect(prepare).not.toHaveBeenCalled();
    expect(revisions[0].status).toBe("failed");
  });

  test("failure recording cannot rewrite a succeeded revision after a lost commit acknowledgement", async () => {
    const original = new Error("commit acknowledgement lost");
    const compose = async ({ transaction }) => {
      transaction.commit.mockImplementationOnce(async () => {
        transaction.finished = "commit";
        throw original;
      });
      return composeRow("dataset-a");
    };
    await expect(
      materializeCanonicalRevision({
        ...scope,
        datasetId: "dataset-a",
        compose,
      }),
    ).rejects.toBe(original);
    expect(revisions[0].status).toBe("succeeded");
    expect(revisions[0].failure).toBeNull();
  });

  test("requested profile is authoritative even when the support record names another profile", async () => {
    mockDb.PtrsColumnMap.findOne.mockResolvedValueOnce({
      profileId: "another-profile",
      joins,
      customFields: [],
    });
    const snapshot = await buildCanonicalInputSnapshot({
      ...scope,
      datasetId: "dataset-a",
      transaction: {},
    });
    expect(mockDb.PtrsFieldMap.findAll).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({ profileId: scope.profileId }),
      }),
    );
    expect(snapshot.preparedInput.supportConfig.profileId).toBe(
      scope.profileId,
    );
    expect(snapshot.inputSnapshot.profileId).toBe(scope.profileId);
  });

  test("hashes mapping dataset ownership and freezes precisely the reachable execution graph", async () => {
    const originalJoins = joins;
    joins = {
      conditions: [
        ...joins.conditions,
        {
          from: { datasetId: "reference-a" },
          to: { datasetId: "dataset-b" },
        },
      ],
    };
    try {
      const snapshot = await buildCanonicalInputSnapshot({
        ...scope,
        datasetId: "dataset-a",
        transaction: {},
      });
      expect(
        snapshot.preparedInput.datasets.map((dataset) => dataset.id).sort(),
      ).toEqual(["dataset-a", "reference-a"]);
      expect(
        snapshot.preparedInput.supportConfig.joins.conditions,
      ).toHaveLength(1);
      expect(
        snapshot.inputSnapshot.mappings.every(
          (mapping) => mapping.datasetId === "dataset-a",
        ),
      ).toBe(true);
      expect(snapshot.mappingSignature).toContain('"datasetId":"dataset-a"');
    } finally {
      joins = originalJoins;
    }
  });

  test("a failed replacement leaves the previous successful revision current", async () => {
    const compose = jest.fn(async ({ offset }) =>
      offset ? { rows: [] } : composeRow("dataset-a"),
    );
    const successful = await materializeCanonicalRevision({
      ...scope,
      datasetId: "dataset-a",
      compose,
    });
    mappings["dataset-a"] = mappings["dataset-a"].map((mapping, index) =>
      index === 0 ? { ...mapping, sourceColumn: "Changed" } : mapping,
    );
    await expect(
      materializeCanonicalRevision({
        ...scope,
        datasetId: "dataset-a",
        compose: jest.fn(async () => {
          throw new Error("adapter failed");
        }),
      }),
    ).rejects.toThrow("adapter failed");
    expect(revisions.at(-1).status).toBe("failed");
    expect(
      revisions.find((revision) => revision.id === successful.revision.id)
        .status,
    ).toBe("succeeded");
  });

  test("dataset and joined-reference signatures stale only their dependent revision", async () => {
    const transaction = {};
    const beforeA = await buildCanonicalInputSnapshot({
      ...scope,
      datasetId: "dataset-a",
      transaction,
    });
    const beforeB = await buildCanonicalInputSnapshot({
      ...scope,
      datasetId: "dataset-b",
      transaction,
    });
    referenceVersion = "2026-03-01";
    const afterA = await buildCanonicalInputSnapshot({
      ...scope,
      datasetId: "dataset-a",
      transaction,
    });
    const afterB = await buildCanonicalInputSnapshot({
      ...scope,
      datasetId: "dataset-b",
      transaction,
    });
    expect(afterA.materialSignature).not.toBe(beforeA.materialSignature);
    expect(afterB.materialSignature).toBe(beforeB.materialSignature);
    mappings["dataset-a"] = mappings["dataset-a"].map((mapping, index) =>
      index === 0 ? { ...mapping, sourceColumn: "Changed" } : mapping,
    );
    const mappedA = await buildCanonicalInputSnapshot({
      ...scope,
      datasetId: "dataset-a",
      transaction,
    });
    const mappedB = await buildCanonicalInputSnapshot({
      ...scope,
      datasetId: "dataset-b",
      transaction,
    });
    expect(mappedA.materialSignature).not.toBe(afterA.materialSignature);
    expect(mappedB.materialSignature).toBe(afterB.materialSignature);
  });

  test("resolves independent current revisions in stable dataset order and reports a missing one", async () => {
    const compose = jest.fn(async ({ datasetId, offset }) =>
      offset ? { rows: [] } : composeRow(datasetId),
    );
    await materializeCanonicalRevision({
      ...scope,
      datasetId: "dataset-a",
      compose,
    });
    await expect(
      resolveCurrentCanonicalRevisions({ ...scope, transaction: {} }),
    ).rejects.toMatchObject({ code: "CANONICAL_REVISION_REQUIRED" });
    await materializeCanonicalRevision({
      ...scope,
      datasetId: "dataset-b",
      compose,
    });
    const selected = await resolveCurrentCanonicalRevisions({
      ...scope,
      transaction: {},
    });
    expect(selected.map((item) => item.dataset.id)).toEqual([
      "dataset-a",
      "dataset-b",
    ]);
  });

  test("dependency traversal never crosses into another transaction dataset", () => {
    const ids = getReachableDatasetIds(
      {
        conditions: [
          ...joins.conditions,
          {
            from: { datasetId: "dataset-b" },
            to: { datasetId: "reference-a" },
          },
        ],
      },
      "dataset-a",
      new Set(["dataset-b"]),
    );
    expect(Array.from(ids).sort()).toEqual(["dataset-a", "reference-a"]);
  });

  test("dispatches supported accounting-event adapters and rejects unknown adapters", () => {
    expect(resolveCanonicalAdapter(datasets["dataset-a"]).semanticKind).toBe(
      "accounting_event",
    );
    expect(resolveCanonicalAdapter(datasets["dataset-direct"])).toMatchObject({
      adapterVersion: "1",
      semanticKind: "direct_payment",
    });
    expect(() =>
      resolveCanonicalAdapter({ id: "future", adapterType: "future_adapter" }),
    ).toThrow("Unsupported PTRS canonical adapter");
    expect(() =>
      resolveCanonicalAdapter({
        id: "future-version",
        adapterType: "direct_payment",
        adapterVersion: "2",
      }),
    ).toThrow("Unsupported PTRS canonical adapter version");
  });

  test("materialises a valid direct-payment row with explicit settlement semantics and provenance", async () => {
    const result = await materializeCanonicalRevision({
      ...scope,
      datasetId: "dataset-direct",
      compose: jest.fn(async () => composeDirectRow()),
    });

    expect(result.revision).toMatchObject({
      status: "succeeded",
      rowCount: 1,
      semanticKind: "direct_payment",
      adapterType: "direct_payment",
      inputSnapshot: {
        adapterContract: {
          canonicalProjection: "mapped_canonical_row",
          sourceGroupSemantics: "provenance_only_no_event_reconstruction",
          paymentAmountSemantic: "actual_settlement_amount",
        },
      },
    });
    expect(canonicalRows[0]).toMatchObject({
      datasetId: "dataset-direct",
      sourceRawRowId: "raw-direct-1",
      semanticKind: "direct_payment",
      data: {
        payment_amount: "343.20",
        invoice_reference_number: "INV-1",
      },
      provenance: {
        sourceDatasetId: "dataset-direct",
        joinedReferences: { "reference-a": [1] },
      },
    });
  });

  test("rejects a direct-payment row with missing required values at canonical materialisation", async () => {
    await expect(
      materializeCanonicalRevision({
        ...scope,
        datasetId: "dataset-direct",
        compose: jest.fn(async () => composeDirectRow({ payment_date: "" })),
      }),
    ).rejects.toMatchObject({
      code: "DIRECT_PAYMENT_CANONICAL_ROW_INVALID",
      details: {
        datasetId: "dataset-direct",
        sourceRowNo: 1,
        missingFields: ["payment_date"],
      },
    });
    expect(revisions.at(-1)).toMatchObject({ status: "failed" });
    expect(canonicalRows).toHaveLength(0);
  });

  test("reports adapter-specific mapping gaps before composing direct rows", async () => {
    mappings["dataset-direct"] = accountingMappings().filter(
      (mapping) => mapping.canonicalField !== "payee_entity_abn",
    );

    await expect(
      buildCanonicalInputSnapshot({
        ...scope,
        datasetId: "dataset-direct",
        transaction: {},
      }),
    ).rejects.toMatchObject({
      code: "CANONICAL_MAPPING_INCOMPLETE",
      details: { missingFields: ["payee_entity_abn"] },
    });
  });
});

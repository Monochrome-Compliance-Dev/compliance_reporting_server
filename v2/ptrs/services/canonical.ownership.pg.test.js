// Opt-in, isolated PostgreSQL test. Never points at the application database.
// CANONICAL_PG_TEST_URL must name canonical_materialisation_test on loopback.
const { Sequelize, DataTypes } = require("sequelize");
const { readFileSync } = require("fs");
const path = require("path");
const { createHash, randomUUID } = require("crypto");

const mockDb = {};
jest.mock("@/db/database", () => mockDb);
jest.mock("@/helpers/nanoid_helper", () => ({
  getNanoid: () =>
    require("crypto").randomUUID().replace(/-/g, "").slice(0, 10),
}));
jest.mock("@/helpers/logger", () => ({
  logger: { info: jest.fn(), error: jest.fn() },
}));
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: async (customerId) => {
    const transaction = await mockDb.sequelize.transaction();
    await mockDb.sequelize.query(
      "SET LOCAL app.current_customer_id = :customerId",
      {
        replacements: { customerId },
        transaction,
      },
    );
    return transaction;
  },
}));
jest.mock("@/v2/ptrs/services/ptrs.service", () => ({
  toSnake: (value) => value,
  buildStableInputHash: (value) =>
    require("crypto")
      .createHash("sha256")
      .update(JSON.stringify(value))
      .digest("hex"),
}));
jest.mock("@/v2/ptrs/services/stage.payment-time.ptrs.service", () => ({
  buildStageColumnProjection: () => ({}),
}));
jest.mock("@/v2/ptrs/services/maps.compose.ptrs.service", () => ({
  prepareMappedRowsContext: async () => ({}),
}));

const testUrl = process.env.CANONICAL_PG_TEST_URL;
const suite = testUrl ? describe : describe.skip;
const scope = {
  customerId: "customer01",
  ptrsId: "ptrs000001",
  datasetId: "dataset001",
  profileId: "profile001",
};
const source = {
  id: scope.datasetId,
  customerId: scope.customerId,
  ptrsId: scope.ptrsId,
  purpose: "transaction",
  sourceFormat: "csv",
  adapterType: "direct_payment",
  adapterVersion: "1",
  status: "parsed",
  rowsCount: 1,
  updatedAt: "2026-08-01T00:00:00.000Z",
};
const fields = [
  "payer_entity_name",
  "payer_entity_abn",
  "payee_entity_name",
  "payee_entity_abn",
  "invoice_reference_number",
  "payment_amount",
  "payment_date",
  "invoice_issue_date",
];
const row = {
  payer_entity_name: "Payer",
  payer_entity_abn: "11111111111",
  payee_entity_name: "Payee",
  payee_entity_abn: "22222222222",
  invoice_reference_number: "INV-1",
  payment_amount: "10",
  payment_date: "2026-01-15",
  invoice_issue_date: "2026-01-01",
  _ptrsMeta: { sourceRowNo: 1 },
};
function deferred() {
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

suite("isolated PostgreSQL canonical ownership", () => {
  let service;
  beforeAll(async () => {
    const url = new URL(testUrl);
    if (
      !["127.0.0.1", "localhost"].includes(url.hostname) ||
      url.pathname !== "/canonical_materialisation_test"
    ) {
      throw new Error("Refusing non-isolated canonical PostgreSQL test target");
    }
    mockDb.sequelize = new Sequelize(testUrl, {
      logging: false,
      pool: { max: 5 },
    });
    mockDb.PtrsCanonicalRevision = require("../models/ptrs_canonical_revision")(
      mockDb.sequelize,
    );
    // Start from the previous schema and apply the actual new migration.
    mockDb.PtrsCanonicalSourceRow = mockDb.sequelize.define(
      "CanonicalTestRow",
      {
        id: {
          type: DataTypes.STRING(10),
          primaryKey: true,
          defaultValue: () => randomUUID().slice(0, 10),
        },
        customerId: DataTypes.STRING(10),
        ptrsId: DataTypes.STRING(10),
        datasetId: DataTypes.STRING(10),
        canonicalRevisionId: DataTypes.STRING(10),
        sourceRowNo: DataTypes.INTEGER,
        data: DataTypes.JSONB,
        provenance: DataTypes.JSONB,
      },
      { tableName: "canonical_test_rows", timestamps: false },
    );
    // These two fixture tables are owned by this test in the explicitly
    // isolated database. Resetting them also makes failed runs repeatable.
    await mockDb.PtrsCanonicalSourceRow.drop();
    await mockDb.PtrsCanonicalRevision.drop();
    await mockDb.sequelize.sync();
    await mockDb.sequelize.query(
      'DROP INDEX "ptrs_canonical_revision_active_material_ux"',
    );
    await mockDb.sequelize.query(
      'CREATE UNIQUE INDEX "ptrs_canonical_revision_success_material_ux" ON "tbl_ptrs_canonical_revision" ("customerId", "ptrsId", "datasetId", "materialSignature") WHERE status = \'succeeded\'',
    );
    mockDb.PtrsDataset = {
      findOne: async () => source,
      findAll: async () => [source],
    };
    mockDb.PtrsColumnMap = {
      findOne: async () => ({ joins: { conditions: [] }, customFields: [] }),
    };
    mockDb.PtrsFieldMap = {
      findAll: async () =>
        fields.map((field) => ({
          datasetId: scope.datasetId,
          canonicalField: field,
          sourceRole: "transaction",
          sourceColumn: field,
        })),
    };
    mockDb.PtrsImportRaw = {
      count: async () => 1,
      max: async () => source.updatedAt,
    };
    service = require("./canonical.ptrs.service");
  });
  afterAll(async () => {
    if (mockDb.sequelize) await mockDb.sequelize.close();
  });

  test("migration refuses orphan building rows without changing them; failed history and existing success survive", async () => {
    const signature = createHash("sha256").update("legacy").digest("hex");
    const values = {
      ...scope,
      adapterType: "direct_payment",
      adapterVersion: "1",
      canonicalVersion: "ptrs-canonical-v2",
      semanticKind: "direct_payment",
      sourceSignature: signature,
      mappingSignature: signature,
      enrichmentSignature: signature,
      materialSignature: signature,
      inputSnapshot: {},
      status: "building",
    };
    const old = await mockDb.PtrsCanonicalRevision.create(values);
    const migration = readFileSync(
      path.join(
        __dirname,
        "../../../db/migrations/20260831_ptrs_canonical_active_material.sql",
      ),
      "utf8",
    );
    // Keep BEGIN/ROLLBACK on one pinned connection when testing migration rejection.
    const connection = await mockDb.sequelize.connectionManager.getConnection();
    try {
      await expect(connection.query(migration)).rejects.toThrow(
        "operator review",
      );
      await connection.query("ROLLBACK");
      expect((await old.reload()).status).toBe("building");
      await old.update({ status: "failed" });
      const successful = await mockDb.PtrsCanonicalRevision.create({
        ...values,
        status: "succeeded",
      });
      await connection.query(migration);
      expect((await successful.reload()).status).toBe("succeeded");
      expect((await old.reload()).status).toBe("failed");
    } catch (error) {
      await connection.query("ROLLBACK");
      throw error;
    } finally {
      await mockDb.sequelize.connectionManager.releaseConnection(connection);
    }
  });

  test("two service requests use real PostgreSQL claims and only one composes; output publishes atomically", async () => {
    const bothLookedUp = deferred();
    let lookups = 0;
    const findOne = mockDb.PtrsCanonicalRevision.findOne.bind(
      mockDb.PtrsCanonicalRevision,
    );
    const lookup = jest
      .spyOn(mockDb.PtrsCanonicalRevision, "findOne")
      .mockImplementation(async (options) => {
        const result = await findOne(options);
        if (
          !result &&
          options.raw &&
          options.where.materialSignature &&
          lookups < 2
        ) {
          lookups += 1;
          if (lookups === 2) bothLookedUp.resolve();
          await bothLookedUp.promise;
        }
        return result;
      });
    const entered = deferred();
    const finish = deferred();
    const persisted = deferred();
    const publish = deferred();
    const bulkCreate = mockDb.PtrsCanonicalSourceRow.bulkCreate.bind(
      mockDb.PtrsCanonicalSourceRow,
    );
    const persistence = jest
      .spyOn(mockDb.PtrsCanonicalSourceRow, "bulkCreate")
      .mockImplementation(async (...args) => {
        const result = await bulkCreate(...args);
        persisted.resolve();
        await publish.promise;
        return result;
      });
    const compose = jest.fn(async () => {
      entered.resolve();
      await finish.promise;
      return { rows: [row] };
    });
    const requests = [0, 1].map(() =>
      service.materializeCanonicalRevision({ ...scope, compose }),
    );
    await entered.promise;
    try {
      const active = await Promise.race(requests);
      expect(active).toMatchObject({
        reused: true,
        revision: { status: "building" },
      });
      expect(compose).toHaveBeenCalledTimes(1);
      expect(await mockDb.PtrsCanonicalSourceRow.count()).toBe(0);
    } finally {
      finish.resolve();
    }
    await persisted.promise;
    try {
      // The insert has executed on the owner's connection but is not published.
      expect(await mockDb.PtrsCanonicalSourceRow.count()).toBe(0);
    } finally {
      publish.resolve();
      persistence.mockRestore();
    }
    const results = await Promise.all(requests);
    lookup.mockRestore();
    expect(lookups).toBe(2);
    expect(new Set(results.map((result) => result.revision.id)).size).toBe(1);
    expect(await mockDb.PtrsCanonicalSourceRow.count()).toBe(1);
    const reused = await service.materializeCanonicalRevision({
      ...scope,
      compose,
    });
    expect(reused).toMatchObject({
      reused: true,
      revision: { status: "succeeded" },
    });
    expect(compose).toHaveBeenCalledTimes(1);
  });

  test("active uniqueness is scoped to customer and permits failed attempts", async () => {
    const existing = await mockDb.PtrsCanonicalRevision.findOne({
      where: { canonicalVersion: "ptrs-canonical-v3", status: "succeeded" },
      raw: true,
    });
    const { id, createdAt, ...values } = existing;
    await expect(
      mockDb.PtrsCanonicalRevision.create({ ...values, status: "building" }),
    ).rejects.toMatchObject({
      original: { constraint: "ptrs_canonical_revision_active_material_ux" },
    });
    const other = await mockDb.PtrsCanonicalRevision.create({
      ...values,
      customerId: "customer02",
      status: "building",
    });
    expect(other.customerId).toBe("customer02");
    await expect(
      mockDb.PtrsCanonicalRevision.create({ ...values, status: "failed" }),
    ).resolves.toBeDefined();
  });

  test("terminated build connection cannot mask the original error or strand the active claim", async () => {
    const original = new Error("original materialisation failure");
    const compose = async ({ transaction }) => {
      const [[{ pid }]] = await mockDb.sequelize.query(
        "SELECT pg_backend_pid() AS pid",
        { transaction },
      );
      await mockDb.sequelize.query("SELECT pg_terminate_backend(:pid)", {
        replacements: { pid },
      });
      throw original;
    };
    const changedScope = { ...scope, profileId: "profile002" };
    await expect(
      service.materializeCanonicalRevision({ ...changedScope, compose }),
    ).rejects.toBe(original);
    const snapshot = await service.buildCanonicalInputSnapshot(changedScope);
    const failed = await mockDb.PtrsCanonicalRevision.findOne({
      where: {
        status: "failed",
        materialSignature: snapshot.materialSignature,
      },
    });
    expect(failed.failure.message).toBe(original.message);
    const retry = await service.materializeCanonicalRevision({
      ...changedScope,
      compose: async () => ({ rows: [row] }),
    });
    expect(retry.revision.status).toBe("succeeded");
  });
});

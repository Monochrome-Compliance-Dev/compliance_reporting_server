// Opt-in integration tests against a disposable database, never application data.
// ABR_CACHE_PG_TEST_URL must name ptrs_abr_cache_test on loopback.
const { Sequelize, DataTypes } = require("sequelize");
const { readFileSync } = require("fs");
const path = require("path");

jest.mock("@/db/database", () => ({}));
jest.mock("@/helpers/logger", () => ({
  logger: { logEvent: jest.fn() },
}));
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(),
}));
jest.mock("@/helpers/nanoid_helper", () => ({
  getNanoid: () =>
    require("crypto").randomUUID().replace(/-/g, "").slice(0, 10),
}));

const {
  enrichGovReferenceFromStageRows,
  persistNegativeAbrResults,
} = require("./exclusions.gov.enrichment");
const {
  parseExactAbnLookupResponse,
} = require("@/data_cleanse/abn-lookup.util");

const testUrl = process.env.ABR_CACHE_PG_TEST_URL;
const suite = testUrl ? describe : describe.skip;
const DAY_MS = 24 * 60 * 60 * 1000;
const ABN = "62008528523";
const scope = { customerId: "customer01", ptrsId: "ptrs000001" };

suite("isolated PostgreSQL ABR cache renewal", () => {
  let sequelize;
  let cacheModel;
  let govModel;
  let stageModel;
  let lookup;
  const originalGuid = process.env.ABR_GUID;

  beforeAll(async () => {
    const url = new URL(testUrl);
    if (
      !["127.0.0.1", "localhost"].includes(url.hostname) ||
      url.pathname !== "/ptrs_abr_cache_test"
    ) {
      throw new Error(
        "ABR cache tests require an isolated loopback ptrs_abr_cache_test database",
      );
    }
    sequelize = new Sequelize(testUrl, { logging: false });
    cacheModel = require("../models/ptrs_abr_lookup_cache")(sequelize);
    govModel = require("../models/ptrs_gov_entity_ref")(sequelize);
    stageModel = sequelize.define(
      "AbrStageFixture",
      {
        id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
        customerId: DataTypes.STRING(10),
        ptrsId: DataTypes.STRING(10),
        payeeEntityAbn: DataTypes.TEXT,
        data: DataTypes.JSONB,
        deletedAt: DataTypes.DATE,
      },
      { tableName: "tbl_ptrs_stage_row", timestamps: false },
    );
    // Use the existing migration, including the actual UNIQUE and CHECK constraints.
    await sequelize.query(
      readFileSync(
        path.join(
          __dirname,
          "../../..",
          "db/migrations/20260829_ptrs_abr_lookup_cache.sql",
        ),
        "utf8",
      ),
    );
    await govModel.sync();
    await stageModel.sync();
  });

  beforeEach(async () => {
    process.env.ABR_GUID = "isolated-test-guid";
    lookup = jest.fn();
    await sequelize.query(
      'TRUNCATE "tbl_ptrs_abr_lookup_cache", "tbl_ptrs_gov_entity_ref", "tbl_ptrs_stage_row"',
    );
  });

  afterAll(async () => {
    if (originalGuid === undefined) delete process.env.ABR_GUID;
    else process.env.ABR_GUID = originalGuid;
    if (sequelize) await sequelize.close();
  });

  async function stage(abn = ABN, overrides = {}) {
    return stageModel.create({
      ...scope,
      payeeEntityAbn: abn,
      data: { payee_entity_abn: abn },
      ...overrides,
    });
  }

  function negativeResult(classification, abn = ABN) {
    return {
      found: true,
      abn,
      isGovernmentEntity: classification === "INACTIVE_GOVERNMENT",
      isCurrentAbn: true,
      entityStatusCode:
        classification === "INACTIVE_GOVERNMENT" ? "Cancelled" : "Active",
    };
  }

  async function enrich() {
    return enrichGovReferenceFromStageRows({
      ...scope,
      sequelize,
      abrCacheModel: cacheModel,
      govEntityModel: govModel,
      lookupAbnByNumberFn: lookup,
      beginCustomerTransaction: async (customerId) => {
        const transaction = await sequelize.transaction();
        await sequelize.query(
          "SELECT set_config('app.current_customer_id', :customerId, true)",
          {
            replacements: { customerId },
            transaction,
          },
        );
        return transaction;
      },
    });
  }

  async function cacheRow() {
    return cacheModel.findOne({ where: { abn: ABN }, raw: true });
  }

  async function seedCache({ expired, classification = "NON_GOVERNMENT" }) {
    const checkedAt = new Date(Date.now() - 400 * DAY_MS);
    await cacheModel.bulkCreate([
      {
        id: "existing01",
        abn: ABN,
        classification,
        checkedAt,
        expiresAt: new Date(
          expired ? checkedAt.getTime() + DAY_MS : Date.now() + DAY_MS,
        ),
        createdAt: new Date(checkedAt.getTime() - DAY_MS),
        updatedAt: checkedAt,
      },
    ]);
    return cacheRow();
  }

  test.each(["NON_GOVERNMENT", "INACTIVE_GOVERNMENT"])(
    "inserts an unseen %s result with a 365-day refresh period",
    async (classification) => {
      await stage();
      lookup.mockResolvedValue(negativeResult(classification));

      const stats = await enrich();
      const row = await cacheRow();

      expect(row).toMatchObject({ abn: ABN, classification });
      expect(row.id).toHaveLength(10);
      expect(row.expiresAt.getTime() - row.checkedAt.getTime()).toBe(
        365 * DAY_MS,
      );
      expect(stats.negativeResultsCached).toBe(1);
      expect(await govModel.count()).toBe(0);
    },
  );

  test.each([
    ["NON_GOVERNMENT", "INACTIVE_GOVERNMENT"],
    ["INACTIVE_GOVERNMENT", "NON_GOVERNMENT"],
  ])(
    "refreshes expired %s to %s without replacing its identity",
    async (beforeClass, afterClass) => {
      const before = await seedCache({
        expired: true,
        classification: beforeClass,
      });
      await stage();
      lookup.mockResolvedValue(negativeResult(afterClass));

      const stats = await enrich();
      const after = await cacheRow();

      expect(lookup).toHaveBeenCalledTimes(1);
      expect(lookup).toHaveBeenCalledWith(ABN);
      expect(stats.negativeResultsCached).toBe(1);
      expect(await cacheModel.count()).toBe(1);
      expect(after.id).toBe(before.id);
      expect(after.createdAt).toEqual(before.createdAt);
      expect(after.classification).toBe(afterClass);
      for (const field of ["checkedAt", "expiresAt", "updatedAt"]) {
        expect(after[field].getTime()).toBeGreaterThan(before[field].getTime());
      }
      expect(after.expiresAt.getTime() - after.checkedAt.getTime()).toBe(
        365 * DAY_MS,
      );
    },
  );

  test.each(["NON_GOVERNMENT", "INACTIVE_GOVERNMENT"])(
    "reuses unexpired %s without ABR lookup or cache writes",
    async (classification) => {
      const before = await seedCache({ expired: false, classification });
      await stage();
      const persist = jest.spyOn(cacheModel, "bulkCreate");
      try {
        const stats = await enrich();
        expect(stats.distinctStageAbnsToCheck).toBe(0);
        expect(lookup).not.toHaveBeenCalled();
        expect(persist).not.toHaveBeenCalled();
        expect(await cacheRow()).toEqual(before);
      } finally {
        persist.mockRestore();
      }
    },
  );

  test("deduplicates a refresh batch, using the last supplied result for an ABN", async () => {
    const before = await seedCache({ expired: true });
    const checkedAt = new Date();
    const first = {
      abn: ABN,
      classification: "NON_GOVERNMENT",
      checkedAt,
      expiresAt: new Date(checkedAt.getTime() + 365 * DAY_MS),
    };
    const last = { ...first, classification: "INACTIVE_GOVERNMENT" };

    const count = await persistNegativeAbrResults({
      abrCacheModel: cacheModel,
      candidates: [first, last],
    });

    expect(count).toBe(1);
    expect(await cacheModel.count()).toBe(1);
    expect(await cacheRow()).toMatchObject({
      id: before.id,
      createdAt: before.createdAt,
      classification: last.classification,
    });
  });

  test("concurrent negative batches converge on the ABN unique key", async () => {
    const checkedAt = new Date();
    const candidate = {
      abn: ABN,
      classification: "NON_GOVERNMENT",
      checkedAt,
      expiresAt: new Date(checkedAt.getTime() + 365 * DAY_MS),
    };
    await Promise.all(
      [1, 2].map(() =>
        persistNegativeAbrResults({
          abrCacheModel: cacheModel,
          candidates: [candidate],
        }),
      ),
    );
    expect(await cacheModel.count()).toBe(1);
  });

  test("keeps checksum-invalid and malformed candidates out of ABR and the cache", async () => {
    await stage("86768265614");
    await stage("123");
    const stats = await enrich();
    expect(stats).toMatchObject({
      distinctStageAbnsToCheck: 1,
      invalidCandidateAbnsSkipped: 1,
      lookupAttempts: 0,
    });
    expect(lookup).not.toHaveBeenCalled();
    expect(await cacheModel.count()).toBe(0);
    expect(await govModel.count()).toBe(0);
  });

  test("keeps current active government results in the reference table, not the negative cache", async () => {
    await stage("86 768 265 615");
    lookup.mockResolvedValue(
      parseExactAbnLookupResponse(`
      <businessEntity>
        <ABN><identifierValue>86768265615</identifierValue><isCurrentIndicator>Y</isCurrentIndicator></ABN>
        <entityStatus><entityStatusCode>Active</entityStatusCode></entityStatus>
        <entityType><entityTypeCode>CGE</entityTypeCode><entityDescription>Commonwealth Government Entity</entityDescription></entityType>
        <mainName><organisationName>Government fixture</organisationName></mainName>
      </businessEntity>
    `),
    );

    const stats = await enrich();
    expect(stats).toMatchObject({
      governmentMatches: 1,
      inserted: 1,
      negativeResultsCached: 0,
    });
    expect(await cacheModel.count()).toBe(0);
    expect(await govModel.count()).toBe(1);
    lookup.mockClear();
    await enrich();
    expect(lookup).not.toHaveBeenCalled();
  });
});

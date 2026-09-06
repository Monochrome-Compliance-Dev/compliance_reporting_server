jest.mock("@/db/database", () => ({
  sequelize: {
    query: jest.fn(),
    transaction: jest.fn(),
  },
  PtrsGovEntityRef: {
    bulkCreate: jest.fn(),
  },
  PtrsAbrLookupCache: {
    bulkCreate: jest.fn(),
  },
}));
jest.mock("@/helpers/logger", () => ({
  logger: {
    logEvent: jest.fn(),
  },
}));
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(),
}));
jest.mock("@/helpers/nanoid_helper", () => ({
  getNanoid: () => "newcache01",
}));
jest.mock("@/data_cleanse/abn-lookup.util", () => ({
  isValidAbn: jest.fn(() => true),
  lookupAbnByNumber: jest.fn(),
  normalizeAbnDigits: jest.fn((value) =>
    String(value || "").replace(/\D/g, ""),
  ),
}));

const { Sequelize } = require("sequelize");
const { injectReplacements } = require("sequelize/lib/utils/sql");
const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  isValidAbn,
  lookupAbnByNumber,
} = require("@/data_cleanse/abn-lookup.util");
const {
  enrichGovReferenceFromStageRows,
  findUnknownGovCandidateAbns,
  persistNegativeAbrResults,
  persistNewGovernmentReferences,
} = require("./exclusions.gov.enrichment");

describe("PTRS government ABN enrichment", () => {
  const postgresDialect = new Sequelize({
    dialect: "postgres",
    logging: false,
  }).dialect;

  beforeEach(() => {
    db.sequelize.query.mockReset();
    db.sequelize.transaction.mockReset();
    db.PtrsGovEntityRef.bulkCreate.mockReset();
    db.PtrsAbrLookupCache.bulkCreate.mockReset().mockResolvedValue([]);
    lookupAbnByNumber.mockReset();
    isValidAbn.mockReset().mockReturnValue(true);
    beginTransactionWithCustomerContext.mockReset();
    db.sequelize.transaction.mockResolvedValue(makeTransaction());
    process.env.ABR_GUID = "guid";
  });

  function makeTransaction() {
    return {
      finished: false,
      commit: jest.fn(async function commit() {
        this.finished = "commit";
      }),
      rollback: jest.fn(async function rollback() {
        this.finished = "rollback";
      }),
    };
  }

  test("selects distinct unknown stage ABNs in SQL", async () => {
    db.sequelize.query.mockResolvedValueOnce([
      [{ abn: "12345678901" }, { abn: "98765432109" }],
    ]);

    const abns = await findUnknownGovCandidateAbns({
      sequelize: db.sequelize,
      transaction: { id: "tx-1" },
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(abns).toEqual(["12345678901", "98765432109"]);
    expect(db.sequelize.query.mock.calls[0][0]).toContain("SELECT DISTINCT");
    expect(db.sequelize.query.mock.calls[0][0]).toContain(
      'FROM "tbl_ptrs_stage_row" s',
    );
    expect(db.sequelize.query.mock.calls[0][0]).toContain(
      "existing_gov_abns AS MATERIALIZED",
    );
    expect(db.sequelize.query.mock.calls[0][0]).toContain(
      "LEFT JOIN existing_gov_abns g",
    );
    expect(db.sequelize.query.mock.calls[0][0]).toContain(
      "LEFT JOIN current_negative_abr_results cache",
    );
    expect(db.sequelize.query.mock.calls[0][0]).toContain(
      "NULLIF(BTRIM(s.\"payeeEntityAbn\"), '')",
    );
    expect(db.sequelize.query.mock.calls[0][0]).toContain(
      `d."abn" ~ '^[0-9]{11}$'`,
    );
  });

  test("persists ASIC as a single candidate without PostgreSQL array casting", async () => {
    db.sequelize.query.mockResolvedValueOnce([[]]).mockResolvedValueOnce([[]]);

    const inserted = await persistNewGovernmentReferences({
      sequelize: db.sequelize,
      govEntityModel: db.PtrsGovEntityRef,
      candidates: [
        {
          abn: "86768265615",
          name: "AUSTRALIAN SECURITIES & INVESTMENTS COMMISSION",
          category: "Commonwealth Government Entity",
        },
      ],
    });

    const [recheckSql, recheckOptions] = db.sequelize.query.mock.calls[1];
    expect(recheckSql).toContain("IN (:abns)");
    expect(recheckSql).not.toContain("ANY(CAST(:abns AS text[]))");
    expect(recheckOptions.replacements.abns).toEqual(["86768265615"]);
    expect(
      injectReplacements(
        recheckSql,
        postgresDialect,
        recheckOptions.replacements,
      ),
    ).toContain("IN ('86768265615')");
    expect(db.PtrsGovEntityRef.bulkCreate).toHaveBeenCalledTimes(1);
    expect(inserted).toBe(1);
  });

  test("persists multiple confirmed government candidates with one list recheck", async () => {
    db.sequelize.query
      .mockResolvedValueOnce([[]])
      .mockResolvedValueOnce([[]])
      .mockResolvedValueOnce([[]]);

    const candidates = [
      { abn: "51824753556", name: "ATO", category: "Government" },
      { abn: "86768265615", name: "ASIC", category: "Government" },
    ];
    const inserted = await persistNewGovernmentReferences({
      sequelize: db.sequelize,
      govEntityModel: db.PtrsGovEntityRef,
      candidates,
    });

    const [recheckSql, recheckOptions] = db.sequelize.query.mock.calls[2];
    expect(recheckSql).toContain("IN (:abns)");
    expect(recheckOptions.replacements.abns).toEqual([
      "51824753556",
      "86768265615",
    ]);
    expect(
      injectReplacements(
        recheckSql,
        postgresDialect,
        recheckOptions.replacements,
      ),
    ).toContain("IN ('51824753556', '86768265615')");
    expect(db.PtrsGovEntityRef.bulkCreate).toHaveBeenCalledWith(
      candidates,
      expect.objectContaining({ transaction: expect.any(Object) }),
    );
    expect(inserted).toBe(2);
  });

  test("returns safely without opening a transaction when there are no candidates", async () => {
    const inserted = await persistNewGovernmentReferences({
      sequelize: db.sequelize,
      govEntityModel: db.PtrsGovEntityRef,
      candidates: [],
    });

    expect(inserted).toBe(0);
    expect(db.sequelize.transaction).not.toHaveBeenCalled();
    expect(db.sequelize.query).not.toHaveBeenCalled();
    expect(db.PtrsGovEntityRef.bulkCreate).not.toHaveBeenCalled();
  });

  test("inserts only newly identified government ABNs", async () => {
    const candidateTransaction = makeTransaction();
    beginTransactionWithCustomerContext.mockResolvedValue(candidateTransaction);
    lookupAbnByNumber
      .mockResolvedValueOnce({
        found: true,
        abn: "11111111111",
        isCurrentAbn: true,
        entityStatusCode: "Active",
        isGovernmentEntity: true,
        entityTypeCode: "CGE",
        entityTypeDescription: "Commonwealth Government Entity",
        name: "Department One",
      })
      .mockResolvedValueOnce({
        found: true,
        abn: "22222222222",
        isGovernmentEntity: false,
        entityTypeCode: "PUB",
        entityTypeDescription: "Australian Public Company",
        name: "Private Co",
      })
      .mockResolvedValueOnce({
        found: false,
        requestAbn: "33333333333",
      });
    db.sequelize.query
      .mockResolvedValueOnce([
        [
          { abn: "11111111111" },
          { abn: "22222222222" },
          { abn: "33333333333" },
        ],
      ])
      .mockResolvedValueOnce([[]])
      .mockResolvedValueOnce([[]]);
    db.PtrsGovEntityRef.bulkCreate.mockResolvedValue([]);

    const stats = await enrichGovReferenceFromStageRows({
      sequelize: db.sequelize,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(lookupAbnByNumber).toHaveBeenCalledTimes(3);
    expect(candidateTransaction.commit).toHaveBeenCalledTimes(1);
    expect(
      candidateTransaction.commit.mock.invocationCallOrder[0],
    ).toBeLessThan(lookupAbnByNumber.mock.invocationCallOrder[0]);
    expect(db.sequelize.query.mock.calls[1][0]).toContain(
      "pg_advisory_xact_lock",
    );
    expect(db.PtrsGovEntityRef.bulkCreate).toHaveBeenCalledWith(
      [
        {
          abn: "11111111111",
          name: "Department One",
          category: "Commonwealth Government Entity",
        },
      ],
      expect.objectContaining({ transaction: expect.any(Object) }),
    );
    expect(db.PtrsAbrLookupCache.bulkCreate).toHaveBeenCalledWith(
      [
        expect.objectContaining({
          abn: "22222222222",
          classification: "NON_GOVERNMENT",
          checkedAt: expect.any(Date),
          expiresAt: expect.any(Date),
        }),
      ],
      expect.objectContaining({
        updateOnDuplicate: expect.arrayContaining([
          "classification",
          "expiresAt",
        ]),
      }),
    );
    expect(stats).toMatchObject({
      distinctStageAbnsToCheck: 3,
      lookupAttempts: 3,
      governmentMatches: 1,
      inserted: 1,
      nonGovernmentCount: 1,
      unresolvedCount: 1,
      negativeResultsCached: 1,
    });
    const [cached] = db.PtrsAbrLookupCache.bulkCreate.mock.calls[0][0];
    expect(cached.expiresAt.getTime() - cached.checkedAt.getTime()).toBe(
      365 * 24 * 60 * 60 * 1000,
    );
  });

  test("deduplicates negative results by ABN and targets only refresh fields", async () => {
    const first = {
      abn: "62008528523",
      classification: "NON_GOVERNMENT",
      checkedAt: new Date("2026-08-31T00:00:00Z"),
      expiresAt: new Date("2027-08-31T00:00:00Z"),
    };
    const latest = { ...first, classification: "INACTIVE_GOVERNMENT" };
    const other = { ...first, abn: "86768265615" };

    const persisted = await persistNegativeAbrResults({
      abrCacheModel: db.PtrsAbrLookupCache,
      candidates: [first, other, latest],
    });

    expect(persisted).toBe(2);
    expect(db.PtrsAbrLookupCache.bulkCreate).toHaveBeenCalledWith(
      [latest, other],
      {
        conflictAttributes: ["abn"],
        updateOnDuplicate: [
          "classification",
          "checkedAt",
          "expiresAt",
          "updatedAt",
        ],
      },
    );
  });

  test("generates an ABN conflict target with the installed Sequelize and real model", async () => {
    const sequelize = new Sequelize({ dialect: "postgres", logging: false });
    const cacheModel = require("../models/ptrs_abr_lookup_cache")(sequelize);
    const query = jest.spyOn(sequelize, "query").mockResolvedValue([]);
    try {
      await persistNegativeAbrResults({
        abrCacheModel: cacheModel,
        candidates: [
          {
            abn: "62008528523",
            classification: "NON_GOVERNMENT",
            checkedAt: new Date("2026-08-31T00:00:00Z"),
            expiresAt: new Date("2027-08-31T00:00:00Z"),
          },
        ],
      });

      const sql = query.mock.calls[0][0];
      expect(sql).toContain('ON CONFLICT ("abn") DO UPDATE SET');
      expect(sql).not.toContain('ON CONFLICT ("id")');
      const updateClause = sql.split("DO UPDATE SET")[1].split("RETURNING")[0];
      expect(updateClause).not.toMatch(/"(?:id|createdAt)"=/);
      for (const field of [
        "classification",
        "checkedAt",
        "expiresAt",
        "updatedAt",
      ]) {
        expect(updateClause).toContain(`"${field}"=EXCLUDED."${field}"`);
      }
    } finally {
      query.mockRestore();
      await sequelize.close();
    }
  });

  test("does not suppress a cache persistence failure", async () => {
    db.PtrsAbrLookupCache.bulkCreate.mockRejectedValueOnce(
      new Error("write failed"),
    );
    await expect(
      persistNegativeAbrResults({
        abrCacheModel: db.PtrsAbrLookupCache,
        candidates: [{ abn: "62008528523", classification: "NON_GOVERNMENT" }],
      }),
    ).rejects.toThrow("write failed");
  });

  test("preserves exclusion safety when ABR lookup fails", async () => {
    beginTransactionWithCustomerContext.mockResolvedValue(makeTransaction());
    db.sequelize.query.mockResolvedValueOnce([[{ abn: "11111111111" }]]);
    lookupAbnByNumber.mockRejectedValueOnce(new Error("ABR unavailable"));

    const stats = await enrichGovReferenceFromStageRows({
      sequelize: db.sequelize,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(db.PtrsGovEntityRef.bulkCreate).not.toHaveBeenCalled();
    expect(db.PtrsAbrLookupCache.bulkCreate).not.toHaveBeenCalled();
    expect(stats.lookupFailures).toBe(1);
    expect(stats.inserted).toBe(0);
  });

  test("skips checksum-invalid candidates before external ABR lookup", async () => {
    beginTransactionWithCustomerContext.mockResolvedValue(makeTransaction());
    db.sequelize.query.mockResolvedValueOnce([[{ abn: "11111111111" }]]);
    isValidAbn.mockReturnValueOnce(false);

    const stats = await enrichGovReferenceFromStageRows({
      sequelize: db.sequelize,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(lookupAbnByNumber).not.toHaveBeenCalled();
    expect(stats).toMatchObject({
      distinctStageAbnsToCheck: 1,
      invalidCandidateAbnsSkipped: 1,
      lookupAttempts: 0,
      lookupFailures: 0,
      unresolvedCount: 0,
    });
  });

  test("keeps an ABR exception distinct from a confirmed non-government result", async () => {
    beginTransactionWithCustomerContext.mockResolvedValue(makeTransaction());
    db.sequelize.query.mockResolvedValueOnce([[{ abn: "11111111111" }]]);
    lookupAbnByNumber.mockResolvedValueOnce({
      found: false,
      requestAbn: "11111111111",
      exception: "ABN not found",
    });

    const stats = await enrichGovReferenceFromStageRows({
      sequelize: db.sequelize,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(stats.lookupFailures).toBe(1);
    expect(stats.nonGovernmentCount).toBe(0);
    expect(db.PtrsGovEntityRef.bulkCreate).not.toHaveBeenCalled();
    expect(db.PtrsAbrLookupCache.bulkCreate).not.toHaveBeenCalled();
  });

  test("does not call ABR when every staged ABN is already cached", async () => {
    beginTransactionWithCustomerContext.mockResolvedValue(makeTransaction());
    db.sequelize.query.mockResolvedValueOnce([[]]);

    await enrichGovReferenceFromStageRows({
      sequelize: db.sequelize,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(lookupAbnByNumber).not.toHaveBeenCalled();
    expect(db.PtrsGovEntityRef.bulkCreate).not.toHaveBeenCalled();
    expect(db.PtrsAbrLookupCache.bulkCreate).not.toHaveBeenCalled();
  });

  test("does not call ABR or insert when ABR_GUID is unavailable", async () => {
    beginTransactionWithCustomerContext.mockResolvedValue(makeTransaction());
    db.sequelize.query.mockResolvedValueOnce([[{ abn: "11111111111" }]]);
    delete process.env.ABR_GUID;

    const stats = await enrichGovReferenceFromStageRows({
      sequelize: db.sequelize,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(stats.skipped).toBe("ABR_GUID missing");
    expect(lookupAbnByNumber).not.toHaveBeenCalled();
    expect(db.PtrsGovEntityRef.bulkCreate).not.toHaveBeenCalled();
  });

  test("keeps inactive government results out of the exclusion reference and caches their ABR classification", async () => {
    beginTransactionWithCustomerContext.mockResolvedValue(makeTransaction());
    db.sequelize.query.mockResolvedValueOnce([
      [{ abn: "11111111111" }, { abn: "22222222222" }],
    ]);
    lookupAbnByNumber
      .mockResolvedValueOnce({
        found: true,
        abn: "11111111111",
        isCurrentAbn: false,
        entityStatusCode: "Cancelled",
        isGovernmentEntity: true,
      })
      .mockResolvedValueOnce({
        found: true,
        abn: "22222222222",
        isCurrentAbn: true,
        entityStatusCode: "Cancelled",
        isGovernmentEntity: true,
      });

    const stats = await enrichGovReferenceFromStageRows({
      sequelize: db.sequelize,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(stats.inactiveGovernmentCount).toBe(2);
    expect(db.PtrsGovEntityRef.bulkCreate).not.toHaveBeenCalled();
    expect(db.PtrsAbrLookupCache.bulkCreate).toHaveBeenCalledWith(
      [
        expect.objectContaining({
          abn: "11111111111",
          classification: "INACTIVE_GOVERNMENT",
        }),
        expect.objectContaining({
          abn: "22222222222",
          classification: "INACTIVE_GOVERNMENT",
        }),
      ],
      expect.any(Object),
    );
  });

  test("rechecks confirmed candidates and prevents duplicate insertion", async () => {
    beginTransactionWithCustomerContext.mockResolvedValue(makeTransaction());
    db.sequelize.query
      .mockResolvedValueOnce([[{ abn: "11111111111" }]])
      .mockResolvedValueOnce([[]])
      .mockResolvedValueOnce([[{ abn: "11111111111" }]]);
    lookupAbnByNumber.mockResolvedValueOnce({
      found: true,
      abn: "11111111111",
      isCurrentAbn: true,
      entityStatusCode: "Active",
      isGovernmentEntity: true,
      entityTypeCode: "CGE",
      name: "Known Department",
    });

    await enrichGovReferenceFromStageRows({
      sequelize: db.sequelize,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(db.PtrsGovEntityRef.bulkCreate).not.toHaveBeenCalled();
  });
});

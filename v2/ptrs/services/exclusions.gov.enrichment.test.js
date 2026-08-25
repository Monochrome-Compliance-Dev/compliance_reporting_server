jest.mock("@/db/database", () => ({
  sequelize: {
    query: jest.fn(),
    transaction: jest.fn(),
  },
  PtrsGovEntityRef: {
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
jest.mock("@/data_cleanse/abn-lookup.util", () => ({
  lookupAbnByNumber: jest.fn(),
}));

const { Sequelize } = require("sequelize");
const { injectReplacements } = require("sequelize/lib/utils/sql");
const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { lookupAbnByNumber } = require("@/data_cleanse/abn-lookup.util");
const {
  enrichGovReferenceFromStageRows,
  findUnknownGovCandidateAbns,
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
    lookupAbnByNumber.mockReset();
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
      'LEFT JOIN "tbl_ptrs_gov_entity_ref" g',
    );
  });

  test("persists ASIC as a single candidate without PostgreSQL array casting", async () => {
    db.sequelize.query
      .mockResolvedValueOnce([[]])
      .mockResolvedValueOnce([[]]);

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
        [{ abn: "11111111111" }, { abn: "22222222222" }, { abn: "33333333333" }],
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
    expect(stats).toMatchObject({
      distinctStageAbnsToCheck: 3,
      lookupAttempts: 3,
      governmentMatches: 1,
      inserted: 1,
      nonGovernmentCount: 1,
      unresolvedCount: 1,
    });
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
    expect(stats.lookupFailures).toBe(1);
    expect(stats.inserted).toBe(0);
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

  test("does not cache a government result unless its ABN is current and active", async () => {
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

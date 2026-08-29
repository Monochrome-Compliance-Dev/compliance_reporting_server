jest.mock("@/db/database", () => ({
  PtrsSbiUpload: { findOne: jest.fn() },
  sequelize: {
    QueryTypes: { SELECT: "SELECT" },
    query: jest.fn(),
  },
}));
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(async () => ({
    finished: false,
    commit: jest.fn(function commit() {
      this.finished = "commit";
    }),
    rollback: jest.fn(),
  })),
}));

const db = require("@/db/database");
const { reapplyLatestResults } = require("./sbi.ptrs.service");

describe("reapplyLatestResults", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    db.PtrsSbiUpload.findOne.mockResolvedValue({
      id: "upload0001",
      status: "APPLIED",
    });
  });

  test("reapplies SBI evidence set-wise without hydrating Stage JSON", async () => {
    const aggregateStats = {
      totalRows: 309280,
      excludedRows: 39445,
      rowsWithPayeeAbn: 260000,
      matchedAbns: 250000,
      missingAbnRows: 9835,
      invalidMatchRows: 0,
      unknownOutcomeRows: 0,
      dataChangeRows: 250000,
      historyCheckRows: 250000,
    };
    const noOpStats = {
      ...aggregateStats,
      dataChangeRows: 0,
      historyCheckRows: 0,
    };
    db.sequelize.query
      .mockResolvedValueOnce([aggregateStats])
      .mockResolvedValueOnce([{ affectedRows: 250000, historyRows: 250000 }])
      .mockResolvedValueOnce([noOpStats]);

    const first = await reapplyLatestResults({
      customerId: "customer01",
      ptrsId: "ptrs000001",
      userId: "user000001",
    });
    const second = await reapplyLatestResults({
      customerId: "customer01",
      ptrsId: "ptrs000001",
      userId: "user000001",
    });

    const publicStats = {
      totalRows: 309280,
      excludedRows: 39445,
      rowsWithPayeeAbn: 260000,
      matchedAbns: 250000,
      missingAbnRows: 9835,
      invalidMatchRows: 0,
      unknownOutcomeRows: 0,
    };
    expect(first.counts).toEqual({
      ...publicStats,
      affectedRows: 250000,
      historyRows: 250000,
    });
    expect(second.counts).toEqual({
      ...publicStats,
      affectedRows: 0,
      historyRows: 0,
    });
    expect(db.sequelize.query).toHaveBeenCalledTimes(3);

    const updateSql = db.sequelize.query.mock.calls[1][0];
    expect(updateSql).toContain('UPDATE "tbl_ptrs_stage_row" stage_row');
    expect(updateSql).toContain('INSERT INTO "tbl_ptrs_sbi_row_change"');
    expect(updateSql).toContain("jsonb_build_object(");
    expect(updateSql).toContain('AS "affectedRows"');
    expect(updateSql).toContain('AS "historyRows"');
    expect(updateSql).not.toContain('RETURNING stage_row."data"');
    expect(updateSql).not.toContain('RETURNING stage_row."meta"');
    expect(updateSql).toContain("existing_changes AS MATERIALIZED");
    expect(updateSql).toContain("candidate_ids AS MATERIALIZED");
  });
});

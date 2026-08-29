jest.mock("@/db/database", () => ({
  Ptrs: { findOne: jest.fn() },
  PtrsSbiUpload: {
    findOne: jest.fn(),
    create: jest.fn(),
    update: jest.fn(),
  },
  PtrsSbiResult: { bulkCreate: jest.fn() },
  PtrsStageRow: { findAll: jest.fn() },
  PtrsSbiRowChange: { bulkCreate: jest.fn() },
  sequelize: {
    QueryTypes: { SELECT: "SELECT" },
    query: jest.fn(),
  },
}));
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(),
}));

const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  buildSbiExportSql,
  buildSbiValidationSql,
  exportAbnCsv,
  getStatus,
  importResults,
  validateAppliedSbi,
} = require("./sbi.ptrs.service");

function createTransaction() {
  return {
    finished: false,
    commit: jest.fn(function commit() {
      this.finished = "commit";
    }),
    rollback: jest.fn(function rollback() {
      this.finished = "rollback";
    }),
  };
}

describe("bounded SBI workflow", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    beginTransactionWithCustomerContext.mockImplementation(async () =>
      createTransaction(),
    );
    db.Ptrs.findOne.mockResolvedValue({ id: "ptrs-1" });
  });

  test("imports results through aggregate and set-based SQL only", async () => {
    db.PtrsSbiUpload.create.mockResolvedValue({ id: "upload-1" });
    db.sequelize.query
      .mockResolvedValueOnce([
        {
          totalRows: 309280,
          excludedRows: 100,
          rowsWithPayeeAbn: 300000,
          matchedAbns: 200000,
          missingAbnRows: 9180,
          invalidMatchRows: 0,
          unknownOutcomeRows: 0,
          dataChangeRows: 200000,
          historyCheckRows: 200000,
        },
      ])
      .mockResolvedValueOnce([{ affectedRows: 200000, historyRows: 200000 }]);

    const result = await importResults({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      userId: "user-1",
      file: {
        originalname: "sbi.csv",
        buffer: Buffer.from(
          [
            "Year,ABN,Outcome",
            "2026,11111111111,Small business for payment times reporting",
            "2026,22222222222,Not a small business for payment times reporting",
          ].join("\n"),
        ),
      },
    });

    expect(result.status).toBe("APPLIED_WITH_WARNINGS");
    expect(result.counts).toMatchObject({
      totalStageRows: 309280,
      matchedAbns: 200000,
      affectedRows: 200000,
      historyRows: 200000,
    });
    expect(db.PtrsSbiResult.bulkCreate).toHaveBeenCalledWith(
      expect.arrayContaining([
        expect.objectContaining({
          sbiUploadId: "upload-1",
          abn: "11111111111",
          isValidAbn: true,
        }),
      ]),
      expect.objectContaining({ validate: false }),
    );
    expect(db.sequelize.query).toHaveBeenCalledTimes(2);
    expect(db.sequelize.query.mock.calls[0][0]).toContain(
      'AS "missingAbnRows"',
    );
    expect(db.sequelize.query.mock.calls[1][0]).toContain(
      'INSERT INTO "tbl_ptrs_sbi_row_change"',
    );
    expect(db.sequelize.query.mock.calls[1][0]).toContain(
      'UPDATE "tbl_ptrs_stage_row" stage_row',
    );
    expect(db.PtrsStageRow.findAll).not.toHaveBeenCalled();
    expect(db.PtrsSbiRowChange.bulkCreate).not.toHaveBeenCalled();
  });

  test("exports distinct ordered ABNs as one narrow SQL result", async () => {
    db.sequelize.query.mockResolvedValue([
      { csvText: "ABN\n11111111111\n22222222222\n" },
    ]);

    await expect(
      exportAbnCsv({ customerId: "customer-1", ptrsId: "ptrs-1" }),
    ).resolves.toBe("ABN\n11111111111\n22222222222\n");

    const sql = db.sequelize.query.mock.calls[0][0];
    expect(sql).toContain("SELECT DISTINCT");
    expect(sql).toContain("string_agg(abns.abn");
    expect(sql).toContain("ORDER BY abns.abn");
    expect(sql).not.toContain('SELECT stage_row."data"');
    expect(db.PtrsStageRow.findAll).not.toHaveBeenCalled();
  });

  test("validates with aggregate counts and bounded issue samples", async () => {
    db.PtrsSbiUpload.findOne.mockResolvedValue({
      id: "upload-1",
      status: "APPLIED_WITH_WARNINGS",
    });
    db.sequelize.query.mockResolvedValue([
      {
        totalRows: 309280,
        excludedRows: 100,
        blockerCount: 2,
        warningCount: 3,
        totalResults: 20,
        missingPayeeAbnCount: 2,
        blockers: [{ code: "PAYEE_ABN_MISSING" }],
        warnings: [{ code: "SBI_NO_MATCH" }],
      },
    ]);

    const result = await validateAppliedSbi({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      mode: "process",
    });

    expect(result.status).toBe("BLOCKED");
    expect(result.counts).toMatchObject({
      totalRows: 309280,
      blockers: 2,
      warnings: 3,
      missingPayeeAbnCount: 2,
    });
    expect(result.blockers).toHaveLength(1);
    expect(result.warnings).toHaveLength(1);
    expect(db.sequelize.query.mock.calls[0][1].replacements.sampleLimit).toBe(
      200,
    );
    expect(db.PtrsStageRow.findAll).not.toHaveBeenCalled();
  });

  test("reports one blocker when no applied SBI upload exists", async () => {
    db.PtrsSbiUpload.findOne.mockResolvedValue(null);

    const result = await validateAppliedSbi({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      mode: "process",
    });

    expect(result.status).toBe("BLOCKED");
    expect(result.counts.blockers).toBe(1);
    expect(result.blockers).toHaveLength(1);
    expect(result.blockers[0].code).toBe("SBI_MISSING");
    expect(db.sequelize.query).not.toHaveBeenCalled();
  });

  test("status reads only the latest upload summary", async () => {
    db.PtrsSbiUpload.findOne.mockResolvedValue({
      id: "upload-1",
      status: "APPLIED",
      fileName: "sbi.csv",
      fileHash: "hash",
      rawRowCount: 10,
      parsedAbnCount: 9,
      summary: { stage: { affectedRows: 8 } },
      createdAt: "2026-08-28T00:00:00.000Z",
    });

    const result = await getStatus({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(result.latestUpload).toMatchObject({
      id: "upload-1",
      parsedAbnCount: 9,
    });
    expect(db.PtrsSbiUpload.findOne.mock.calls[0][0].attributes).toEqual([
      "id",
      "status",
      "fileName",
      "fileHash",
      "rawRowCount",
      "parsedAbnCount",
      "summary",
      "createdAt",
    ]);
    expect(db.PtrsStageRow.findAll).not.toHaveBeenCalled();
  });

  test("SQL builders keep Stage projections and validation samples bounded", () => {
    const validationSql = buildSbiValidationSql();
    const exportSql = buildSbiExportSql();

    expect(validationSql).toContain('AS "blockerCount"');
    expect(validationSql).toContain('AS "warningCount"');
    expect(validationSql).toContain(
      "ranked_issues.sample_rank <= :sampleLimit",
    );
    expect(validationSql).not.toContain("SELECT stage_row.*");
    expect(exportSql).toContain("SELECT DISTINCT");
    expect(exportSql).not.toContain("SELECT stage_row.*");
  });
});

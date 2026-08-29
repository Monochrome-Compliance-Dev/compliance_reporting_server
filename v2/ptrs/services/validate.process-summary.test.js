jest.mock("@/db/database", () => ({
  Ptrs: { findOne: jest.fn() },
  sequelize: {
    query: jest.fn(),
    QueryTypes: { SELECT: "SELECT" },
  },
}));
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(),
}));
jest.mock("./payment-observations.ptrs.service", () => ({
  buildPaymentObservationsCte: jest.fn(
    () => "payment_observations AS (SELECT 1)",
  ),
  getPaymentObservationReplacements: jest.fn(({ customerId, ptrsId }) => ({
    customerId,
    ptrsId,
  })),
  setPaymentObservationWorkMem: jest.fn(),
}));

const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  setPaymentObservationWorkMem,
} = require("./payment-observations.ptrs.service");
const {
  buildProcessValidateSummarySql,
  getValidate,
  getProcessValidateSummary,
  validate,
} = require("./validate.ptrs.service");

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

describe("bounded PTRS process validation", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    beginTransactionWithCustomerContext.mockResolvedValue(makeTransaction());
    db.Ptrs.findOne.mockResolvedValue({ id: "ptrs-1" });
  });

  test("returns aggregate counts without materialising validation rows in Node", async () => {
    db.sequelize.query.mockResolvedValue([
      {
        totalRows: 309280,
        missingPayeeAbnCount: 201,
        invalidPayeeAbnCount: 3,
        missingPayerAbnCount: 0,
        invalidPayerAbnCount: 0,
        missingPaymentTimeReferenceDateCount: 0,
        invalidPaymentTimeReferenceDateCount: 0,
        invalidInvoiceIssueDateCount: 2,
        missingPaymentDateCount: 0,
        invalidPaymentDateCount: 0,
        paymentBeforeInvoiceCount: 1,
        missingPaymentAmountCount: 0,
        invalidPaymentAmountCount: 0,
        duplicatesSuspectedCount: 4,
        smallBusinessUnknownCount: 0,
        missing_term_days: 0,
        invalid_term_days: 0,
        missing_time_days: 0,
        invalid_time_days: 0,
        blockerCount: 204,
        warningCount: 7,
        blockers: Array.from({ length: 200 }, (_, index) => ({
          code: "PAYEE_ABN_MISSING",
          rowNo: index + 1,
        })),
        warnings: [{ code: "DUPLICATE_SUSPECTED", rowNo: 1 }],
      },
    ]);

    const result = await getProcessValidateSummary({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(result).toEqual(
      expect.objectContaining({
        status: "BLOCKED",
        blockers: expect.any(Array),
        warnings: [{ code: "DUPLICATE_SUSPECTED", rowNo: 1 }],
        counts: expect.objectContaining({
          totalRows: 309280,
          blockers: 204,
          warnings: 7,
          missingPayeeAbnCount: 201,
          invalidPayeeAbnCount: 3,
        }),
      }),
    );
    expect(setPaymentObservationWorkMem).toHaveBeenCalledTimes(1);
    expect(result.blockers).toHaveLength(200);
    const [sql] = db.sequelize.query.mock.calls[0];
    expect(sql).toContain("validation_counts AS");
    expect(sql).toContain("COUNT(*) FILTER");
    expect(sql).toContain("ROW_NUMBER() OVER");
    expect(sql).not.toContain("FROM payment_observations");
    expect(sql).toContain("sample_rank <= :sampleLimit");
  });

  test("builds one scalar validation result over the observation relation", () => {
    const sql = buildProcessValidateSummarySql();

    expect(sql).toContain("validation_source AS MATERIALIZED (");
    expect(sql).toContain(
      "invoice_payload.\"data\"->>'payee_entity_abn' AS payee_abn_raw",
    );
    expect(sql).not.toContain("FROM payment_observations");
    expect(sql).toContain("issue_summary AS");
  });

  test.each([
    ["validate", validate, "run"],
    ["getValidate", getValidate, "read"],
  ])(
    "%s uses the same bounded aggregate and sample query",
    async (_, fn, mode) => {
      db.sequelize.query.mockResolvedValue([
        {
          totalRows: 145190,
          blockerCount: 6541,
          warningCount: 238601,
          blockers: [{ code: "PAYEE_ABN_MISSING" }],
          warnings: [{ code: "DUPLICATE_SUSPECTED" }],
        },
      ]);

      const result = await fn({
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        userId: "user-1",
      });

      expect(result).toMatchObject({
        status: "BLOCKED",
        mode,
        counts: {
          totalRows: 145190,
          blockers: 6541,
          warnings: 238601,
        },
        blockers: [{ code: "PAYEE_ABN_MISSING" }],
        warnings: [{ code: "DUPLICATE_SUSPECTED" }],
      });
      expect(db.Ptrs.findOne).toHaveBeenCalledWith(
        expect.objectContaining({
          where: { id: "ptrs-1", customerId: "customer-1" },
        }),
      );
      expect(db.sequelize.query).toHaveBeenCalledTimes(1);
      expect(db.sequelize.query.mock.calls[0][1].replacements.sampleLimit).toBe(
        200,
      );
    },
  );
});

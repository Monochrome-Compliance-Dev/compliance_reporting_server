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
    normalisationResultId: "norm-1",
  })),
  resolveNormalisationResultId: jest.fn(async () => "norm-1"),
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
  buildValidateSummarySql,
  getValidate,
  getValidateSummary,
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
    expect(result.timings).toEqual(
      expect.objectContaining({
        transactionAcquire: expect.objectContaining({
          startedAt: expect.any(String),
          finishedAt: expect.any(String),
          elapsedMs: expect.any(Number),
        }),
        normalisationResultLookup: expect.objectContaining({
          elapsedMs: expect.any(Number),
        }),
        validationQuery: expect.objectContaining({
          elapsedMs: expect.any(Number),
        }),
        commit: expect.objectContaining({ elapsedMs: expect.any(Number) }),
      }),
    );
    expect(result.blockers).toHaveLength(200);
    const [sql] = db.sequelize.query.mock.calls[0];
    expect(sql).toContain("validation_counts AS");
    expect(sql).toContain("COUNT(*) FILTER");
    expect(sql).toContain("ROW_NUMBER() OVER");
    expect(sql).toContain("FROM payment_observations");
    expect(sql).toContain("sample_rank <= :sampleLimit");
  });

  test("distinguishes missing and supplied-invalid payer ABNs using the common validation result", () => {
    const sql = buildProcessValidateSummarySql();

    expect(sql).toContain("AS payer_abn_supplied");
    expect(sql).toContain("AS payer_abn_valid");
    expect(sql).toContain("COUNT(*) FILTER (WHERE NOT payer_abn_supplied)");
    expect(sql).toContain("WHERE payer_abn_supplied AND NOT payer_abn_valid");
    expect(sql).toContain("NOT numbered.payer_abn_supplied");
    expect(sql).toContain("numbered.payer_abn_supplied");
    expect(sql).toContain("AND NOT numbered.payer_abn_valid");
    expect(sql).toContain("'PAYER_ABN_MISSING'");
    expect(sql).toContain("'PAYER_ABN_INVALID'");
    expect(sql).toContain("% 89 = 0");
    expect(sql).toContain("'value', numbered.payer_abn_raw");
  });

  test("blocks a non-empty SAP obligation population with no ZP rows", async () => {
    db.sequelize.query.mockResolvedValue([
      {
        totalRows: 0,
        sourceStageRows: 1138,
        accountingStageRows: 1138,
        excludedStageRows: 23,
        invoiceObligationRows: 1102,
        zpPaymentRows: 0,
        viableObligationRows: 1080,
        viablePaymentRows: 0,
        paymentAllocationRows: 0,
        paymentObservationRows: 0,
        normalisationExceptionRows: 13,
        blockerCount: 1,
        warningCount: 0,
        blockers: [{ code: "ZP_PAYMENT_ROWS_MISSING" }],
        warnings: [],
      },
    ]);

    const result = await getProcessValidateSummary({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(result).toMatchObject({
      status: "BLOCKED",
      counts: {
        sourceStageRows: 1138,
        excludedRows: 23,
        invoiceObligationRows: 1102,
        zpPaymentRows: 0,
        paymentObservationRows: 0,
        blockers: 1,
      },
      blockers: [{ code: "ZP_PAYMENT_ROWS_MISSING" }],
    });
  });

  test("blocks viable obligations and ZPs producing no observations", async () => {
    db.sequelize.query.mockResolvedValue([
      {
        totalRows: 0,
        sourceStageRows: 20,
        accountingStageRows: 20,
        invoiceObligationRows: 10,
        zpPaymentRows: 10,
        viableObligationRows: 10,
        viablePaymentRows: 10,
        paymentAllocationRows: 0,
        paymentObservationRows: 0,
        normalisationExceptionRows: 10,
        blockerCount: 1,
        warningCount: 0,
        blockers: [{ code: "PAYMENT_OBSERVATIONS_EMPTY" }],
        warnings: [],
      },
    ]);

    const result = await getProcessValidateSummary({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(result.status).toBe("BLOCKED");
    expect(result.counts).toMatchObject({
      zpPaymentRows: 10,
      viableObligationRows: 10,
      viablePaymentRows: 10,
      paymentObservationRows: 0,
    });
    expect(result.blockers).toEqual([{ code: "PAYMENT_OBSERVATIONS_EMPTY" }]);
  });

  test("surfaces active normalisation exceptions but omits excluded sources", async () => {
    db.sequelize.query.mockResolvedValue([
      {
        totalRows: 4,
        paymentObservationRows: 4,
        normalisationExceptionRows: 2,
        blockerCount: 1,
        warningCount: 0,
        blockers: [{ code: "UNMATCHED_PAYMENT", excluded: false }],
        warnings: [],
      },
    ]);

    const result = await getProcessValidateSummary({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(result.status).toBe("BLOCKED");
    expect(result.counts.normalisationExceptionRows).toBe(2);
    expect(result.blockers).toContainEqual(
      expect.objectContaining({ code: "UNMATCHED_PAYMENT" }),
    );
    expect(result.warnings).toEqual([]);
  });

  test("builds one scalar validation result over the observation relation", () => {
    const sql = buildProcessValidateSummarySql();

    expect(sql).toContain("validation_source AS MATERIALIZED (");
    expect(sql).toContain(
      "observation.\"data\"->>'payee_entity_abn' AS payee_abn_raw",
    );
    expect(sql).toContain("FROM payment_observations");
    expect(sql).toContain("pipeline_counts AS MATERIALIZED (");
    expect(sql).toContain("'ZP_PAYMENT_ROWS_MISSING'");
    expect(sql).toContain("'PAYMENT_OBSERVATIONS_EMPTY'");
    expect(sql).toContain("FROM payment_normalisation_exceptions exception");
    expect(sql).toContain("WHERE NOT source.excluded");
    expect(sql).toContain("exception.reason_code");
    expect(sql).toContain("WHEN 'UNRECOGNISED_DOCUMENT_TYPE'");
    expect(sql).toContain("WHEN 'UNMATCHED_ADJUSTMENT'");
    expect(sql).toContain("WHEN 'UNMATCHED_PAYMENT'");
    expect(sql).toContain("WHEN 'UNMATCHED_OBLIGATION_OFFSET'");
    expect(sql).toContain("WHEN 'UNMATCHED_KG_REVERSAL'");
    expect(sql).toContain("no recognised ZP or KZ settlement rows");
    expect(sql).toContain("WHEN 'MAPPING_EXCEPTION'");
    expect(sql).toContain("issue_summary AS");
  });

  test("uses the current source CTE throughout the aggregated summary", async () => {
    db.sequelize.query.mockResolvedValue([]);

    await getValidateSummary({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      profileId: "profile-1",
    });

    const sql = db.sequelize.query.mock.calls
      .map(([statement]) => statement)
      .join("\n");
    expect(sql).toContain("FROM payment_normalisation_source_rows");
    expect(sql).not.toContain("payment_observation_source_rows");
  });

  test("standalone summary projects the persisted observation population once", () => {
    const sql = buildValidateSummarySql();

    expect(sql).toContain("validate_summary_observations AS MATERIALIZED");
    expect(sql).toContain("validate_summary_reference_kinds AS");
    expect(sql).toContain("validate_summary_payment_terms AS");
    expect(sql).toContain("validate_summary_missing AS");
    expect(sql.match(/payment_observations AS \(SELECT 1\)/g)).toHaveLength(1);
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

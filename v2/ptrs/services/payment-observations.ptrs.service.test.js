jest.mock("@/db/database", () => ({
  sequelize: {
    QueryTypes: { SELECT: "SELECT" },
    query: jest.fn(),
  },
}));
jest.mock("./payment-normalisation.ptrs.service", () => ({
  buildPersistedPaymentNormalisationCte: jest.fn(
    () => `payment_normalisation_result AS (
      SELECT * FROM "tbl_ptrs_payment_normalisation_result"
      WHERE "id" = :normalisationResultId
    ),
    payment_normalisation_source_rows AS (
      SELECT * FROM "tbl_ptrs_payment_normalisation_row"
    ),
    payment_normalisation_payment_allocations AS (
      SELECT * FROM "tbl_ptrs_payment_normalisation_allocation"
    ),
    payment_normalisation_exceptions AS (
      SELECT * FROM "tbl_ptrs_payment_normalisation_exception"
    )`,
  ),
  requireCurrentPaymentNormalisationResult: jest.fn(async () => ({
    result: { id: "norm-1" },
  })),
  VEOLIA_PAYMENT_TIME_REFERENCE_POLICY: {
    canonicalConcept: "invoiceReceiptDate",
  },
}));

const db = require("@/db/database");
const {
  PAYMENT_OBSERVATION_WORK_MEM,
  buildPaymentObservationSummaryCte,
  buildPaymentObservationsCte,
  getPaymentObservationSummary,
  listPaymentObservationLinks,
  listPaymentObservations,
} = require("./payment-observations.ptrs.service");

describe("PTRS derived payment observations", () => {
  beforeEach(() => db.sequelize.query.mockReset());

  test("derives accounting observations from progressive payment allocations", () => {
    const sql = buildPaymentObservationsCte();
    expect(sql).toContain("payment_normalisation_payment_allocations");
    expect(sql).toContain('"tbl_ptrs_payment_normalisation_allocation"');
    expect(sql).not.toContain(
      "payment_normalisation_payment_allocations_raw AS",
    );
    expect(sql).toContain(
      "'payment-observation:' || invoice.\"id\" || ':' || payment.\"id\"",
    );
    expect(sql).toContain('allocation.allocated_amount AS "paymentAmount"');
    expect(sql).toContain(
      'allocation.settlement_payment_date::text AS "paymentDate"',
    );
    expect(sql).toContain("'{partial_payment}'");
    expect(sql).toContain("'PARTIAL_PAYMENT'");
    expect(sql).not.toContain("s.*");
    expect(sql).toContain('JOIN "tbl_ptrs_stage_row" invoice');
    expect(sql).toContain('JOIN "tbl_ptrs_stage_row" payment');
    expect(sql).toContain(
      "allocation.normalisation_result_id = :normalisationResultId",
    );
    expect(sql).toContain("allocation.normalisation_group_key");
    expect(sql).toContain("payment.\"documentType\"");
    expect(sql).toContain(
      "'outstanding_obligation_after_clearing_settlement'",
    );
    expect(sql).not.toContain(
      'JOIN "tbl_ptrs_payment_normalisation_row" invoice_normalisation',
    );
    expect(sql).not.toContain(
      'JOIN payment_normalisation_source_rows invoice_source',
    );
    expect(sql).not.toContain(
      'JOIN payment_normalisation_source_rows payment_source',
    );
  });

  test("projects persisted Payment Time provenance without recalculating it", () => {
    const sql = buildPaymentObservationsCte();
    expect(sql).toContain("allocation.payment_time_days");
    expect(sql).toContain("allocation.payment_time_reference_kind");
    expect(sql).toContain("allocation.payment_time_reference_date");
    expect(sql).toContain("allocation.payment_time_reference_policy");
    expect(sql).toContain("allocation.payment_time_reference_reason");
    expect(sql).toContain("'canonicalConcept'");
    expect(sql).toContain("'canonicalSource'");
    expect(sql).toContain("'canonicalSources'->'invoice_receipt_date'");
    expect(sql).not.toContain('invoice."invoiceDueDate"');
  });

  test("retains canonical, raw-row, adapter and worksheet dataset provenance", () => {
    const sql = buildPaymentObservationsCte();
    for (const field of [
      "canonicalRevisionId",
      "canonicalSourceRowId",
      "datasetId",
      "sourceRawRowId",
      "sourceRowNo",
      "adapterType",
      "adapterVersion",
      "semanticKind",
      "sourceGroupScope",
    ]) {
      expect(sql).toContain(`'${field}'`);
    }
  });

  test("keeps partial allocations in settlement value without double counting a ZP", () => {
    const sql = buildPaymentObservationsCte();
    const settlementCte = sql.slice(
      sql.indexOf("payment_observation_accounting_settlement_groups AS"),
    );
    expect(settlementCte).toContain(
      "'payment:' || allocation.payment_stage_row_id",
    );
    expect(settlementCte).toContain(
      'SUM(allocation.allocated_amount)::numeric AS "settlementPaymentAmount"',
    );
    expect(settlementCte).not.toContain(
      "FROM payment_observation_accounting_observations",
    );
  });

  test("projects direct payments without fabricating accounting events", () => {
    const sql = buildPaymentObservationsCte();
    const direct = sql.slice(
      sql.indexOf("payment_observation_direct_observations AS"),
      sql.indexOf("payment_observations AS"),
    );
    expect(direct).toContain("'direct_payment'::text");
    expect(direct).toContain('ARRAY[direct."id"] AS "sourceStageRowIds"');
    expect(direct).toContain("'partialPayment', false");
    expect(sql).toContain(
      `persisted."normalisationRole" = 'DIRECT_PAYMENT'`,
    );
  });

  test("summarises partial payments and normalisation exceptions", async () => {
    const sql = buildPaymentObservationSummaryCte();
    expect(sql).toContain('AS "partialPayments"');
    expect(sql).toContain('AS "partialPaymentValue"');
    expect(sql).toContain('AS "tcpPaymentValue"');
    expect(sql).toContain('AS "paymentTimePopulationCount"');
    expect(sql).toContain('AS "normalisationExceptions"');

    const summary = {
      sourceStageRows: 20,
      derivedPaymentObservations: 4,
      partialPayments: 1,
      normalisationExceptions: 2,
    };
    db.sequelize.query
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([summary]);
    await expect(
      getPaymentObservationSummary({
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        transaction: { id: "tx-1" },
      }),
    ).resolves.toBe(summary);
  });

  test("executes observation and provenance reads as bounded tenant SQL", async () => {
    const transaction = { id: "tx-2" };
    db.sequelize.query
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([{ observationId: "observation-1" }])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([{ observationId: "observation-1" }]);

    await listPaymentObservations({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      transaction,
    });
    await listPaymentObservationLinks({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      transaction,
    });

    expect(db.sequelize.query.mock.calls[0]).toEqual([
      `SET LOCAL work_mem = '${PAYMENT_OBSERVATION_WORK_MEM}'`,
      { transaction },
    ]);
    expect(db.sequelize.query.mock.calls[1][1]).toMatchObject({
      transaction,
      replacements: {
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        normalisationResultId: "norm-1",
      },
      type: "SELECT",
    });
    expect(db.sequelize.query.mock.calls[3][0]).toContain(
      "FROM payment_observations",
    );
  });
});

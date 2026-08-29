jest.mock("@/db/database", () => ({
  sequelize: {
    QueryTypes: { SELECT: "SELECT" },
    query: jest.fn(),
  },
}));
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(),
}));
jest.mock("./payment-observations.ptrs.service", () => ({
  buildPaymentObservationsCte: jest.fn(
    () =>
      "payment_observation_source_rows AS (SELECT 1), payment_observation_accounting_keys AS (SELECT 1), payment_observation_direct_keys AS (SELECT 1), payment_observation_earlytrade_matches AS (SELECT 1)",
  ),
  getPaymentObservationReplacements: jest.fn(({ customerId, ptrsId }) => ({
    customerId,
    ptrsId,
    paymentObservationInvoiceType: "RE",
    paymentObservationEarlytradeType: "ET",
  })),
  setPaymentObservationWorkMem: jest.fn(),
}));

const db = require("@/db/database");
const {
  setPaymentObservationWorkMem,
} = require("./payment-observations.ptrs.service");
const {
  buildStageTransformationHistorySql,
  recordStageTransformationHistory,
} = require("./stage.history.ptrs.service");

describe("PTRS Stage transformation history persistence", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test("updates all history kinds set-wise and returns only aggregate counts", async () => {
    const transaction = { id: "tx-1" };
    const result = {
      rowsUpdated: 309280,
      paymentObservationLinks: 155280,
      earlytradeMatches: 100,
    };
    db.sequelize.query.mockResolvedValue([result]);

    await expect(
      recordStageTransformationHistory({
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        transaction,
      }),
    ).resolves.toBe(result);

    expect(setPaymentObservationWorkMem).toHaveBeenCalledWith({ transaction });
    expect(db.sequelize.query).toHaveBeenCalledTimes(1);
    const sql = db.sequelize.query.mock.calls[0][0];
    expect(sql).toContain("exclusion_events AS");
    expect(sql).toContain("payment_term_events AS");
    expect(sql).toContain("payment_time_events AS");
    expect(sql).toContain("direct_observation_events AS");
    expect(sql).toContain("anchor_events AS");
    expect(sql).toContain("earlytrade_events AS");
    expect(sql).toContain("stage_history_source AS MATERIALIZED");
    expect(sql).toContain("grouped AS NOT MATERIALIZED");
    expect(sql).toContain("prepared AS NOT MATERIALIZED");
    expect(sql).toContain("JOIN stage_history_source stage_row");
    expect(sql).toContain('UPDATE "tbl_ptrs_stage_row" stage_row');
    expect(sql).toContain('AS "rowsUpdated"');
    expect(sql).toContain("FROM payment_observation_earlytrade_matches match");
    expect(sql).not.toContain(
      "JOIN payment_observation_source_rows earlytrade",
    );
    expect(sql).not.toContain('SELECT stage_row."data"');
    expect(sql).not.toContain('RETURNING stage_row."meta"');
  });

  test("deduplicates existing history keys in PostgreSQL", () => {
    const sql = buildStageTransformationHistorySql();

    expect(sql).toContain("jsonb_array_elements");
    expect(sql).toContain("history_item->>'key' = candidate.event->>'key'");
    expect(sql).toContain("stage_row.\"meta\" IS DISTINCT FROM prepared.next_meta");
  });
});

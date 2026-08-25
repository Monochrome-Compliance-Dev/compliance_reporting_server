jest.mock("@/db/database", () => ({
  sequelize: { query: jest.fn() },
}));
jest.mock("./payment-observations.ptrs.service", () => ({
  buildPaymentObservationsCte: jest.fn(() => "payment_observations AS (SELECT 1)"),
  getPaymentObservationReplacements: jest.fn(({ customerId, ptrsId }) => ({
    customerId,
    ptrsId,
  })),
}));

const db = require("@/db/database");
const {
  calculatePaymentTermMetricsFromFrequencies,
  calculateSmallBusinessTradeCreditPaymentsPct,
  fetchPaymentObservationMetricsAggs,
} = require("./metrics.ptrs.service");

describe("PTRS payment-term metrics", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test("common term frequencies use only SBI-positive payment observations", async () => {
    db.sequelize.query.mockResolvedValue([
      [
        {
          sbTermFrequencies: [
            { term: 27, count: 168 },
            { term: 60, count: 112 },
          ],
          sbEntityTermFrequencies: [
            { payerEntityKey: "abn:11111111111", term: 27, count: 168 },
            { payerEntityKey: "abn:11111111111", term: 60, count: 112 },
          ],
        },
      ],
    ]);

    const result = await fetchPaymentObservationMetricsAggs({
      t: { id: "transaction" },
      customerId: "customer01",
      ptrsId: "ptrs000001",
    });
    const sql = db.sequelize.query.mock.calls[0][0];

    expect(sql).toMatch(/sb AS \([^]*WHERE is_small_business IS TRUE[^]*\)/);
    expect(sql).toMatch(/sb_term_frequencies AS \([^]*FROM sb[^]*\)/);
    expect(sql).toMatch(/sb_entity_term_frequencies AS \([^]*FROM sb[^]*\)/);
    expect(result.commonTermMode).toBe(27);
  });

  test("non-small-business observations cannot change the mode relation", async () => {
    db.sequelize.query.mockResolvedValue([
      [
        {
          sbTermFrequencies: [
            { term: 27, count: 3 },
            { term: 60, count: 2 },
          ],
          sbEntityTermFrequencies: [
            { payerEntityKey: "abn:11111111111", term: 27, count: 3 },
            { payerEntityKey: "abn:11111111111", term: 60, count: 2 },
          ],
        },
      ],
    ]);

    const result = await fetchPaymentObservationMetricsAggs({
      t: { id: "transaction" },
      customerId: "customer01",
      ptrsId: "ptrs000001",
    });

    expect(result.commonTermMode).toBe(27);
    const sql = db.sequelize.query.mock.calls[0][0];
    const commonFrequencyCte = sql.slice(
      sql.indexOf("sb_term_frequencies AS"),
      sql.indexOf("sb_entity_term_frequencies AS"),
    );
    expect(commonFrequencyCte).toContain("FROM sb");
    expect(commonFrequencyCte).not.toContain("FROM population");
  });

  test("range uses entity modes rather than transaction-level extrema", () => {
    const result = calculatePaymentTermMetricsFromFrequencies({
      sbTermFrequencies: [
        { term: 10, count: 1 },
        { term: 27, count: 3 },
        { term: 60, count: 4 },
        { term: 120, count: 1 },
      ],
      sbEntityTermFrequencies: [
        { payerEntityKey: "abn:11111111111", term: 10, count: 1 },
        { payerEntityKey: "abn:11111111111", term: 27, count: 3 },
        { payerEntityKey: "abn:22222222222", term: 60, count: 4 },
        { payerEntityKey: "abn:22222222222", term: 120, count: 1 },
      ],
    });

    expect(result).toEqual({
      commonTermMode: 60,
      termMin: 27,
      termMax: 60,
    });
  });

  test("single entity produces the same common mode and range values", () => {
    const overall = [
      { term: 27, count: 168 },
      { term: 60, count: 112 },
      { term: 30, count: 77 },
      { term: 10, count: 9 },
      { term: 1, count: 4 },
    ];
    const result = calculatePaymentTermMetricsFromFrequencies({
      sbTermFrequencies: overall,
      sbEntityTermFrequencies: overall.map((row) => ({
        ...row,
        payerEntityKey: "abn:11111111111",
      })),
    });

    expect(result).toEqual({
      commonTermMode: 27,
      termMin: 27,
      termMax: 27,
    });
  });

  test("multi-entity range is the minimum and maximum entity mode", () => {
    const result = calculatePaymentTermMetricsFromFrequencies({
      sbTermFrequencies: [
        { term: 15, count: 4 },
        { term: 30, count: 5 },
        { term: 45, count: 6 },
      ],
      sbEntityTermFrequencies: [
        { payerEntityKey: "abn:11111111111", term: 15, count: 4 },
        { payerEntityKey: "abn:11111111111", term: 30, count: 1 },
        { payerEntityKey: "abn:22222222222", term: 30, count: 4 },
        { payerEntityKey: "abn:22222222222", term: 45, count: 1 },
        { payerEntityKey: "name:entity three", term: 45, count: 5 },
      ],
    });

    expect(result.termMin).toBe(15);
    expect(result.termMax).toBe(45);
  });

  test("ties choose the lowest term and empty entities do not add zero", () => {
    expect(
      calculatePaymentTermMetricsFromFrequencies({
        sbTermFrequencies: [
          { term: 60, count: 2 },
          { term: 30, count: 2 },
        ],
        sbEntityTermFrequencies: [],
      }),
    ).toEqual({ commonTermMode: 30, termMin: null, termMax: null });
  });
});

describe("PTRS small-business trade-credit payment value", () => {
  test("uses the distinct ZP settlement aggregate as the denominator", async () => {
    db.sequelize.query.mockResolvedValue([
      [
        {
          totalValue: "6221100.81",
          tcpSettlementValue: "6201833.39",
          sbValue: "2980553.96",
          sbTermFrequencies: [],
          sbEntityTermFrequencies: [],
        },
      ],
    ]);

    const result = await fetchPaymentObservationMetricsAggs({
      t: { id: "transaction" },
      customerId: "customer01",
      ptrsId: "ptrs000001",
    });
    const sql = db.sequelize.query.mock.calls[0][0];

    expect(result).toMatchObject({
      totalValue: "6221100.81",
      tcpSettlementValue: "6201833.39",
      sbValue: "2980553.96",
    });
    expect(sql).toContain("FROM payment_observation_settlement_groups");
    expect(sql).toContain('SUM(ABS("settlementPaymentAmount"))');
  });

  test.each([
    ["one ZP / one RE", 100, 100, 100],
    ["one ZP / multiple RE", 150, 100, 150],
    ["RE aggregate greater than ZP", 120, 80, 150],
    ["multiple payment groups", 75, 300, 25],
    ["SBI status changes numerator only", 50, 200, 25],
  ])(
    "%s calculates the percentage from the unchanged SBTCP numerator and ZP total",
    (_caseName, sbValue, tcpSettlementValue, expected) => {
      expect(
        calculateSmallBusinessTradeCreditPaymentsPct({
          sbValue,
          tcpSettlementValue,
        }),
      ).toBe(expected);
    },
  );

  test("preserves the reconciled SBTCP numerator while changing only the denominator", () => {
    expect(
      calculateSmallBusinessTradeCreditPaymentsPct({
        sbValue: 2980553.96,
        tcpSettlementValue: 6201833.39,
      }),
    ).toBeCloseTo(48.06, 2);
  });
});

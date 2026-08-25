jest.mock("@/db/database", () => ({
  sequelize: {
    query: jest.fn(),
  },
}));

jest.mock("./ptrs.service", () => ({
  slog: {
    info: jest.fn(),
  },
}));

const db = require("@/db/database");
const { applyCrossRowRulesSql } = require("./rules.crossRow.sql");

describe("PTRS cross-row typed-column persistence", () => {
  test("updates canonical payment_amount and paymentAmount atomically", async () => {
    db.sequelize.query.mockResolvedValue([[], { rowCount: 1 }]);

    await applyCrossRowRulesSql({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      transaction: { id: "tx-1" },
      rules: [
        {
          id: "et-netting",
          when: [{ field: "document_type", op: "eq", value: "ET" }],
          target: {
            match: [
              {
                currentField: "veolia_et_discount",
                targetField: "veolia_et_discount",
              },
            ],
            where: [
              { field: "document_type", op: "neq", value: "ET" },
            ],
          },
          action: {
            op: "add",
            field: "payment_amount",
            valueFieldFromCurrent: "payment_amount",
            round: 2,
          },
        },
      ],
    });

    const sql = db.sequelize.query.mock.calls[0][0];
    expect(sql).toContain("'{payment_amount}'");
    expect(sql).toMatch(
      /"paymentAmount"\s*=\s*ROUND\(\(COALESCE\([^]*x\.delta\)::numeric, 2\)/,
    );
    expect(sql).toContain("to_jsonb(ROUND(");
  });
});

jest.mock("@/db/database", () => ({
  sequelize: { query: jest.fn() },
}));
jest.mock("./ptrs.service", () => ({ slog: { info: jest.fn() } }));

const db = require("@/db/database");
const { applyRowRulesSql } = require("./rules.row.sql");

describe("SQL row-rule action history", () => {
  test("tracks each action independently and emits structured history", async () => {
    db.sequelize.query.mockResolvedValue([[], { rowCount: 1 }]);

    await applyRowRulesSql({
      customerId: "customer01",
      ptrsId: "ptrs000001",
      transaction: { id: "transaction" },
      rules: [
        {
          id: "two-actions",
          when: [],
          then: [
            { op: "assign", field: "field_a", valueField: "source_a" },
            { op: "assign", field: "field_b", valueField: "source_b" },
          ],
        },
      ],
    });

    expect(db.sequelize.query).toHaveBeenCalledTimes(2);
    const firstSql = db.sequelize.query.mock.calls[0][0];
    const secondSql = db.sequelize.query.mock.calls[1][0];
    expect(firstSql).toContain("two-actions:action:0");
    expect(secondSql).toContain("two-actions:action:1");
    expect(firstSql).toContain("transformationHistory");
    expect(firstSql).toContain("beforeValue");
    expect(firstSql).toContain("afterValue");
  });
});

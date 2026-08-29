jest.mock("@/db/database", () => ({
  PtrsStageRow: { count: jest.fn() },
}));
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(),
}));
jest.mock("./ptrs.service", () => ({
  slog: { info: jest.fn() },
  safeMeta: jest.fn((value) => value),
}));
jest.mock("./canonical.ptrs.service", () => ({
  resolveCurrentCanonicalRevisions: jest.fn(),
  loadCanonicalRevisionRows: jest.fn(),
}));
jest.mock("./rules.row.sql", () => ({
  applyRowRulesSql: jest.fn(),
  buildRowRulesProjectionSql: jest.fn(),
}));
jest.mock("./rules.crossRow.sql", () => ({
  applyCrossRowRulesSql: jest.fn(),
}));

const db = require("@/db/database");
const { countCurrentStageRowsForRules } = require("./rules.ptrs.service");

describe("PTRS rules population count", () => {
  test("counts only the current non-deleted Stage population", async () => {
    const transaction = { id: "tx-1" };
    db.PtrsStageRow.count.mockResolvedValue(309280);

    await expect(
      countCurrentStageRowsForRules({
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        transaction,
      }),
    ).resolves.toBe(309280);

    expect(db.PtrsStageRow.count).toHaveBeenCalledWith({
      where: {
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        deletedAt: null,
      },
      transaction,
    });
  });
});

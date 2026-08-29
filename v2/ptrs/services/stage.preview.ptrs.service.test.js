const { getStagePreview } = require("./stage.preview.ptrs.service");

function makeTransaction() {
  return {
    finished: false,
    commit: jest.fn(async function commit() {
      this.finished = "commit";
    }),
    rollback: jest.fn(),
  };
}

describe("PTRS Stage preview database access", () => {
  test("serialises preview rows and count on the transaction client", async () => {
    const transaction = makeTransaction();
    const findAll = jest.fn(async () => [
      { rowNo: 1, data: { payment_amount: "10.00" } },
    ]);
    const count = jest.fn(async () => 309280);

    const result = await getStagePreview({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      limit: 1,
      beginTransactionWithCustomerContext: jest.fn(async () => transaction),
      createPtrsTrace: jest.fn(() => ({
        write: jest.fn(),
        close: jest.fn(),
      })),
      hrMsSince: jest.fn(() => 1),
      safeMeta: jest.fn((value) => value),
      slog: { info: jest.fn() },
      db: {
        PtrsStageRow: { findAll, count },
      },
    });

    expect(findAll.mock.invocationCallOrder[0]).toBeLessThan(
      count.mock.invocationCallOrder[0],
    );
    expect(result.totalRows).toBe(309280);
    expect(transaction.commit).toHaveBeenCalledTimes(1);
  });
});

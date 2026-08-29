jest.mock("@/db/database", () => ({
  getPgPool: jest.fn(),
  sequelize: {
    QueryTypes: { SELECT: "SELECT" },
    query: jest.fn(),
  },
}));
jest.mock("@/helpers/logger", () => ({
  logger: { warn: jest.fn(), error: jest.fn() },
}));

const db = require("@/db/database");
const {
  acquireProcessExecutionLock,
  buildProcessExecutionLockKey,
  tryAcquireProcessExecutionReconciliationLock,
} = require("./process-lock.ptrs.service");

describe("PTRS process execution advisory lock", () => {
  let client;

  beforeEach(() => {
    jest.clearAllMocks();
    client = { query: jest.fn(), release: jest.fn() };
    db.getPgPool.mockReturnValue({
      connect: jest.fn().mockResolvedValue(client),
    });
  });

  test("holds a session lock until the process releases it", async () => {
    client.query
      .mockResolvedValueOnce({ rows: [{ acquired: true }] })
      .mockResolvedValueOnce({ rows: [{ released: true }] });

    const lock = await acquireProcessExecutionLock({
      customerId: "customer01",
      ptrsId: "ptrs000001",
    });

    expect(lock.lockKey).toBe("ptrs:process:customer01:ptrs000001");
    expect(client.release).not.toHaveBeenCalled();
    await lock.release();
    expect(client.query).toHaveBeenLastCalledWith(
      expect.stringContaining("pg_advisory_unlock"),
      [lock.lockKey],
    );
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  test("returns no ownership when another backend holds the lock", async () => {
    client.query.mockResolvedValue({ rows: [{ acquired: false }] });

    await expect(
      acquireProcessExecutionLock({
        customerId: "customer01",
        ptrsId: "ptrs000001",
      }),
    ).resolves.toBeNull();
    expect(client.release).toHaveBeenCalledTimes(1);
  });

  test.each([true, false])(
    "reports reconciliation ownership as %s",
    async (acquired) => {
      db.sequelize.query.mockResolvedValue([{ acquired }]);
      const transaction = { id: "transaction01" };

      await expect(
        tryAcquireProcessExecutionReconciliationLock({
          customerId: "customer01",
          ptrsId: "ptrs000001",
          transaction,
        }),
      ).resolves.toBe(acquired);
      expect(db.sequelize.query).toHaveBeenCalledWith(
        expect.stringContaining("pg_try_advisory_xact_lock"),
        expect.objectContaining({
          replacements: {
            processExecutionLockKey: buildProcessExecutionLockKey({
              customerId: "customer01",
              ptrsId: "ptrs000001",
            }),
          },
          transaction,
        }),
      );
    },
  );
});

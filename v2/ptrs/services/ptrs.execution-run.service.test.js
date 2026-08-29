jest.mock("@/db/database", () => ({
  PtrsExecutionRun: {
    findOne: jest.fn(),
    update: jest.fn(),
  },
}));
jest.mock("@/v2/ptrs/services/data.ptrs.service", () => ({
  emitCsvUploadStatus: jest.fn(),
}));
jest.mock("@/helpers/logger", () => ({
  logger: {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  },
}));
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(),
}));
jest.mock("./process-lock.ptrs.service", () => ({
  tryAcquireProcessExecutionReconciliationLock: jest.fn(),
}));

const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  tryAcquireProcessExecutionReconciliationLock,
} = require("./process-lock.ptrs.service");
const {
  PROCESS_INTERRUPTED_ERROR,
  getLatestExecutionRun,
} = require("./ptrs.service");

function transaction() {
  return {
    finished: false,
    commit: jest.fn(function commit() {
      this.finished = "commit";
    }),
    rollback: jest.fn(function rollback() {
      this.finished = "rollback";
    }),
  };
}

describe("PTRS process execution-run reconciliation", () => {
  let tx;

  beforeEach(() => {
    jest.clearAllMocks();
    tx = transaction();
    beginTransactionWithCustomerContext.mockResolvedValue(tx);
    db.PtrsExecutionRun.update.mockResolvedValue([1]);
  });

  test("keeps a genuinely owned process run running", async () => {
    const running = {
      id: "process01",
      customerId: "customer01",
      ptrsId: "ptrs000001",
      step: "process",
      status: "running",
    };
    db.PtrsExecutionRun.findOne.mockResolvedValue(running);
    tryAcquireProcessExecutionReconciliationLock.mockResolvedValue(false);

    await expect(
      getLatestExecutionRun({
        customerId: "customer01",
        ptrsId: "ptrs000001",
        step: "process",
      }),
    ).resolves.toEqual(running);

    expect(db.PtrsExecutionRun.update).not.toHaveBeenCalled();
    expect(tx.commit).toHaveBeenCalledTimes(1);
  });

  test("marks an unowned running process as interrupted before returning it", async () => {
    db.PtrsExecutionRun.findOne.mockResolvedValue({
      id: "0rvaWck5fj",
      customerId: "customer01",
      ptrsId: "ptrs000001",
      step: "process",
      status: "running",
    });
    tryAcquireProcessExecutionReconciliationLock.mockResolvedValue(true);

    const result = await getLatestExecutionRun({
      customerId: "customer01",
      ptrsId: "ptrs000001",
      step: "process",
    });

    expect(result).toEqual(
      expect.objectContaining({
        id: "0rvaWck5fj",
        status: "failed",
        errorMessage: PROCESS_INTERRUPTED_ERROR,
        finishedAt: expect.any(Date),
      }),
    );
    expect(db.PtrsExecutionRun.update).toHaveBeenCalledWith(
      expect.objectContaining({
        status: "failed",
        errorMessage: PROCESS_INTERRUPTED_ERROR,
      }),
      expect.objectContaining({
        where: expect.objectContaining({
          id: "0rvaWck5fj",
          status: "running",
        }),
        transaction: tx,
      }),
    );
  });

  test.each([
    { step: "stage", status: "running" },
    { step: "process", status: "success" },
    { step: "process", status: "failed" },
  ])("does not reconcile $step/$status runs", async ({ step, status }) => {
    const row = { id: "run0000001", step, status };
    db.PtrsExecutionRun.findOne.mockResolvedValue(row);

    await expect(
      getLatestExecutionRun({
        customerId: "customer01",
        ptrsId: "ptrs000001",
        step,
      }),
    ).resolves.toEqual(row);

    expect(
      tryAcquireProcessExecutionReconciliationLock,
    ).not.toHaveBeenCalled();
    expect(db.PtrsExecutionRun.update).not.toHaveBeenCalled();
  });
});

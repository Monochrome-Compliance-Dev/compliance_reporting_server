const mockTransaction = {
  commit: jest.fn(),
  rollback: jest.fn(),
  finished: false,
};

const mockPtrsRow = {
  id: "ptrs-1",
  get: jest.fn(() => ({ id: "ptrs-1" })),
};

const mockDb = {
  Ptrs: {
    create: jest.fn(async () => mockPtrsRow),
  },
  PtrsReportingEntitySnapshot: {
    create: jest.fn(async (values) => values),
  },
};

jest.mock("@/db/database", () => mockDb);
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
  beginTransactionWithCustomerContext: jest.fn(async () => mockTransaction),
}));
jest.mock("./process-lock.ptrs.service", () => ({
  tryAcquireProcessExecutionReconciliationLock: jest.fn(),
}));

const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { createPtrs } = require("./ptrs.service");

describe("PTRS reporting-entity snapshot persistence", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockTransaction.finished = false;
  });

  test("creates the run and authoritative snapshot in one tenant transaction", async () => {
    await expect(
      createPtrs({
        customerId: "customer-1",
        profileId: "profile-1",
        reportingEntityName: "Example Entity Pty Ltd",
        reportingEntityAbn: "12 345 678 901",
        reportingEntityAcn: "123 456 789",
        createdBy: "user-1",
      }),
    ).resolves.toEqual({ id: "ptrs-1" });

    expect(beginTransactionWithCustomerContext).toHaveBeenCalledWith(
      "customer-1",
    );
    expect(mockDb.Ptrs.create).toHaveBeenCalledWith(
      expect.objectContaining({
        customerId: "customer-1",
        reportingEntityName: "Example Entity Pty Ltd",
      }),
      { transaction: mockTransaction },
    );
    expect(mockDb.PtrsReportingEntitySnapshot.create).toHaveBeenCalledWith(
      expect.objectContaining({
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        profileId: "profile-1",
        entityName: "Example Entity Pty Ltd",
        abn: "12345678901",
        acn: "123456789",
        source: "manual",
        createdBy: "user-1",
        updatedBy: "user-1",
      }),
      { transaction: mockTransaction },
    );
    expect(mockTransaction.commit).toHaveBeenCalledTimes(1);
    expect(mockTransaction.rollback).not.toHaveBeenCalled();
  });

  test("rejects an invalid ABN before opening a transaction", async () => {
    await expect(
      createPtrs({
        customerId: "customer-1",
        reportingEntityName: "Example Entity Pty Ltd",
        reportingEntityAbn: "1234",
      }),
    ).rejects.toMatchObject({
      message: "reportingEntityAbn must be an 11-digit ABN",
      statusCode: 400,
    });

    expect(beginTransactionWithCustomerContext).not.toHaveBeenCalled();
    expect(mockDb.Ptrs.create).not.toHaveBeenCalled();
    expect(mockDb.PtrsReportingEntitySnapshot.create).not.toHaveBeenCalled();
  });
});

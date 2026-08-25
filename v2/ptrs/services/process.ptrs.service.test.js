jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(),
}));
jest.mock("./exclusions.ptrs.service", () => ({
  applyExclusionsAndPersist: jest.fn(),
}));
jest.mock("./rules.ptrs.service", () => ({
  applyRulesAndPersist: jest.fn(),
}));
jest.mock("./sbi.ptrs.service", () => ({
  reapplyLatestResults: jest.fn(),
}));
jest.mock("./validate.ptrs.service", () => ({ getValidate: jest.fn() }));
jest.mock("./metrics.ptrs.service", () => ({ getMetrics: jest.fn() }));
jest.mock("./payment-observations.ptrs.service", () => ({
  getPaymentObservationSummary: jest.fn(),
}));
jest.mock("./stage.history.ptrs.service", () => ({
  recordStageTransformationHistory: jest.fn(),
}));

const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const exclusionsService = require("./exclusions.ptrs.service");
const rulesService = require("./rules.ptrs.service");
const sbiService = require("./sbi.ptrs.service");
const validateService = require("./validate.ptrs.service");
const metricsService = require("./metrics.ptrs.service");
const {
  getPaymentObservationSummary,
} = require("./payment-observations.ptrs.service");
const {
  recordStageTransformationHistory,
} = require("./stage.history.ptrs.service");
const { processPtrs } = require("./process.ptrs.service");

function transaction() {
  return {
    finished: false,
    commit: jest.fn(function commit() {
      this.finished = "commit";
    }),
    rollback: jest.fn(),
  };
}

describe("processPtrs", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    beginTransactionWithCustomerContext.mockImplementation(async () =>
      transaction(),
    );
    getPaymentObservationSummary
      .mockResolvedValueOnce({ sourceStageRows: 1786 })
      .mockResolvedValueOnce({
        sourceStageRows: 1786,
        excludedStageRows: 200,
        survivingStageRows: 1586,
        derivedPaymentObservations: 1063,
        sbiPositiveObservations: 370,
        earlytradeMatches: 13,
      });
    exclusionsService.applyExclusionsAndPersist.mockResolvedValue({
      persisted: 10,
    });
    rulesService.applyRulesAndPersist.mockResolvedValue({ persisted: 26 });
    sbiService.reapplyLatestResults.mockResolvedValue({ status: "APPLIED" });
    recordStageTransformationHistory.mockResolvedValue({ rowsUpdated: 100 });
    validateService.getValidate.mockResolvedValue({
      status: "PASS",
      counts: { blockers: 0, warnings: 0 },
    });
    metricsService.getMetrics.mockResolvedValue({ status: "READY" });
  });

  test("runs the complete post-Stage chain in the authoritative order", async () => {
    const result = await processPtrs({
      customerId: "customer01",
      ptrsId: "_2zMv6X3jb",
      profileId: "hp0UwS8j3J",
      userId: "user000001",
    });

    const calls = [
      exclusionsService.applyExclusionsAndPersist,
      rulesService.applyRulesAndPersist,
      sbiService.reapplyLatestResults,
      recordStageTransformationHistory,
      validateService.getValidate,
      metricsService.getMetrics,
    ].map((mock) => mock.mock.invocationCallOrder[0]);
    expect(calls).toEqual([...calls].sort((a, b) => a - b));
    expect(exclusionsService.applyExclusionsAndPersist).toHaveBeenCalledWith(
      expect.objectContaining({ category: "all" }),
    );
    expect(rulesService.applyRulesAndPersist).toHaveBeenCalledWith(
      expect.objectContaining({ limit: null }),
    );
    expect(result.counts).toEqual(
      expect.objectContaining({
        sourceStageRows: 1786,
        derivedPaymentObservations: 1063,
        sbiPositiveObservations: 370,
        earlytradeMatches: 13,
        blockers: 0,
        warnings: 0,
      }),
    );
  });

  test("rejects a run without Stage rows before mutating anything", async () => {
    getPaymentObservationSummary.mockReset().mockResolvedValue({
      sourceStageRows: 0,
    });

    await expect(
      processPtrs({ customerId: "customer01", ptrsId: "ptrs000001" }),
    ).rejects.toMatchObject({ statusCode: 409 });
    expect(exclusionsService.applyExclusionsAndPersist).not.toHaveBeenCalled();
    expect(rulesService.applyRulesAndPersist).not.toHaveBeenCalled();
    expect(sbiService.reapplyLatestResults).not.toHaveBeenCalled();
  });
});

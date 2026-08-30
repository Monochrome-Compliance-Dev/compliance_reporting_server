jest.mock("@/db/database", () => ({
  PtrsStageRow: {
    findOne: jest.fn(),
  },
  sequelize: {
    QueryTypes: { SELECT: "SELECT" },
    query: jest.fn(),
  },
}));
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(),
}));
jest.mock("@/helpers/logger", () => ({
  logger: { warn: jest.fn() },
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
jest.mock("./validate.ptrs.service", () => ({
  getProcessValidateSummary: jest.fn(),
}));
jest.mock("./metrics.ptrs.service", () => ({
  getMetricsWithExecution: jest.fn(),
}));
jest.mock("./payment-observations.ptrs.service", () => ({
  getPaymentObservationSummary: jest.fn(),
}));
jest.mock("./stage.history.ptrs.service", () => ({
  recordStageTransformationHistory: jest.fn(),
}));
jest.mock("./process-lock.ptrs.service", () => ({
  acquireProcessExecutionLock: jest.fn(),
}));
jest.mock("./ptrs.service", () => ({
  buildStableInputHash: jest.fn(() => "process-input-hash"),
  createExecutionRun: jest.fn(),
  getLatestExecutionRun: jest.fn(),
  updateExecutionRun: jest.fn(),
}));

const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { logger } = require("@/helpers/logger");
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
const { acquireProcessExecutionLock } = require("./process-lock.ptrs.service");
const { processPtrs } = require("./process.ptrs.service");
const {
  createExecutionRun,
  getLatestExecutionRun,
  updateExecutionRun,
} = require("./ptrs.service");

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
  const executionLock = { release: jest.fn() };

  beforeEach(() => {
    jest.clearAllMocks();
    beginTransactionWithCustomerContext.mockImplementation(async () =>
      transaction(),
    );
    getLatestExecutionRun.mockResolvedValueOnce(null).mockResolvedValueOnce({
      id: "stage-run-1",
      status: "success",
      inputHash: "stage-input-hash",
    });
    createExecutionRun.mockResolvedValue({ id: "process-run-1" });
    updateExecutionRun.mockResolvedValue({ id: "process-run-1" });
    executionLock.release.mockResolvedValue(undefined);
    acquireProcessExecutionLock.mockResolvedValue(executionLock);
    db.PtrsStageRow.findOne.mockResolvedValue({ id: "stage-row-1" });
    db.sequelize.query
      .mockResolvedValueOnce([{ tempFiles: "10", tempBytes: "100" }])
      .mockResolvedValueOnce([{ tempFiles: "11", tempBytes: "200" }])
      .mockResolvedValueOnce([{ tempFiles: "11", tempBytes: "200" }])
      .mockResolvedValueOnce([{ tempFiles: "12", tempBytes: "300" }])
      .mockResolvedValueOnce([{ tempFiles: "12", tempBytes: "300" }])
      .mockResolvedValueOnce([{ tempFiles: "14", tempBytes: "600" }])
      .mockResolvedValueOnce([{ tempFiles: "14", tempBytes: "600" }])
      .mockResolvedValueOnce([{ tempFiles: "17", tempBytes: "1000" }])
      .mockResolvedValueOnce([{ tempFiles: "17", tempBytes: "1000" }])
      .mockResolvedValueOnce([{ tempFiles: "21", tempBytes: "1500" }]);
    getPaymentObservationSummary.mockResolvedValue({
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
    sbiService.reapplyLatestResults.mockResolvedValue({
      status: "APPLIED",
      sbiUploadId: "existing-upload",
    });
    recordStageTransformationHistory.mockResolvedValue({ rowsUpdated: 100 });
    validateService.getProcessValidateSummary.mockResolvedValue({
      status: "PASS",
      counts: { blockers: 0, warnings: 0 },
    });
    metricsService.getMetricsWithExecution.mockResolvedValue({
      preview: { status: "READY" },
      execution: {
        source: "calculated",
        inputSignature: "metrics-signature",
        calculationVersion: "metrics-v1",
        metricsResultId: "metrics001",
      },
    });
  });

  test("passes a non-empty scoped Stage gate and runs the authoritative post-Stage chain", async () => {
    const result = await processPtrs({
      customerId: "customer01",
      ptrsId: "_2zMv6X3jb",
      profileId: "hp0UwS8j3J",
      userId: "user000001",
    });

    const calls = [
      exclusionsService.applyExclusionsAndPersist,
      rulesService.applyRulesAndPersist,
      recordStageTransformationHistory,
      validateService.getProcessValidateSummary,
      metricsService.getMetricsWithExecution,
    ].map((mock) => mock.mock.invocationCallOrder[0]);
    expect(calls).toEqual([...calls].sort((a, b) => a - b));
    expect(exclusionsService.applyExclusionsAndPersist).toHaveBeenCalledWith(
      expect.objectContaining({ category: "all" }),
    );
    expect(rulesService.applyRulesAndPersist).toHaveBeenCalledWith(
      expect.objectContaining({ limit: null }),
    );
    expect(db.PtrsStageRow.findOne).toHaveBeenCalledWith({
      attributes: ["id"],
      where: {
        customerId: "customer01",
        ptrsId: "_2zMv6X3jb",
        deletedAt: null,
      },
      raw: true,
      transaction: expect.any(Object),
    });
    expect(beginTransactionWithCustomerContext).toHaveBeenNthCalledWith(
      1,
      "customer01",
    );
    expect(beginTransactionWithCustomerContext).toHaveBeenNthCalledWith(
      2,
      "customer01",
    );
    expect(getPaymentObservationSummary).toHaveBeenCalledTimes(1);
    expect(getPaymentObservationSummary).toHaveBeenCalledWith({
      customerId: "customer01",
      ptrsId: "_2zMv6X3jb",
      transaction: expect.any(Object),
    });
    expect(db.PtrsStageRow.findOne.mock.invocationCallOrder[0]).toBeLessThan(
      exclusionsService.applyExclusionsAndPersist.mock.invocationCallOrder[0],
    );
    expect(
      getPaymentObservationSummary.mock.invocationCallOrder[0],
    ).toBeGreaterThan(
      recordStageTransformationHistory.mock.invocationCallOrder[0],
    );
    expect(sbiService.reapplyLatestResults).not.toHaveBeenCalled();
    expect(result.steps).not.toHaveProperty("sbi");
    expect(result.steps.metrics).toEqual({
      status: "READY",
      generated: true,
      resultSource: "calculated",
      inputSignature: "metrics-signature",
      calculationVersion: "metrics-v1",
      metricsResultId: "metrics001",
    });
    expect(result.steps.timings).toEqual({
      stageGateMs: expect.any(Number),
      exclusionsMs: expect.any(Number),
      rulesMs: expect.any(Number),
      transformationHistoryMs: expect.any(Number),
      paymentObservationsMs: expect.any(Number),
      validationMs: expect.any(Number),
      metricsMs: expect.any(Number),
    });
    expect(result.steps.databaseTempDeltas).toEqual({
      stageGate: { tempFilesDelta: 1, tempBytesDelta: 100 },
      transformationHistory: { tempFilesDelta: 1, tempBytesDelta: 100 },
      paymentObservations: { tempFilesDelta: 2, tempBytesDelta: 300 },
      validation: { tempFilesDelta: 3, tempBytesDelta: 400 },
      metrics: { tempFilesDelta: 4, tempBytesDelta: 500 },
    });
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
    expect(createExecutionRun).toHaveBeenCalledWith(
      expect.objectContaining({
        ptrsId: "_2zMv6X3jb",
        profileId: "hp0UwS8j3J",
        step: "process",
        status: "running",
        inputHash: "process-input-hash",
      }),
    );
    expect(updateExecutionRun).toHaveBeenLastCalledWith(
      expect.objectContaining({
        executionRunId: "process-run-1",
        status: "success",
        rowsIn: 1786,
        rowsOut: 1063,
        stats: expect.objectContaining({
          timings: result.steps.timings,
          databaseTempDeltas: result.steps.databaseTempDeltas,
        }),
      }),
    );
    expect(updateExecutionRun.mock.lastCall[0].stats).not.toHaveProperty("sbi");
    expect(executionLock.release).toHaveBeenCalledTimes(1);
  });

  test("does not reapply an existing imported SBI upload", async () => {
    const result = await processPtrs({
      customerId: "customer01",
      ptrsId: "ptrs000001",
      profileId: "profile01",
      userId: "user000001",
    });

    expect(sbiService.reapplyLatestResults).not.toHaveBeenCalled();
    expect(result.steps).not.toHaveProperty("sbi");
    expect(recordStageTransformationHistory).toHaveBeenCalledTimes(1);
    expect(validateService.getProcessValidateSummary).toHaveBeenCalledTimes(1);
    expect(metricsService.getMetricsWithExecution).toHaveBeenCalledTimes(1);
  });

  test("continues all phases when database temp counters are unavailable", async () => {
    db.sequelize.query.mockReset().mockRejectedValue(new Error("unavailable"));

    const result = await processPtrs({
      customerId: "customer01",
      ptrsId: "ptrs000001",
      profileId: "profile01",
    });

    expect(result.steps.databaseTempDeltas).toEqual({
      stageGate: {
        tempFilesDelta: null,
        tempBytesDelta: null,
      },
      transformationHistory: {
        tempFilesDelta: null,
        tempBytesDelta: null,
      },
      paymentObservations: {
        tempFilesDelta: null,
        tempBytesDelta: null,
      },
      validation: { tempFilesDelta: null, tempBytesDelta: null },
      metrics: { tempFilesDelta: null, tempBytesDelta: null },
    });
    expect(recordStageTransformationHistory).toHaveBeenCalledTimes(1);
    expect(getPaymentObservationSummary).toHaveBeenCalledTimes(1);
    expect(validateService.getProcessValidateSummary).toHaveBeenCalledTimes(1);
    expect(metricsService.getMetricsWithExecution).toHaveBeenCalledTimes(1);
    expect(db.sequelize.query).toHaveBeenCalledTimes(1);
    expect(logger.warn).toHaveBeenCalledTimes(1);
  });

  test("rejects a run without Stage rows before mutating anything", async () => {
    db.PtrsStageRow.findOne.mockResolvedValueOnce(null);

    await expect(
      processPtrs({
        customerId: "customer01",
        ptrsId: "ptrs000001",
        profileId: "profile01",
      }),
    ).rejects.toMatchObject({
      statusCode: 409,
      message: "Stage must be completed before PTRS transformations can run.",
    });
    expect(exclusionsService.applyExclusionsAndPersist).not.toHaveBeenCalled();
    expect(rulesService.applyRulesAndPersist).not.toHaveBeenCalled();
    expect(sbiService.reapplyLatestResults).not.toHaveBeenCalled();
    expect(getPaymentObservationSummary).not.toHaveBeenCalled();
    expect(updateExecutionRun).toHaveBeenLastCalledWith(
      expect.objectContaining({
        executionRunId: "process-run-1",
        status: "failed",
      }),
    );
  });

  test("marks a cancelled transformation query as failed and releases the run", async () => {
    db.PtrsStageRow.findOne.mockRejectedValueOnce(
      new Error("canceling statement due to user request"),
    );

    await expect(
      processPtrs({
        customerId: "customer01",
        ptrsId: "ptrs000001",
        profileId: "profile01",
      }),
    ).rejects.toThrow("canceling statement due to user request");

    expect(updateExecutionRun).toHaveBeenLastCalledWith(
      expect.objectContaining({
        executionRunId: "process-run-1",
        status: "failed",
        errorMessage: "canceling statement due to user request",
      }),
    );
  });

  test("closes an interrupted durable run before starting its replacement", async () => {
    getLatestExecutionRun
      .mockReset()
      .mockResolvedValueOnce({ id: "interrupted-run", status: "running" })
      .mockResolvedValueOnce({
        id: "stage-run-1",
        status: "success",
        inputHash: "stage-input-hash",
      });

    await processPtrs({
      customerId: "customer01",
      ptrsId: "ptrs000001",
      profileId: "profile01",
    });

    expect(updateExecutionRun).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        executionRunId: "interrupted-run",
        status: "failed",
        errorMessage:
          "Transformation process was interrupted before completion.",
      }),
    );
    expect(updateExecutionRun.mock.invocationCallOrder[0]).toBeLessThan(
      createExecutionRun.mock.invocationCallOrder[0],
    );
  });

  test("rejects a concurrent process owned by another backend instance", async () => {
    acquireProcessExecutionLock.mockResolvedValueOnce(null);

    await expect(
      processPtrs({
        customerId: "customer01",
        ptrsId: "ptrs000001",
        profileId: "profile01",
      }),
    ).rejects.toMatchObject({ statusCode: 409 });

    expect(getLatestExecutionRun).not.toHaveBeenCalled();
    expect(createExecutionRun).not.toHaveBeenCalled();
  });
});

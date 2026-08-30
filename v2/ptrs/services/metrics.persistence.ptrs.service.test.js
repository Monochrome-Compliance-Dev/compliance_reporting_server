const mockDb = {
  sequelize: {
    QueryTypes: { SELECT: "SELECT" },
    query: jest.fn(),
  },
  Ptrs: {
    findOne: jest.fn(),
    update: jest.fn(),
  },
  PtrsMetricsResult: {
    findOne: jest.fn(),
    create: jest.fn(),
  },
};

jest.mock("@/db/database", () => mockDb);
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(),
}));
jest.mock("./ptrs.service", () => ({
  buildStableInputHash: (value) =>
    require("crypto")
      .createHash("sha256")
      .update(JSON.stringify(value))
      .digest("hex"),
}));
jest.mock("./payment-observations.ptrs.service", () => ({
  buildPaymentObservationsCte: jest.fn(
    () => "payment_observations AS (SELECT 1)",
  ),
  getPaymentObservationReplacements: jest.fn(() => ({})),
  setPaymentObservationWorkMem: jest.fn(),
}));

const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  PTRS_METRICS_CALCULATION_VERSION,
  buildMetricsInputSignature,
  getMetricsWithExecution,
  updateMetricsDraft,
} = require("./metrics.ptrs.service");

function makeTransaction() {
  return {
    finished: false,
    lockRelease: null,
    async commit() {
      this.finished = "commit";
      this.lockRelease?.();
    },
    async rollback() {
      this.finished = "rollback";
      this.lockRelease?.();
    },
  };
}

function makeResultRow(values) {
  return {
    ...values,
    async update(patch) {
      Object.assign(this, patch);
      return this;
    },
  };
}

describe("PTRS persisted metrics results", () => {
  const customerId = "customer01";
  const ptrsId = "ptrs000001";
  const aggregateResult = {
    stageRowCount: 309280,
    paymentObservationCount: 145190,
    totalCount: 145190,
    totalValue: "1000",
    tcpSettlementValue: "1000",
    missingAmountCount: 0,
    sbCount: 100,
    sbValue: "400",
    missingSbFlagCount: 0,
    missingTermDaysCount: 0,
    missingDatesCount: 0,
    sbBand0to30Count: 50,
    sbBand31to60Count: 30,
    sbBandOver60Count: 20,
    sbWithinTermsKnownCount: 100,
    sbWithinTermsYesCount: 75,
    avgDays: "30",
    medianDays: "25",
    p80Days: "45",
    p95Days: "70",
    commonTermMode: 30,
    termMin: 14,
    termMax: 60,
  };

  let ptrs;
  let materialState;
  let results;
  let lockTails;

  beforeEach(() => {
    jest.clearAllMocks();
    ptrs = {
      id: ptrsId,
      customerId,
      reportingEntityName: "Example Entity",
      periodStart: "2026-01-01",
      periodEnd: "2026-06-30",
      meta: { abn: "12345678901" },
      reportPreviewDraft: {
        supplyChainFinanceOffered: false,
        procurementFeesCharged: false,
        smallBusinessPaymentObligations: true,
        reportComments: "Original comment",
      },
    };
    materialState = {
      stageExecution: {
        id: "stage00001",
        inputHash: "stage-hash-1",
        status: "success",
      },
      transformationExecution: {
        id: "process001",
        inputHash: "process-hash-1",
        status: "success",
      },
      sbiApplication: {
        id: "sbi0000001",
        fileHash: "sbi-hash-1",
        status: "APPLIED",
      },
      stageMaterialState: {
        revision: "7",
      },
    };
    results = [];
    lockTails = new Map();

    beginTransactionWithCustomerContext.mockImplementation(async () =>
      makeTransaction(),
    );
    mockDb.Ptrs.findOne.mockImplementation(async ({ where }) =>
      where.customerId === customerId && where.id === ptrsId ? ptrs : null,
    );
    mockDb.Ptrs.update.mockImplementation(async (patch, { where }) => {
      if (where.customerId === customerId && where.id === ptrsId) {
        ptrs.reportPreviewDraft = patch.reportPreviewDraft;
        return [1];
      }
      return [0];
    });
    mockDb.PtrsMetricsResult.findOne.mockImplementation(async ({ where }) =>
      results.find(
        (row) =>
          row.customerId === where.customerId &&
          row.ptrsId === where.ptrsId &&
          row.inputSignature === where.inputSignature &&
          (where.status == null || row.status === where.status),
      ),
    );
    mockDb.PtrsMetricsResult.create.mockImplementation(async (values) => {
      const row = makeResultRow({
        id: `result${results.length + 1}`,
        ...values,
      });
      results.push(row);
      return row;
    });
    mockDb.sequelize.query.mockImplementation(async (sql, options) => {
      if (sql.includes('AS "stageExecution"')) return [{ ...materialState }];
      if (sql.includes("pg_advisory_xact_lock")) {
        const key = options.replacements.metricsResultLockKey;
        const previous = lockTails.get(key) || Promise.resolve();
        let release;
        const current = new Promise((resolve) => {
          release = resolve;
        });
        lockTails.set(
          key,
          previous.then(() => current),
        );
        await previous;
        options.transaction.lockRelease = release;
        return [];
      }
      throw new Error(`Unexpected SQL: ${sql}`);
    });
  });

  test("first calculation persists raw aggregates and preserves the API shape", async () => {
    const fetchAggregates = jest.fn().mockResolvedValue(aggregateResult);

    const result = await getMetricsWithExecution({
      customerId,
      ptrsId,
      fetchAggregates,
    });

    expect(fetchAggregates).toHaveBeenCalledTimes(1);
    expect(results).toHaveLength(1);
    expect(results[0]).toEqual(
      expect.objectContaining({
        customerId,
        ptrsId,
        calculationVersion: PTRS_METRICS_CALCULATION_VERSION,
        status: "succeeded",
        aggregateResult,
        provenance: materialState,
      }),
    );
    expect(result.execution.source).toBe("calculated");
    expect(result.preview).toEqual(
      expect.objectContaining({
        header: expect.objectContaining({ reportId: ptrsId }),
        declarations: expect.objectContaining({
          reportComments: "Original comment",
        }),
        computed: expect.objectContaining({
          averagePaymentTimeDays: 30,
          percentageOfSbInvoicesPaidWithinPaymentTerm: 75,
          percentageOfSmallBusinessTradeCreditPayments: 40,
        }),
        quality: expect.objectContaining({
          basedOnRowCount: 145190,
          sbRowCount: 100,
        }),
      }),
    );
  });

  test("unchanged material state reuses the persisted result", async () => {
    const fetchAggregates = jest.fn().mockResolvedValue(aggregateResult);
    await getMetricsWithExecution({ customerId, ptrsId, fetchAggregates });
    const second = await getMetricsWithExecution({
      customerId,
      ptrsId,
      fetchAggregates,
    });

    expect(fetchAggregates).toHaveBeenCalledTimes(1);
    expect(results).toHaveLength(1);
    expect(second.execution.source).toBe("persisted");
    expect(second.preview.computed.averagePaymentTimeDays).toBe(30);
  });

  test("draft-only edits compose live declarations without recalculation", async () => {
    const fetchAggregates = jest.fn().mockResolvedValue(aggregateResult);
    await getMetricsWithExecution({ customerId, ptrsId, fetchAggregates });

    const preview = await updateMetricsDraft({
      customerId,
      ptrsId,
      userId: "user000001",
      patch: { reportComments: "Updated declaration comment" },
      fetchAggregates,
    });

    expect(fetchAggregates).toHaveBeenCalledTimes(1);
    expect(preview.declarations.reportComments).toBe(
      "Updated declaration comment",
    );
    expect(preview.computed.averagePaymentTimeDays).toBe(30);
  });

  test.each([
    [
      "Transformation",
      () => {
        materialState.transformationExecution = {
          id: "process002",
          inputHash: "process-hash-2",
          status: "success",
        };
        materialState.stageMaterialState.revision = "8";
      },
    ],
    [
      "SBI",
      () => {
        materialState.sbiApplication = {
          id: "sbi0000002",
          fileHash: "sbi-hash-2",
          status: "APPLIED",
        };
        materialState.stageMaterialState.revision = "9";
      },
    ],
  ])(
    "a material %s state change causes one new calculation",
    async (_, mutate) => {
      const fetchAggregates = jest.fn().mockResolvedValue(aggregateResult);
      await getMetricsWithExecution({ customerId, ptrsId, fetchAggregates });
      mutate();
      await getMetricsWithExecution({ customerId, ptrsId, fetchAggregates });
      await getMetricsWithExecution({ customerId, ptrsId, fetchAggregates });

      expect(fetchAggregates).toHaveBeenCalledTimes(2);
      expect(results).toHaveLength(2);
    },
  );

  test("a calculation version change invalidates the prior result", async () => {
    const fetchAggregates = jest.fn().mockResolvedValue(aggregateResult);
    await getMetricsWithExecution({ customerId, ptrsId, fetchAggregates });
    await getMetricsWithExecution({
      customerId,
      ptrsId,
      calculationVersion: "ptrs-payment-observation-metrics-v2",
      fetchAggregates,
    });

    expect(fetchAggregates).toHaveBeenCalledTimes(2);
    expect(results).toHaveLength(2);
    expect(results[0].inputSignature).not.toBe(results[1].inputSignature);
  });

  test("process status completion does not invalidate the material result", async () => {
    materialState.transformationExecution.status = "running";
    const fetchAggregates = jest.fn().mockResolvedValue(aggregateResult);
    await getMetricsWithExecution({ customerId, ptrsId, fetchAggregates });
    materialState.transformationExecution.status = "success";
    const completed = await getMetricsWithExecution({
      customerId,
      ptrsId,
      fetchAggregates,
    });

    expect(fetchAggregates).toHaveBeenCalledTimes(1);
    expect(completed.execution.source).toBe("persisted");
  });

  test("a direct persisted Stage edit invalidates through the material revision", async () => {
    const fetchAggregates = jest.fn().mockResolvedValue(aggregateResult);
    await getMetricsWithExecution({ customerId, ptrsId, fetchAggregates });
    materialState.stageMaterialState.revision = "8";
    await getMetricsWithExecution({ customerId, ptrsId, fetchAggregates });

    expect(fetchAggregates).toHaveBeenCalledTimes(2);
    expect(results).toHaveLength(2);
  });

  test("failed calculation records failure without a valid aggregate result", async () => {
    const fetchAggregates = jest.fn().mockRejectedValue(new Error("boom"));

    await expect(
      getMetricsWithExecution({ customerId, ptrsId, fetchAggregates }),
    ).rejects.toThrow("boom");

    expect(results).toHaveLength(1);
    expect(results[0]).toEqual(
      expect.objectContaining({
        customerId,
        ptrsId,
        status: "failed",
        aggregateResult: null,
        errorMessage: "boom",
      }),
    );
  });

  test("same-signature concurrent requests produce one unambiguous result", async () => {
    let releaseCalculation;
    const calculationGate = new Promise((resolve) => {
      releaseCalculation = resolve;
    });
    const fetchAggregates = jest
      .fn()
      .mockImplementationOnce(async () => {
        await calculationGate;
        return aggregateResult;
      })
      .mockResolvedValue(aggregateResult);

    const first = getMetricsWithExecution({
      customerId,
      ptrsId,
      fetchAggregates,
    });
    await new Promise((resolve) => setImmediate(resolve));
    const second = getMetricsWithExecution({
      customerId,
      ptrsId,
      fetchAggregates,
    });
    releaseCalculation();

    const outcomes = await Promise.all([first, second]);
    expect(fetchAggregates).toHaveBeenCalledTimes(1);
    expect(results).toHaveLength(1);
    expect(outcomes.map((result) => result.execution.source).sort()).toEqual([
      "calculated",
      "persisted",
    ]);
  });

  test("signature and result reads remain customer scoped", async () => {
    const fetchAggregates = jest.fn().mockResolvedValue(aggregateResult);
    await getMetricsWithExecution({ customerId, ptrsId, fetchAggregates });

    expect(mockDb.Ptrs.findOne).toHaveBeenCalledWith(
      expect.objectContaining({ where: { id: ptrsId, customerId } }),
    );
    expect(mockDb.PtrsMetricsResult.findOne).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({ customerId, ptrsId }),
      }),
    );
    expect(mockDb.sequelize.query).toHaveBeenCalledWith(
      expect.stringContaining('report."customerId" = :customerId'),
      expect.objectContaining({ replacements: { customerId, ptrsId } }),
    );
  });

  test("calculation version is an explicit signature component", () => {
    const v1 = buildMetricsInputSignature({
      materialState,
      calculationVersion: "v1",
    });
    const v2 = buildMetricsInputSignature({
      materialState,
      calculationVersion: "v2",
    });
    expect(v1).not.toBe(v2);
  });
});

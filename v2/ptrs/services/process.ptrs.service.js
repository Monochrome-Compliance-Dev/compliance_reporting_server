const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { logger } = require("@/helpers/logger");
const exclusionsService = require("./exclusions.ptrs.service");
const rulesService = require("./rules.ptrs.service");
const validateService = require("./validate.ptrs.service");
const metricsService = require("./metrics.ptrs.service");
const {
  PAYMENT_NORMALISATION_VERSION,
  persistPaymentNormalisationEvidence,
} = require("./payment-normalisation.ptrs.service");
const {
  getPaymentObservationSummary,
} = require("./payment-observations.ptrs.service");
const { acquireProcessExecutionLock } = require("./process-lock.ptrs.service");
const {
  finishExecutionTiming,
  measureExecutionPhase,
  startExecutionTiming,
} = require("./execution-timing.ptrs.service");
const {
  buildStableInputHash,
  createExecutionRun,
  getLatestExecutionRun,
  updateExecutionRun,
} = require("./ptrs.service");

const activeRuns = new Set();

async function readDatabaseTempCounters({ phase, boundary, diagnosticsState }) {
  if (!diagnosticsState.available) return null;
  try {
    const rows = await db.sequelize.query(
      `
        SELECT
          temp_files::text AS "tempFiles",
          temp_bytes::text AS "tempBytes"
        FROM pg_stat_database
        WHERE datname = current_database()
      `,
      { type: db.sequelize.QueryTypes.SELECT },
    );
    const tempFiles = Number(rows?.[0]?.tempFiles);
    const tempBytes = Number(rows?.[0]?.tempBytes);
    if (!Number.isSafeInteger(tempFiles) || !Number.isSafeInteger(tempBytes)) {
      throw new Error("pg_stat_database returned invalid temp counters");
    }
    return {
      capturedAt: new Date().toISOString(),
      tempFiles,
      tempBytes,
    };
  } catch (error) {
    diagnosticsState.available = false;
    if (!diagnosticsState.warningLogged) {
      diagnosticsState.warningLogged = true;
      logger.warn("PTRS process temp-counter diagnostics unavailable", {
        action: "PtrsV2ProcessTempCountersUnavailable",
        phase,
        boundary,
        error: error.message,
      });
    }
    return null;
  }
}

function tempCounterDelta(before, after) {
  if (!before || !after) {
    return {
      tempFilesDelta: null,
      tempBytesDelta: null,
      before: before || null,
      after: after || null,
    };
  }
  return {
    tempFilesDelta: after.tempFiles - before.tempFiles,
    tempBytesDelta: after.tempBytes - before.tempBytes,
    before,
    after,
  };
}

async function measureProcessPhase({
  name,
  run,
  timings,
  phaseTimings,
  databaseTempDeltas = null,
  tempCounterDiagnostics = null,
}) {
  const before = databaseTempDeltas
    ? await readDatabaseTempCounters({
        phase: name,
        boundary: "before",
        diagnosticsState: tempCounterDiagnostics,
      })
    : null;
  const timing = startExecutionTiming();
  try {
    return await run();
  } finally {
    const completedTiming = finishExecutionTiming(timing);
    timings[`${name}Ms`] = completedTiming.elapsedMs;
    phaseTimings[name] = completedTiming;
    if (databaseTempDeltas) {
      const after = before
        ? await readDatabaseTempCounters({
            phase: name,
            boundary: "after",
            diagnosticsState: tempCounterDiagnostics,
          })
        : null;
      databaseTempDeltas[name] = tempCounterDelta(before, after);
    }
  }
}

async function readObservationSummary({
  customerId,
  ptrsId,
  normalisationResultId,
}) {
  const timings = {};
  const measure = (name, run) => measureExecutionPhase({ timings, name, run });
  let transaction;
  try {
    transaction = await measure("transactionAcquire", () =>
      beginTransactionWithCustomerContext(customerId),
    );
    const summary = await measure("summaryQuery", () =>
      getPaymentObservationSummary({
        customerId,
        ptrsId,
        normalisationResultId,
        transaction,
      }),
    );
    await measure("commit", () => transaction.commit());
    return { summary, timings };
  } catch (error) {
    if (transaction && !transaction.finished) await transaction.rollback();
    throw error;
  }
}

async function hasActiveStageRows({ customerId, ptrsId }) {
  const transaction = await beginTransactionWithCustomerContext(customerId);
  try {
    const stageRow = await db.PtrsStageRow.findOne({
      attributes: ["id"],
      where: { customerId, ptrsId, deletedAt: null },
      raw: true,
      transaction,
    });
    await transaction.commit();
    return Boolean(stageRow);
  } catch (error) {
    if (!transaction.finished) await transaction.rollback();
    throw error;
  }
}

async function processPtrs({
  customerId,
  ptrsId,
  profileId = null,
  userId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!profileId) throw new Error("profileId is required");

  const runKey = `${customerId}:${ptrsId}`;
  if (activeRuns.has(runKey)) {
    const error = new Error(
      "PTRS transformations are already running for this report.",
    );
    error.statusCode = 409;
    throw error;
  }
  activeRuns.add(runKey);

  const startedAt = Date.now();
  const timings = {};
  const phaseTimings = {};
  const databaseTempDeltas = {};
  const tempCounterDiagnostics = { available: true, warningLogged: false };
  let executionRun = null;
  let executionLock = null;
  try {
    executionLock = await acquireProcessExecutionLock({ customerId, ptrsId });
    if (!executionLock) {
      const error = new Error(
        "PTRS transformations are already running for this report.",
      );
      error.statusCode = 409;
      throw error;
    }

    const latestProcessRun = await getLatestExecutionRun({
      customerId,
      ptrsId,
      step: "process",
    });
    if (latestProcessRun?.status === "running") {
      await updateExecutionRun({
        customerId,
        executionRunId: latestProcessRun.id,
        status: "failed",
        finishedAt: new Date(),
        errorMessage:
          "Transformation process was interrupted before completion.",
        updatedBy: userId,
      });
    }

    const latestStageRun = await getLatestExecutionRun({
      customerId,
      ptrsId,
      step: "stage",
    });
    executionRun = await createExecutionRun({
      customerId,
      ptrsId,
      profileId,
      step: "process",
      inputHash: buildStableInputHash({
        ptrsId,
        profileId,
        stageExecutionRunId: latestStageRun?.id || null,
        stageInputHash: latestStageRun?.inputHash || null,
        paymentNormalisationVersion: PAYMENT_NORMALISATION_VERSION,
      }),
      status: "running",
      startedAt: new Date(startedAt),
      createdBy: userId,
    });

    const stageHasRows = await measureProcessPhase({
      name: "stageGate",
      timings,
      phaseTimings,
      databaseTempDeltas,
      tempCounterDiagnostics,
      run: () => hasActiveStageRows({ customerId, ptrsId }),
    });
    if (!stageHasRows) {
      const error = new Error(
        "Stage must be completed before PTRS transformations can run.",
      );
      error.statusCode = 409;
      throw error;
    }

    const rules = await measureProcessPhase({
      name: "rules",
      timings,
      phaseTimings,
      run: () =>
        rulesService.applyRulesAndPersist({
          customerId,
          ptrsId,
          profileId,
          limit: null,
          includeCrossRowRules: false,
        }),
    });
    const normalisation = await measureProcessPhase({
      name: "paymentNormalisation",
      timings,
      phaseTimings,
      databaseTempDeltas,
      tempCounterDiagnostics,
      run: () =>
        persistPaymentNormalisationEvidence({
          customerId,
          ptrsId,
          profileId,
          userId,
        }),
    });
    const exclusions = await measureProcessPhase({
      name: "exclusions",
      timings,
      phaseTimings,
      run: () =>
        exclusionsService.applyExclusionsAndPersist({
          customerId,
          ptrsId,
          profileId,
          category: "all",
        }),
    });
    const exclusionSummary = await measureProcessPhase({
      name: "exclusionSummary",
      timings,
      phaseTimings,
      run: () =>
        exclusionsService.getExclusionsSummary({
          customerId,
          ptrsId,
          profileId,
        }),
    });
    const paymentObservations = await measureProcessPhase({
      name: "paymentObservations",
      timings,
      phaseTimings,
      databaseTempDeltas,
      tempCounterDiagnostics,
      run: () =>
        readObservationSummary({
          customerId,
          ptrsId,
          normalisationResultId: normalisation.normalisationResultId,
        }),
    });
    const counts = paymentObservations.summary;
    const validation = await measureProcessPhase({
      name: "validation",
      timings,
      phaseTimings,
      databaseTempDeltas,
      tempCounterDiagnostics,
      run: () =>
        validateService.getProcessValidateSummary({
          customerId,
          ptrsId,
          normalisationResultId: normalisation.normalisationResultId,
        }),
    });
    const metricsResult = await measureProcessPhase({
      name: "metrics",
      timings,
      phaseTimings,
      databaseTempDeltas,
      tempCounterDiagnostics,
      run: () =>
        metricsService.getMetricsWithExecution({
          customerId,
          ptrsId,
          userId,
          normalisationResultId: normalisation.normalisationResultId,
        }),
    });
    const metrics = metricsResult.preview;
    const normalisationSummary = normalisation?.summary || {};
    const reconciliation = {
      startingStage: {
        count: normalisationSummary.startingStageCount ?? 0,
        absoluteValue: normalisationSummary.startingAbsoluteValue ?? 0,
      },
      exclusionsByReason: {
        counts: exclusionSummary?.byReason || {},
        values: exclusionSummary?.byReasonValue || {},
      },
      credits: {
        sourceValue: normalisationSummary.creditValue ?? 0,
        allocatedValue: normalisationSummary.creditAllocatedValue ?? 0,
      },
      refunds: {
        sourceValue: normalisationSummary.refundValue ?? 0,
        allocatedValue: normalisationSummary.refundAllocatedValue ?? 0,
      },
      earlyTradeDiscounts: {
        sourceValue: normalisationSummary.earlyTradeDiscountValue ?? 0,
        allocatedValue:
          normalisationSummary.earlyTradeDiscountAllocatedValue ?? 0,
      },
      netZeroAdjustmentReversals: {
        offsetValue: normalisationSummary.adjustmentReversalOffsetValue ?? 0,
      },
      unmatchedAdjustments: {
        value: normalisationSummary.unmatchedAdjustmentValue ?? 0,
        exceptionCount:
          normalisationSummary.unmatchedAdjustmentExceptionCount ?? 0,
        totalNormalisationExceptionCount:
          normalisationSummary.exceptionCount ?? 0,
      },
      normalisedObligations: {
        count: normalisationSummary.invoiceObligationCount ?? 0,
        originalValue: normalisationSummary.originalObligationValue ?? 0,
        adjustedValue: normalisationSummary.adjustedObligationValue ?? 0,
      },
      zpPayments: {
        eventCount: normalisationSummary.paymentEventCount ?? 0,
        eventValue: normalisationSummary.paymentEventValue ?? 0,
        allocationCount: normalisationSummary.paymentAllocationCount ?? 0,
      },
      tcp: {
        count: counts.derivedPaymentObservations ?? 0,
        paymentValue: counts.tcpPaymentValue ?? 0,
      },
      sbiClassifications: {
        positiveCount: counts.sbiPositiveObservations ?? 0,
        unclassifiedCount: counts.sbiUnclassifiedObservations ?? 0,
      },
      sbtcp: {
        count: counts.sbiPositiveObservations ?? 0,
        paymentValue: counts.sbiPositivePaymentValue ?? 0,
      },
      partialsRemovedFromPaymentTime: {
        count: counts.partialPayments ?? 0,
        paymentValue: counts.partialPaymentValue ?? 0,
      },
      paymentTimePopulation: {
        nonPartialCount: counts.sbiNonPartialObservations ?? 0,
        calculatedCount: counts.paymentTimePopulationCount ?? 0,
        calculatedValue: counts.paymentTimePopulationValue ?? 0,
      },
      finalReportMeasures: metrics?.computed || {},
    };

    const result = {
      ptrsId,
      profileId,
      executionRunId: executionRun.id,
      tookMs: Date.now() - startedAt,
      counts: {
        ...counts,
        blockers:
          Number(validation?.counts?.blockers) ||
          (Array.isArray(validation?.blockers)
            ? validation.blockers.length
            : 0),
        warnings:
          Number(validation?.counts?.warnings) ||
          (Array.isArray(validation?.warnings)
            ? validation.warnings.length
            : 0),
      },
      steps: {
        timings,
        phaseTimings,
        databaseTempDeltas,
        exclusions: {
          persisted: exclusions?.persisted ?? 0,
          stats: exclusions?.stats || null,
          summary: exclusionSummary,
        },
        rules: {
          persisted: rules?.persisted ?? 0,
          stats: rules?.stats || null,
        },
        paymentNormalisation: normalisation,
        reconciliation,
        paymentObservations: {
          derived: counts.derivedPaymentObservations,
          timings: paymentObservations.timings,
        },
        validation: {
          status: validation?.status || null,
          blockers:
            validation?.counts?.blockers ?? validation?.blockers?.length ?? 0,
          warnings:
            validation?.counts?.warnings ?? validation?.warnings?.length ?? 0,
          timings: validation?.timings || null,
        },
        metrics: {
          status: metrics?.status || "ready",
          generated: metricsResult.execution.source === "calculated",
          resultSource: metricsResult.execution.source,
          inputSignature: metricsResult.execution.inputSignature,
          calculationVersion: metricsResult.execution.calculationVersion,
          metricsResultId: metricsResult.execution.metricsResultId,
          timings: metricsResult.execution.timings || null,
        },
      },
    };
    await updateExecutionRun({
      customerId,
      executionRunId: executionRun.id,
      status: "success",
      finishedAt: new Date(),
      rowsIn: Number(counts.sourceStageRows) || 0,
      rowsOut: Number(counts.derivedPaymentObservations) || 0,
      stats: result.steps,
      errorMessage: null,
      updatedBy: userId,
    });
    return result;
  } catch (error) {
    if (executionRun?.id) {
      try {
        await updateExecutionRun({
          customerId,
          executionRunId: executionRun.id,
          status: "failed",
          finishedAt: new Date(),
          errorMessage: error?.message || "PTRS transformations failed",
          updatedBy: userId,
        });
      } catch (_) {
        // The original transformation error remains authoritative.
      }
    }
    throw error;
  } finally {
    if (executionLock) await executionLock.release();
    activeRuns.delete(runKey);
  }
}

module.exports = { processPtrs };

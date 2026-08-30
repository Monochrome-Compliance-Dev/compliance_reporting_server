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
  getPaymentObservationSummary,
} = require("./payment-observations.ptrs.service");
const {
  recordStageTransformationHistory,
} = require("./stage.history.ptrs.service");
const { acquireProcessExecutionLock } = require("./process-lock.ptrs.service");
const {
  buildStableInputHash,
  createExecutionRun,
  getLatestExecutionRun,
  updateExecutionRun,
} = require("./ptrs.service");

const activeRuns = new Set();

function elapsedMs(startedAt) {
  return Number(process.hrtime.bigint() - startedAt) / 1e6;
}

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
    return { tempFiles, tempBytes };
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
    return { tempFilesDelta: null, tempBytesDelta: null };
  }
  return {
    tempFilesDelta: after.tempFiles - before.tempFiles,
    tempBytesDelta: after.tempBytes - before.tempBytes,
  };
}

async function measureProcessPhase({
  name,
  run,
  timings,
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
  const startedAt = process.hrtime.bigint();
  try {
    return await run();
  } finally {
    timings[`${name}Ms`] = elapsedMs(startedAt);
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

async function readObservationSummary({ customerId, ptrsId }) {
  const transaction = await beginTransactionWithCustomerContext(customerId);
  try {
    const summary = await getPaymentObservationSummary({
      customerId,
      ptrsId,
      transaction,
    });
    await transaction.commit();
    return summary;
  } catch (error) {
    if (!transaction.finished) await transaction.rollback();
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
      }),
      status: "running",
      startedAt: new Date(startedAt),
      createdBy: userId,
    });

    const stageHasRows = await measureProcessPhase({
      name: "stageGate",
      timings,
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

    const exclusions = await measureProcessPhase({
      name: "exclusions",
      timings,
      run: () =>
        exclusionsService.applyExclusionsAndPersist({
          customerId,
          ptrsId,
          profileId,
          category: "all",
        }),
    });
    const rules = await measureProcessPhase({
      name: "rules",
      timings,
      run: () =>
        rulesService.applyRulesAndPersist({
          customerId,
          ptrsId,
          profileId,
          limit: null,
        }),
    });
    const history = await measureProcessPhase({
      name: "transformationHistory",
      timings,
      databaseTempDeltas,
      tempCounterDiagnostics,
      run: () => recordStageTransformationHistory({ customerId, ptrsId }),
    });
    const counts = await measureProcessPhase({
      name: "paymentObservations",
      timings,
      databaseTempDeltas,
      tempCounterDiagnostics,
      run: () => readObservationSummary({ customerId, ptrsId }),
    });
    const validation = await measureProcessPhase({
      name: "validation",
      timings,
      databaseTempDeltas,
      tempCounterDiagnostics,
      run: () =>
        validateService.getProcessValidateSummary({ customerId, ptrsId }),
    });
    const metricsResult = await measureProcessPhase({
      name: "metrics",
      timings,
      databaseTempDeltas,
      tempCounterDiagnostics,
      run: () =>
        metricsService.getMetricsWithExecution({ customerId, ptrsId, userId }),
    });
    const metrics = metricsResult.preview;

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
        databaseTempDeltas,
        exclusions: {
          persisted: exclusions?.persisted ?? 0,
          stats: exclusions?.stats || null,
        },
        rules: {
          persisted: rules?.persisted ?? 0,
          stats: rules?.stats || null,
        },
        transformationHistory: history,
        paymentObservations: { derived: counts.derivedPaymentObservations },
        validation: {
          status: validation?.status || null,
          blockers:
            validation?.counts?.blockers ?? validation?.blockers?.length ?? 0,
          warnings:
            validation?.counts?.warnings ?? validation?.warnings?.length ?? 0,
        },
        metrics: {
          status: metrics?.status || "ready",
          generated: metricsResult.execution.source === "calculated",
          resultSource: metricsResult.execution.source,
          inputSignature: metricsResult.execution.inputSignature,
          calculationVersion: metricsResult.execution.calculationVersion,
          metricsResultId: metricsResult.execution.metricsResultId,
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

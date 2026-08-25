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

const activeRuns = new Set();

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

async function processPtrs({
  customerId,
  ptrsId,
  profileId = null,
  userId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

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
  try {
    const initialCounts = await readObservationSummary({ customerId, ptrsId });
    if (!initialCounts.sourceStageRows) {
      const error = new Error(
        "Stage must be completed before PTRS transformations can run.",
      );
      error.statusCode = 409;
      throw error;
    }

    const exclusions = await exclusionsService.applyExclusionsAndPersist({
      customerId,
      ptrsId,
      profileId,
      category: "all",
    });
    const rules = await rulesService.applyRulesAndPersist({
      customerId,
      ptrsId,
      profileId,
      limit: null,
    });
    const sbi = await sbiService.reapplyLatestResults({
      customerId,
      ptrsId,
      userId,
    });
    const history = await recordStageTransformationHistory({
      customerId,
      ptrsId,
    });
    const counts = await readObservationSummary({ customerId, ptrsId });
    const validation = await validateService.getValidate({
      customerId,
      ptrsId,
      userId,
    });
    const metrics = await metricsService.getMetrics({
      customerId,
      ptrsId,
      userId,
    });

    return {
      ptrsId,
      profileId,
      tookMs: Date.now() - startedAt,
      counts: {
        ...counts,
        blockers:
          Number(validation?.counts?.blockers) ||
          (Array.isArray(validation?.blockers) ? validation.blockers.length : 0),
        warnings:
          Number(validation?.counts?.warnings) ||
          (Array.isArray(validation?.warnings) ? validation.warnings.length : 0),
      },
      steps: {
        exclusions: {
          persisted: exclusions?.persisted ?? 0,
          stats: exclusions?.stats || null,
        },
        rules: {
          persisted: rules?.persisted ?? 0,
          stats: rules?.stats || null,
        },
        sbi,
        transformationHistory: history,
        paymentObservations: { derived: counts.derivedPaymentObservations },
        validation: {
          status: validation?.status || null,
          blockers: validation?.counts?.blockers ?? validation?.blockers?.length ?? 0,
          warnings: validation?.counts?.warnings ?? validation?.warnings?.length ?? 0,
        },
        metrics: {
          status: metrics?.status || "ready",
          generated: true,
        },
      },
    };
  } finally {
    activeRuns.delete(runKey);
  }
}

module.exports = { processPtrs };

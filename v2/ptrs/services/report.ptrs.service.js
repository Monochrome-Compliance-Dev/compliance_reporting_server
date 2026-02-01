const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const metricsService = require("@/v2/ptrs/services/metrics.ptrs.service");

module.exports = {
  getReport,
};

/**
 * Composes a read-only report snapshot from PTRS entity + metrics preview.
 * No side effects. No persistence.
 */
async function getReport({ customerId, ptrsId, userId = null }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const ptrs = await db.Ptrs.findOne({
      where: { id: ptrsId, customerId },
      transaction: t,
    });

    if (!ptrs) {
      const e = new Error("Ptrs not found");
      e.statusCode = 404;
      throw e;
    }

    // Reuse the existing regulator-shaped metrics preview
    const metrics = await metricsService.getMetrics({
      customerId,
      ptrsId,
      userId,
    });

    await t.commit();

    return {
      ptrs: {
        id: ptrs.id,
        customerId: ptrs.customerId,
        label: ptrs.label,
        periodStart: ptrs.periodStart,
        periodEnd: ptrs.periodEnd,
        reportingEntityName: ptrs.reportingEntityName,
        profileId: ptrs.profileId,
        status: ptrs.status,
        currentStep: ptrs.currentStep,
        meta: ptrs.meta,
      },
      metrics,
    };
  } catch (err) {
    try {
      await t.rollback();
    } catch (_) {
      // ignore
    }
    throw err;
  }
}

const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const metricsService = require("@/v2/ptrs/services/metrics.ptrs.service");

module.exports = {
  getMetrics,
  updateMetricsDraft,
};

/**
 * GET /api/v2/ptrs/:id/metrics
 * Computes and returns the regulator-shaped report preview without side effects.
 */
async function getMetrics(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const result = await metricsService.getMetrics({
      customerId,
      ptrsId,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2GetMetrics",
      entity: "Ptrs",
      entityId: ptrsId,
      details: {
        basedOnRowCount: result?.quality?.basedOnRowCount || 0,
        sbRowCount: result?.quality?.sbRowCount || 0,
      },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS v2 metrics", {
      action: "PtrsV2GetMetrics",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

/**
 * PATCH /api/v2/ptrs/:id/metrics
 * Updates only the report preview draft declarations/comments.
 */
async function updateMetricsDraft(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const result = await metricsService.updateMetricsDraft({
      customerId,
      ptrsId,
      userId,
      patch: req.body,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2UpdateMetricsDraft",
      entity: "Ptrs",
      entityId: ptrsId,
      details: {
        updatedKeys: Object.keys(req.body || {}),
      },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error updating PTRS v2 metrics draft", {
      action: "PtrsV2UpdateMetricsDraft",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const stageService = require("@/v2/ptrs/services/stage.ptrs.service");
const ptrsService = require("@/v2/ptrs/services/ptrs.service");
const { safeLog } = require("./ptrs.controller");

module.exports = {
  stagePtrs,
  getStagePreview,
};

/**
 * POST /api/v2/ptrs/:id/stage
 * Body: { steps?: Array<...>, persist?: boolean, limit?: number }
 * When persist=true, writes staged rows and updates ptrs status.
 */
async function stagePtrs(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const {
    steps = [],
    persist = false,
    limit = 50,
    profileId = null,
  } = req.body || {};

  safeLog("[PTRS controller.stagePtrs] received", {
    customerId: req.effectiveCustomerId,
    ptrsId: req.params.id,
    body: req.body,
    profileId,
  });

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    safeLog("[PTRS controller.stagePtrs] invoking service", {
      steps,
      persist,
      limit,
      profileId,
    });

    // Confirm the PTRS run exists and belongs to this tenant
    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    const result = await stageService.stagePtrs({
      customerId,
      ptrsId,
      steps,
      persist: Boolean(persist),
      limit: Math.min(Number(limit) || 50, 500),
      userId,
      profileId,
    });

    safeLog("[PTRS controller.stagePtrs] result", result);

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: persist ? "PtrsV2StagePersist" : "PtrsV2StagePreview",
      entity: "PtrsStage",
      entityId: ptrsId,
      details: { stepCount: Array.isArray(steps) ? steps.length : 0, limit },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error staging PTRS v2 ptrs", {
      action: "PtrsV2StagePtrs",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

/**
 * GET or POST /api/v2/ptrs/:id/stage/preview
 * Returns: { sample: [], affectedCount: number }
 */
async function getStagePreview(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  safeLog("[PTRS controller.getStagePreview] received", {
    customerId: req.effectiveCustomerId,
    ptrsId: req.params.id,
    body: req.body,
    query: req.query,
  });

  const limit = Math.min(
    Number(req.body?.limit ?? req.query?.limit ?? 50) || 50,
    500
  );

  const profileId = req.body?.profileId ?? req.query?.profileId ?? null;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    // Confirm the PTRS run exists and belongs to this tenant
    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    safeLog("[PTRS controller.getStagePreview] invoking service", {
      limit,
      profileId,
    });

    const result = await stageService.getStagePreview({
      customerId,
      ptrsId,
      limit,
    });

    safeLog("[PTRS controller.getStagePreview] result", {
      headers: Array.isArray(result?.headers) ? result.headers.length : 0,
      rows: Array.isArray(result?.rows) ? result.rows.length : 0,
      headerSample:
        Array.isArray(result?.headers) && result.headers.length
          ? result.headers.slice(0, 10)
          : [],
      firstRowKeys:
        Array.isArray(result?.rows) && result.rows[0]
          ? Object.keys(result.rows[0])
          : [],
      firstRowSample:
        Array.isArray(result?.rows) && result.rows[0] ? result.rows[0] : null,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2StagePreview",
      entity: "PtrsStage",
      entityId: ptrsId,
      details: { limit },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error getting PTRS v2 stage preview", {
      action: "PtrsV2StagePreview",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

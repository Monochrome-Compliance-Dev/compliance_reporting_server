const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const validateService = require("@/v2/ptrs/services/validate.ptrs.service");

module.exports = {
  runValidate,
  getValidate,
  getValidateSummary,
};

/**
 * POST /api/v2/ptrs/:id/validate
 * Runs validation and returns a deterministic summary.
 */
async function runValidate(req, res, next) {
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

    const result = await validateService.validate({
      customerId,
      ptrsId,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2Validate",
      entity: "Ptrs",
      entityId: ptrsId,
      details: {
        validateStatus: result.status,
        blockers: result.counts?.blockers || 0,
        warnings: result.counts?.warnings || 0,
        totalRows: result.counts?.totalRows || 0,
      },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error validating PTRS v2", {
      action: "PtrsV2Validate",
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
 * GET /api/v2/ptrs/:id/validate
 * Computes and returns validation summary without side effects.
 */
async function getValidate(req, res, next) {
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

    const result = await validateService.getValidate({
      customerId,
      ptrsId,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2GetValidate",
      entity: "Ptrs",
      entityId: ptrsId,
      details: {
        validateStatus: result.status,
        blockers: result.counts?.blockers || 0,
        warnings: result.counts?.warnings || 0,
        totalRows: result.counts?.totalRows || 0,
      },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS v2 validate summary", {
      action: "PtrsV2GetValidate",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

/*
 * GET /api/v2/ptrs/:id/validate/summary
 * Returns an aggregated Validate summary payload to drive the Validate UI.
 */
async function getValidateSummary(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  const profileId = req.query?.profileId ?? req.body?.profileId ?? null;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const result = await validateService.getValidateSummary({
      customerId,
      ptrsId,
      profileId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ValidateSummary",
      entity: "PtrsValidate",
      entityId: ptrsId,
      details: {
        stageRowCount: result?.summary?.stage?.stageRowCount || 0,
        tradeCreditIncludedCount:
          result?.summary?.population?.tradeCreditIncludedCount || 0,
      },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error getting PTRS v2 validate summary", {
      action: "PtrsV2ValidateSummary",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

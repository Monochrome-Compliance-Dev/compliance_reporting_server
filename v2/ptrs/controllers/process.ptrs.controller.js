const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const ptrsService = require("@/v2/ptrs/services/ptrs.service");
const processService = require("@/v2/ptrs/services/process.ptrs.service");

async function processPtrs(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ptrsId = req.params.id;
  const profileId = req.body?.profileId || null;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    const result = await processService.processPtrs({
      customerId,
      ptrsId,
      profileId,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip: req.ip,
      device: req.headers["user-agent"],
      action: "PtrsV2TransformationsRun",
      entity: "PtrsStage",
      entityId: ptrsId,
      details: result.counts,
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error running PTRS v2 transformations", {
      action: "PtrsV2TransformationsRun",
      customerId,
      ptrsId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

module.exports = { processPtrs };

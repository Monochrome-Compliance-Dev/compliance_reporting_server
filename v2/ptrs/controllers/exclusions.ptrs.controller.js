const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const ptrsService = require("@/v2/ptrs/services/ptrs.service");
const exclusionsService = require("@/v2/ptrs/services/exclusions.ptrs.service");

module.exports = {
  exclusionsApply,
};

/**
 * POST /api/v2/ptrs/:id/exclusions/apply
 * Body: { profileId? }
 */
async function exclusionsApply(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
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

    const out = await exclusionsService.applyExclusionsAndPersist({
      customerId,
      ptrsId,
      profileId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ExclusionsApply",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: {
        persisted: out?.persisted ?? 0,
      },
    });

    return res.status(200).json({ status: "success", data: out });
  } catch (error) {
    logger.logEvent("error", "Error applying PTRS v2 exclusions", {
      action: "PtrsV2ExclusionsApply",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

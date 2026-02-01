const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const reportService = require("@/v2/ptrs/services/report.ptrs.service");

module.exports = {
  getReport,
};

/**
 * GET /api/v2/ptrs/:id/report
 * Returns a read-only PTRS report snapshot.
 */
async function getReport(req, res, next) {
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

    const result = await reportService.getReport({
      customerId,
      ptrsId,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2GetReport",
      entity: "Ptrs",
      entityId: ptrsId,
      details: {
        basedOnRowCount: result?.metrics?.quality?.basedOnRowCount || 0,
      },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS v2 report", {
      action: "PtrsV2GetReport",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

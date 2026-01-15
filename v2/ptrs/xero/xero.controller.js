const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const xeroService = require("./xero.service");

module.exports = {
  startImport,
  getStatus,
};

/**
 * POST /api/v2/ptrs/:id/xero/import
 * Body: { forceRefresh?: boolean }
 */
async function startImport(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const { forceRefresh } = req.body || {};

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    if (!ptrsId) {
      return res
        .status(400)
        .json({ status: "error", message: "ptrsId is required" });
    }

    const result = await xeroService.startImport({
      customerId,
      ptrsId,
      forceRefresh: Boolean(forceRefresh),
      userId: userId || null,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2XeroImportStart",
      entity: "PtrsXeroImport",
      entityId: ptrsId,
      details: {
        ptrsId,
        forceRefresh: Boolean(forceRefresh),
        status: result?.status || null,
      },
    });

    return res.status(201).json({ status: "success", data: result });
  } catch (error) {
    logger?.logEvent?.("error", "Error starting PTRS v2 Xero import", {
      action: "PtrsV2XeroImportStart",
      ptrsId,
      customerId,
      userId,
      error: error?.message,
      statusCode: error?.statusCode || 500,
    });
    return next(error);
  }
}

/**
 * GET /api/v2/ptrs/:id/xero/status
 */
async function getStatus(req, res, next) {
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
    if (!ptrsId) {
      return res
        .status(400)
        .json({ status: "error", message: "ptrsId is required" });
    }

    const status = await xeroService.getStatus({ customerId, ptrsId });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2XeroImportStatus",
      entity: "PtrsXeroImport",
      entityId: ptrsId,
      details: {
        ptrsId,
        status: status?.status || null,
      },
    });

    return res.status(200).json({ status: "success", data: status });
  } catch (error) {
    logger?.logEvent?.("error", "Error fetching PTRS v2 Xero import status", {
      action: "PtrsV2XeroImportStatus",
      ptrsId,
      customerId,
      userId,
      error: error?.message,
      statusCode: error?.statusCode || 500,
    });
    return next(error);
  }
}

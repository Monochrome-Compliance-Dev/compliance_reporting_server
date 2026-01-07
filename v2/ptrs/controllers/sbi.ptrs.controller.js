const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const sbiService = require("@/v2/ptrs/services/sbi.ptrs.service");

module.exports = {
  importSbiResults,
  getSbiStatus,
};

/**
 * POST /api/v2/ptrs/:id/sbi/import
 * Accepts SBI results CSV upload, stores evidence + results, applies classification to stage rows.
 */
async function importSbiResults(req, res, next) {
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

    if (!req.file || !req.file.buffer) {
      return res.status(400).json({
        status: "error",
        message: "SBI results file is required (multipart field name: file)",
      });
    }

    const result = await sbiService.importResults({
      customerId,
      ptrsId,
      userId,
      file: {
        originalname: req.file.originalname,
        mimetype: req.file.mimetype,
        buffer: req.file.buffer,
      },
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2SbiImport",
      entity: "Ptrs",
      entityId: ptrsId,
      details: {
        sbiUploadId: result.sbiUploadId,
        status: result.status,
        parsedAbns: result.counts?.parsedAbns || 0,
        affectedRows: result.counts?.affectedRows || 0,
        invalidAbns: result.counts?.invalidAbns || 0,
        unknownOutcomes: result.counts?.unknownOutcomes || 0,
      },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error importing SBI results (PTRS v2)", {
      action: "PtrsV2SbiImport",
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
 * GET /api/v2/ptrs/:id/sbi/status
 * Returns latest SBI upload summary for this ptrs run.
 */
async function getSbiStatus(req, res, next) {
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

    const result = await sbiService.getStatus({ customerId, ptrsId, userId });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2SbiStatus",
      entity: "Ptrs",
      entityId: ptrsId,
      details: {
        hasUpload: !!result?.latestUpload?.id,
        sbiUploadId: result?.latestUpload?.id || null,
        status: result?.latestUpload?.status || null,
      },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error fetching SBI status (PTRS v2)", {
      action: "PtrsV2SbiStatus",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

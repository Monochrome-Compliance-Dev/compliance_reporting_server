const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const sbiService = require("@/v2/ptrs/services/sbi.ptrs.service");

module.exports = {
  importSbiResults,
  getSbiStatus,
  exportSbiAbns,
  runSbiValidate,
  getSbiValidate,
};
/**
 * POST /api/v2/ptrs/:id/sbi/validate
 * Validates that the latest SBI upload was applied correctly to stage rows.
 */
async function runSbiValidate(req, res, next) {
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

    const result = await sbiService.validateAppliedSbi({
      customerId,
      ptrsId,
      userId,
      mode: "run",
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2SbiValidate",
      entity: "Ptrs",
      entityId: ptrsId,
      details: {
        validateStatus: result.status,
        blockers: result.counts?.blockers || 0,
        warnings: result.counts?.warnings || 0,
        totalRows: result.counts?.totalRows || 0,
        sbiUploadId: result?.sbi?.latestUploadId || null,
      },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error validating SBI application (PTRS v2)", {
      action: "PtrsV2SbiValidate",
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
 * GET /api/v2/ptrs/:id/sbi/validate
 * Computes and returns SBI validation summary without side effects.
 */
async function getSbiValidate(req, res, next) {
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

    const result = await sbiService.validateAppliedSbi({
      customerId,
      ptrsId,
      userId,
      mode: "read",
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2GetSbiValidate",
      entity: "Ptrs",
      entityId: ptrsId,
      details: {
        validateStatus: result.status,
        blockers: result.counts?.blockers || 0,
        warnings: result.counts?.warnings || 0,
        totalRows: result.counts?.totalRows || 0,
        sbiUploadId: result?.sbi?.latestUploadId || null,
      },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error fetching SBI validate summary (PTRS v2)", {
      action: "PtrsV2GetSbiValidate",
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

/**
 * GET /api/v2/ptrs/:id/sbi/export
 * Returns a CSV containing the unique payee ABNs for the current staged dataset.
 */
async function exportSbiAbns(req, res, next) {
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

    const csvText = await sbiService.exportAbnCsv({
      customerId,
      ptrsId,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2SbiExport",
      entity: "Ptrs",
      entityId: ptrsId,
      details: {
        bytes: Buffer.byteLength(csvText || "", "utf8"),
      },
    });

    const safeId = String(ptrsId || "ptrs").replace(/[^a-zA-Z0-9_-]/g, "");

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="sbi_export_${safeId}.csv"`
    );

    return res.status(200).send(csvText);
  } catch (error) {
    logger.logEvent("error", "Error exporting SBI ABNs (PTRS v2)", {
      action: "PtrsV2SbiExport",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

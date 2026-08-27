const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const ptrsService = require("@/v2/ptrs/services/data.ptrs.service");
const {
  cleanupUploadedFile,
} = require("@/v2/ptrs/middleware/csv-upload.ptrs.middleware");

module.exports = {
  addDataset,
  listDatasets,
  removeDataset,
  getDatasetSample,
};

/**
 * POST /api/v2/ptrs/:id/datasets
 * Multipart (file) + classification fields and optional sourceName.
 */
async function addDataset(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const purpose = (req.body?.purpose || req.query?.purpose || "").trim();
  const sourceFormat = (
    req.body?.sourceFormat ||
    req.query?.sourceFormat ||
    "csv"
  ).trim();
  const referenceKind = (
    req.body?.referenceKind ||
    req.query?.referenceKind ||
    ""
  ).trim();
  const adapterType = (
    req.body?.adapterType ||
    req.query?.adapterType ||
    ""
  ).trim();
  const adapterVersion = (
    req.body?.adapterVersion ||
    req.query?.adapterVersion ||
    ""
  ).trim();
  const sourceGroupScope = (
    req.body?.sourceGroupScope ||
    req.query?.sourceGroupScope ||
    ""
  ).trim();
  const sourceName = req.body?.sourceName || req.query?.sourceName || null;
  const file = req.file;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    if (!file || !file.path) {
      return res
        .status(400)
        .json({ status: "error", message: "File is required" });
    }
    if (!purpose) {
      return res
        .status(400)
        .json({ status: "error", message: "purpose is required" });
    }

    const created = await ptrsService.addDataset({
      customerId,
      ptrsId,
      purpose,
      sourceFormat,
      referenceKind: referenceKind || null,
      adapterType: adapterType || null,
      adapterVersion: adapterVersion || null,
      sourceGroupScope: sourceGroupScope || null,
      sourceName,
      fileName: file.originalname || null,
      fileSize: file.size || null,
      mimeType: file.mimetype || null,
      uploadPath: file.path,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2AddDataset",
      entity: "PtrsRawDataset",
      entityId: created.id,
      details: {
        purpose: created.purpose,
        referenceKind: created.referenceKind || null,
        sourceFormat: created.sourceFormat,
        fileName: created.fileName,
        rowsCount: created.meta?.rowsCount || 0,
      },
    });

    return res.status(201).json({ status: "success", data: created });
  } catch (error) {
    logger.logEvent("error", "Error adding PTRS v2 dataset", {
      action: "PtrsV2AddDataset",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  } finally {
    try {
      await cleanupUploadedFile(file);
    } catch (cleanupError) {
      logger.logEvent("warn", "Could not clean PTRS upload temporary file", {
        action: "PtrsV2AddDatasetTempCleanup",
        ptrsId,
        customerId,
        path: file?.path || null,
        error: cleanupError.message,
      });
    }
  }
}

/**
 * GET /api/v2/ptrs/:id/datasets
 */
async function listDatasets(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const ptrsId = req.params.id;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const items = await ptrsService.listDatasets({ customerId, ptrsId });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ListDatasets",
      entity: "PtrsRawDataset",
      entityId: ptrsId,
      details: { count: Array.isArray(items) ? items.length : 0 },
    });

    return res.status(200).json({ status: "success", data: { items } });
  } catch (error) {
    return next(error);
  }
}

/**
 * DELETE /api/v2/ptrs/:id/datasets/:datasetId
 */
async function removeDataset(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const datasetId = req.params.datasetId;
  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    const result = await ptrsService.removeDataset({
      customerId,
      ptrsId,
      datasetId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2RemoveDataset",
      entity: "PtrsRawDataset",
      entityId: datasetId,
      details: { ok: result.ok === true },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error removing PTRS v2 dataset", {
      action: "PtrsV2RemoveDataset",
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
 * GET /api/v2/ptrs/datasets/:datasetId/sample
 * Query: limit, offset
 */
async function getDatasetSample(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const datasetId = req.params.datasetId;
  const limit = Math.min(parseInt(req.query.limit || "10", 10), 200);
  const offset = Math.max(parseInt(req.query.offset || "0", 10), 0);

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const { headers, rows, total } = await ptrsService.getDatasetSample({
      customerId,
      datasetId,
      limit,
      offset,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2GetDatasetSample",
      entity: "PtrsRawDataset",
      entityId: datasetId,
      details: { returned: rows.length, total, limit, offset },
    });

    return res.status(200).json({
      status: "success",
      data: { headers, rows, total, limit, offset },
    });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS v2 dataset sample", {
      action: "PtrsV2GetDatasetSample",
      datasetId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

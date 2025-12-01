const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const ptrsService = require("@/v2/ptrs/services/data.ptrs.service");

module.exports = {
  addDataset,
  listDatasets,
  removeDataset,
  getDatasetSample,
};

/**
 * POST /api/v2/ptrs/:id/datasets
 * Multipart (file) + fields: role, sourceName (optional)
 */
async function addDataset(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const role = (req.body?.role || req.query?.role || "").trim();
  const sourceName = req.body?.sourceName || req.query?.sourceName || null;
  const file = req.file;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    if (!file || !file.buffer) {
      return res
        .status(400)
        .json({ status: "error", message: "File is required" });
    }
    if (!role) {
      return res
        .status(400)
        .json({ status: "error", message: "role is required" });
    }

    const created = await ptrsService.addDataset({
      customerId,
      ptrsId,
      role,
      sourceName,
      fileName: file.originalname || null,
      fileSize: file.size || null,
      mimeType: file.mimetype || null,
      buffer: file.buffer,
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
        role,
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

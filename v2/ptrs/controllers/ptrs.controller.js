const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const ptrsService = require("@/v2/ptrs/services/ptrs.service");

/**
 * POST /api/v2/ptrs/uploads  (alias: POST /api/v2/ptrs/runs)
 * Body: { fileName: string, fileSize?: number, mimeType?: string, hash?: string }
 */
async function createUpload(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const { fileName, fileSize, mimeType, hash } = req.body || {};
    if (!fileName || typeof fileName !== "string") {
      return res
        .status(400)
        .json({ status: "error", message: "fileName is required" });
    }

    const upload = await ptrsService.createUpload({
      customerId,
      fileName,
      fileSize,
      mimeType,
      hash,
      createdBy: userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2CreateUpload",
      entity: "PtrsUpload",
      entityId: upload.id,
      details: { fileName, fileSize, mimeType },
    });

    res.status(201).json({ status: "success", data: upload });
  } catch (error) {
    logger.logEvent("error", "Error creating PTRS v2 upload", {
      action: "PtrsV2CreateUpload",
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
 * POST /api/v2/ptrs/uploads/:id/import
 * Accepts:
 *  - text/csv body
 *  - multipart/form-data (file field named "file")
 */
async function importCsv(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const runId = req.params.id;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    // Confirm the upload exists and belongs to this tenant
    const upload = await ptrsService.getUpload({ runId, customerId });
    if (!upload) {
      return res
        .status(404)
        .json({ status: "error", message: "Upload not found" });
    }

    // Choose input source:
    const isTextCsv = (req.headers["content-type"] || "").includes("text/csv");
    const fileBuffer = req.file?.buffer;

    if (!isTextCsv && !fileBuffer) {
      return res.status(400).json({
        status: "error",
        message:
          "Provide CSV as text/csv body or multipart/form-data with 'file'",
      });
    }

    let rowsInserted = 0;
    const started = Date.now();

    if (isTextCsv) {
      // Stream directly from request
      rowsInserted = await ptrsService.importCsvStream({
        customerId,
        runId,
        stream: req, // readable
      });
    } else {
      // Parse the in-memory buffer (if using Multer)
      const { Readable } = require("stream");
      const stream = Readable.from(fileBuffer);
      rowsInserted = await ptrsService.importCsvStream({
        customerId,
        runId,
        stream,
      });
    }

    const durationMs = Date.now() - started;

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ImportCsv",
      entity: "PtrsUpload",
      entityId: runId,
      details: { rowsInserted, durationMs },
    });

    res
      .status(200)
      .json({ status: "success", data: { rowsInserted, durationMs } });
  } catch (error) {
    logger.logEvent("error", "Error importing PTRS v2 CSV", {
      action: "PtrsV2ImportCsv",
      runId,
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
 * GET /api/v2/ptrs/uploads/:id/sample?limit=10&offset=0
 * Returns a small window of staged rows + total count + inferred headers.
 */
async function getSample(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const runId = req.params.id;
  const limit = Math.min(parseInt(req.query.limit || "10", 10), 200);
  const offset = Math.max(parseInt(req.query.offset || "0", 10), 0);

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const upload = await ptrsService.getUpload({ runId, customerId });
    if (!upload) {
      return res
        .status(404)
        .json({ status: "error", message: "Upload not found" });
    }

    const { rows, total, headers } = await ptrsService.getImportSample({
      customerId,
      runId,
      limit,
      offset,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2GetSample",
      entity: "PtrsUpload",
      entityId: runId,
      details: { limit, offset, returned: rows.length, total },
    });

    res.status(200).json({
      status: "success",
      data: { rows, total, headers, limit, offset },
    });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS v2 sample", {
      action: "PtrsV2GetSample",
      runId,
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
 * GET /api/v2/ptrs/uploads/:id/map
 * Returns existing column map (if any) and inferred headers to assist UI mapping.
 */
async function getMap(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const runId = req.params.id;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const upload = await ptrsService.getUpload({ runId, customerId });
    if (!upload) {
      return res
        .status(404)
        .json({ status: "error", message: "Upload not found" });
    }

    const map = await ptrsService.getColumnMap({ customerId, runId });
    const { headers, total } = await ptrsService.getImportSample({
      customerId,
      runId,
      limit: 10,
      offset: 0,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2GetMap",
      entity: "PtrsUpload",
      entityId: runId,
      details: { hasMap: !!map, total },
    });

    res.status(200).json({
      status: "success",
      data: { map, headers },
    });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS v2 map", {
      action: "PtrsV2GetMap",
      runId,
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
 * POST /api/v2/ptrs/uploads/:id/map
 * Body: { mappings: { "<sourceHeader>": { field: "<logical>", type: "<type>", fmt?: "<format>" } } }
 */
async function saveMap(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const runId = req.params.id;
  const { mappings } = req.body || {};

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    if (!mappings || typeof mappings !== "object" || Array.isArray(mappings)) {
      return res
        .status(400)
        .json({ status: "error", message: "mappings object is required" });
    }

    const upload = await ptrsService.getUpload({ runId, customerId });
    if (!upload) {
      return res
        .status(404)
        .json({ status: "error", message: "Upload not found" });
    }

    // Validate source headers exist (best-effort using inferred headers)
    const { headers } = await ptrsService.getImportSample({
      customerId,
      runId,
      limit: 50,
      offset: 0,
    });
    const missing = Object.keys(mappings).filter(
      (src) => !headers.includes(src)
    );
    if (missing.length) {
      return res.status(400).json({
        status: "error",
        message: "One or more source headers were not found in the staged data",
        details: { missing },
      });
    }

    const saved = await ptrsService.saveColumnMap({
      customerId,
      runId,
      mappings,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2SaveMap",
      entity: "PtrsUpload",
      entityId: runId,
      details: { keys: Object.keys(mappings).length },
    });

    res.status(200).json({ status: "success", data: saved });
  } catch (error) {
    logger.logEvent("error", "Error saving PTRS v2 map", {
      action: "PtrsV2SaveMap",
      runId,
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
 * POST /api/v2/ptrs/uploads/:id/preview
 * Body: {
 *   steps?: Array<{
 *     kind: "filter" | "derive" | "rename",
 *     config: any
 *   }>,
 *   limit?: number
 * }
 * Returns: { sample: [], affectedCount: number }
 */
async function preview(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const runId = req.params.id;
  const { steps = [], limit = 50 } = req.body || {};

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    const upload = await ptrsService.getUpload({ runId, customerId });
    if (!upload) {
      return res
        .status(404)
        .json({ status: "error", message: "Upload not found" });
    }

    const result = await ptrsService.previewTransform({
      customerId,
      runId,
      steps,
      limit: Math.min(Number(limit) || 50, 500),
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2Preview",
      entity: "PtrsUpload",
      entityId: runId,
      details: { stepCount: Array.isArray(steps) ? steps.length : 0, limit },
    });

    res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error previewing PTRS v2 transform", {
      action: "PtrsV2Preview",
      runId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

module.exports = {
  createUpload,
  importCsv,
  getSample,
  getMap,
  saveMap,
  preview,
};

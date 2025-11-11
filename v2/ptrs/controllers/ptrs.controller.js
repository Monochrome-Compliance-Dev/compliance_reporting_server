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
const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const ptrsService = require("@/v2/ptrs/services/ptrs.service");
const fs = require("fs");
const path = require("path");
// --- Safe logging helpers (avoid circular/Set/BigInt issues) ---
function _safeReplacer() {
  const seen = new WeakSet();
  return function (key, value) {
    if (typeof value === "bigint") return value.toString();
    if (value instanceof Set) return Array.from(value);
    if (value instanceof Map) return Object.fromEntries(value);
    if (Buffer.isBuffer?.(value))
      return { __type: "Buffer", length: value.length };
    if (value instanceof Error) {
      return { name: value.name, message: value.message, stack: value.stack };
    }
    if (typeof value === "object" && value !== null) {
      if (seen.has(value)) return "[Circular]";
      seen.add(value);
    }
    return value;
  };
}
function safeJson(obj, { maxLen = 5000 } = {}) {
  try {
    const s = JSON.stringify(obj, _safeReplacer());
    if (s.length > maxLen) return s.slice(0, maxLen) + "...[truncated]";
    return s;
  } catch (e) {
    return `"[Unserializable: ${e.message}]"`;
  }
}
function safeLog(prefix, meta) {
  try {
    console.log(prefix, JSON.parse(safeJson(meta)));
  } catch {
    console.log(prefix, meta);
  }
}

/**
 * POST /api/v2/ptrs/runs
 * Body: { fileName: string, fileSize?: number, mimeType?: string, hash?: string }
 */
async function createRun(req, res, next) {
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

    const upload = await ptrsService.createRun({
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
 * POST /api/v2/ptrs/runs/:id/import
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
        .json({ status: "error", message: "Run not found" });
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
 * GET /api/v2/ptrs/runs/:id
 * Returns the run/upload metadata for the tenant
 */
async function getRun(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const runId = req.params.id;
  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    const run = await ptrsService.getRun({ customerId, runId });
    if (!run) {
      return res
        .status(404)
        .json({ status: "error", message: "Run not found" });
    }
    return res.status(200).json({ status: "success", data: run });
  } catch (error) {
    return next(error);
  }
}

/**
 * GET /api/v2/ptrs/runs/:id/sample?limit=10&offset=0
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
        .json({ status: "error", message: "Run not found" });
    }

    const { rows, total, headers, headerMeta } =
      await ptrsService.getImportSample({
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
      data: { rows, total, headers, headerMeta, limit, offset },
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
 * GET /api/v2/ptrs/runs/:id/unified-sample?limit=10&offset=0
 * Returns a small window of main rows + unified headers/examples merged from all datasets.
 */
async function getUnifiedSample(req, res, next) {
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
        .json({ status: "error", message: "Run not found" });
    }

    const { rows, total, headers, headerMeta } =
      await ptrsService.getUnifiedSample({
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
      action: "PtrsV2GetUnifiedSample",
      entity: "PtrsUpload",
      entityId: runId,
      details: { limit, offset, returned: rows.length, total },
    });

    return res.status(200).json({
      status: "success",
      data: { rows, total, headers, headerMeta, limit, offset },
    });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS v2 unified sample", {
      action: "PtrsV2GetUnifiedSample",
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
 * GET /api/v2/ptrs/runs/:id/map
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
        .json({ status: "error", message: "Run not found" });
    }

    const map = await ptrsService.getColumnMap({ customerId, runId });
    // Normalize JSON-typed fields that might be persisted as TEXT
    const maybeParse = (v) => {
      if (v == null || typeof v !== "string") return v;
      try {
        return JSON.parse(v);
      } catch {
        return v;
      }
    };
    if (map) {
      map.extras = maybeParse(map.extras);
      map.fallbacks = maybeParse(map.fallbacks);
      map.defaults = maybeParse(map.defaults);
      map.joins = maybeParse(map.joins);
      map.rowRules = maybeParse(map.rowRules);
    }
    const { headers, total, headerMeta } = await ptrsService.getImportSample({
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
      data: { map, headers, headerMeta },
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
 * POST /api/v2/ptrs/runs/:id/map
 * Body:
 * {
 *   mappings: { "<sourceHeader>": { field: "<logical>", type: "<type>", fmt?: "<format>", alias?: "<string>" } },
 *   extras?: { "<sourceHeader>": "<alias|null>" },
 *   fallbacks?: { "<canonicalField>": ["Alt A","Alt B","RUN_DEFAULT:..."] },
 *   defaults?: { "payerEntityName"?: "...", "payerEntityAbn"?: "..." },
 *   joins?: any,
 *   rowRules?: any[],
 *   profileId?: string
 * }
 */
async function saveMap(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const runId = req.params.id;
  const {
    mappings,
    extras = null,
    fallbacks = null,
    defaults = null,
    joins = null,
    rowRules = null,
    profileId = null,
  } = req.body || {};

  try {
    // Log incoming joins before validation
    console.log("[PTRS v2 saveMap] incoming joins:", joins);
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
        .json({ status: "error", message: "Run not found" });
    }

    // Best-effort validation: warn but don't block if headers slightly differ (case/space)
    const { headers } = await ptrsService.getImportSample({
      customerId,
      runId,
      limit: 50,
      offset: 0,
    });
    const norm = (s) =>
      String(s || "")
        .toLowerCase()
        .replace(/\s+/g, "");
    const headerSet = new Set((headers || []).map(norm));
    const missing = Object.keys(mappings).filter(
      (src) => !headerSet.has(norm(src))
    );
    if (missing.length) {
      // Include a hint but allow save (front-end will reconcile via tolerant matching)
      logger.info(
        "PTRS v2 saveMap: some mapping headers not found exactly in inferred headers",
        {
          action: "PtrsV2SaveMap",
          runId,
          customerId,
          missing,
        }
      );
    }

    const saved = await ptrsService.saveColumnMap({
      customerId,
      runId,
      mappings,
      extras,
      fallbacks,
      defaults,
      joins,
      rowRules,
      profileId,
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
 * POST /api/v2/ptrs/runs/:id/stage
 * Body: { steps?: Array<...>, persist?: boolean, limit?: number }
 * When persist=true, writes staged rows and updates run status.
 */
async function stageRun(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const runId = req.params.id;
  const {
    steps = [],
    persist = false,
    limit = 50,
    profileId = null,
  } = req.body || {};

  safeLog("[PTRS controller.stageRun] received", {
    customerId: req.effectiveCustomerId,
    runId: req.params.id,
    body: req.body,
    profileId,
  });

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    safeLog("[PTRS controller.stageRun] invoking service", {
      steps,
      persist,
      limit,
      profileId,
    });

    const upload = await ptrsService.getUpload({ runId, customerId });
    if (!upload) {
      return res
        .status(404)
        .json({ status: "error", message: "Run not found" });
    }

    const result = await ptrsService.stageRun({
      customerId,
      runId,
      steps,
      persist: Boolean(persist),
      limit: Math.min(Number(limit) || 50, 500),
      userId,
      profileId,
    });

    // safeLog("[PTRS controller.stageRun] result", result);

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: persist ? "PtrsV2StagePersist" : "PtrsV2StagePreview",
      entity: "PtrsStage",
      entityId: runId,
      details: { stepCount: Array.isArray(steps) ? steps.length : 0, limit },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error staging PTRS v2 run", {
      action: "PtrsV2StageRun",
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
 * POST /api/v2/ptrs/runs/:id/preview
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
        .json({ status: "error", message: "Run not found" });
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

/**
 * GET or POST /api/v2/ptrs/runs/:id/stage/preview
 * Accepts steps + limit in body (POST) or as query (GET with steps as JSON string).
 * Returns: { sample: [], affectedCount: number }
 */
async function getStagePreview(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const runId = req.params.id;

  safeLog("[PTRS controller.getStagePreview] received", {
    customerId: req.effectiveCustomerId,
    runId: req.params.id,
    body: req.body,
    query: req.query,
  });

  // Allow steps from body or query (query.steps can be a JSON string)
  let steps = req.body?.steps;
  if (!steps && req.query?.steps) {
    try {
      steps = JSON.parse(req.query.steps);
    } catch {
      steps = [];
    }
  }
  const limit = Math.min(
    Number(req.body?.limit ?? req.query?.limit ?? 50) || 50,
    500
  );

  const profileId = req.body?.profileId ?? req.query?.profileId ?? null;

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
        .json({ status: "error", message: "Run not found" });
    }

    safeLog("[PTRS controller.getStagePreview] invoking service", {
      steps,
      limit,
      profileId,
    });

    const result = await ptrsService.getStagePreview({
      customerId,
      runId,
      steps: Array.isArray(steps) ? steps : [],
      limit,
      profileId,
    });

    safeLog("[PTRS controller.getStagePreview] result", {
      headers: Array.isArray(result?.headers) ? result.headers.length : 0,
      rows: Array.isArray(result?.rows) ? result.rows.length : 0,
      headerSample:
        Array.isArray(result?.headers) && result.headers.length
          ? result.headers.slice(0, 10)
          : [],
      firstRowKeys:
        Array.isArray(result?.rows) && result.rows[0]
          ? Object.keys(result.rows[0])
          : [],
      firstRowSample:
        Array.isArray(result?.rows) && result.rows[0] ? result.rows[0] : null,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2StagePreview",
      entity: "PtrsStage",
      entityId: runId,
      details: { stepCount: Array.isArray(steps) ? steps.length : 0, limit },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error getting PTRS v2 stage preview", {
      action: "PtrsV2StagePreview",
      runId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

/**
 * GET /api/v2/ptrs/runs?hasMap=true
 * Returns a list of runs for the tenant (optionally only those with a saved column map)
 */
async function listRuns(req, res, next) {
  const customerId = req.effectiveCustomerId;
  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    const hasMap =
      String(req.query.hasMap || req.query.mapped || "")
        .toLowerCase()
        .startsWith("t") || req.query.hasMap === "1";

    const items = await ptrsService.listRuns({
      customerId,
      hasMap,
    });

    return res.status(200).json({ status: "success", data: { items } });
  } catch (error) {
    return next(error);
  }
}

/**
 * POST /api/v2/ptrs/runs/:id/datasets
 * Multipart (file) + fields: role, sourceName (optional)
 */
async function addDataset(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const runId = req.params.id;
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
      runId,
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
      runId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

/**
 * GET /api/v2/ptrs/runs/:id/datasets
 */
async function listDatasets(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const runId = req.params.id;
  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    const items = await ptrsService.listDatasets({ customerId, runId });
    return res.status(200).json({ status: "success", data: { items } });
  } catch (error) {
    return next(error);
  }
}

/**
 * DELETE /api/v2/ptrs/runs/:id/datasets/:datasetId
 */
async function removeDataset(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const runId = req.params.id;
  const datasetId = req.params.datasetId;
  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    const result = await ptrsService.removeDataset({
      customerId,
      runId,
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
      runId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

/**
 * GET /api/v2/ptrs/blueprint?profileId=veolia
 * Returns the generic blueprint optionally merged with a customer/profile overlay.
 */
async function getBlueprint(req, res, next) {
  try {
    const basePath = path.resolve(
      __dirname,
      "../config/ptrsCalculationBlueprint.json"
    );
    const rawBase = JSON.parse(fs.readFileSync(basePath, "utf8"));

    const profileId = (req.query.profileId || "").trim();
    let merged = rawBase;

    if (profileId) {
      const profilePath = path.resolve(
        __dirname,
        `../config/profiles/${profileId}.json`
      );
      if (fs.existsSync(profilePath)) {
        const rawProfile = JSON.parse(fs.readFileSync(profilePath, "utf8"));
        // Shallow-merge known hook areas
        merged = {
          ...rawBase,
          synonyms: {
            ...(rawBase.synonyms || {}),
            ...(rawProfile.synonyms || {}),
          },
          fallbacks: {
            ...(rawBase.fallbacks || {}),
            ...(rawProfile.fallbacks || {}),
          },
          rowRules: [
            ...(rawBase.rowRules || []),
            ...(rawProfile.rowRules || []),
          ],
          joins: { ...(rawBase.joins || {}), ...(rawProfile.joins || {}) },
        };
      }
    }

    return res.status(200).json({ status: "success", data: merged });
  } catch (error) {
    return next(error);
  }
}

// in v2/ptrs/controllers/ptrs.controller.js
async function listProfiles(req, res, next) {
  try {
    const customerId = req.query.customerId || req.effectiveCustomerId;
    const profiles = await ptrsService.listProfiles(customerId);
    res.status(200).json({ status: "success", data: { items: profiles } });
  } catch (error) {
    next(error);
  }
}

/**
 * POST /api/v2/ptrs/profiles
 * Body: { profileId?, name, description?, isDefault?, config? }
 */
async function createProfile(req, res, next) {
  const customerId = req.effectiveCustomerId || req.body?.customerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    const created = await ptrsService.createProfile({
      customerId,
      payload: req.body || {},
      userId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2CreateProfile",
      entity: "PtrsProfile",
      entityId: created.id,
      details: {
        name: created.name,
        profileId: created.profileId || created.id,
      },
    });
    return res.status(201).json({ status: "success", data: created });
  } catch (error) {
    return next(error);
  }
}

/**
 * GET /api/v2/ptrs/profiles/:id
 */
async function getProfile(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const profileId = req.params.id;
  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    const row = await ptrsService.getProfile({ customerId, profileId });
    if (!row) {
      return res
        .status(404)
        .json({ status: "error", message: "Profile not found" });
    }
    return res.status(200).json({ status: "success", data: row });
  } catch (error) {
    return next(error);
  }
}

/**
 * PATCH /api/v2/ptrs/profiles/:id
 */
async function updateProfile(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const profileId = req.params.id;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    const updated = await ptrsService.updateProfile({
      customerId,
      profileId,
      payload: req.body || {},
      userId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2UpdateProfile",
      entity: "PtrsProfile",
      entityId: profileId,
    });
    return res.status(200).json({ status: "success", data: updated });
  } catch (error) {
    return next(error);
  }
}

/**
 * DELETE /api/v2/ptrs/profiles/:id
 */
async function deleteProfile(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const profileId = req.params.id;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    const result = await ptrsService.deleteProfile({ customerId, profileId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2DeleteProfile",
      entity: "PtrsProfile",
      entityId: profileId,
      details: { ok: result.ok === true },
    });
    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    return next(error);
  }
}

module.exports = {
  createRun,
  importCsv,
  getRun,
  getSample,
  getUnifiedSample,
  getMap,
  saveMap,
  stageRun,
  preview,
  getStagePreview,
  listRuns,
  addDataset,
  listDatasets,
  getDatasetSample,
  removeDataset,
  getBlueprint,
  listProfiles,
  // Profiles CRUD
  createProfile,
  getProfile,
  updateProfile,
  deleteProfile,
};

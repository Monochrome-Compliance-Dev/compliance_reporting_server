const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const { safeLog } = require("@/v2/ptrs/controllers/ptrs.controller");
const { safeMeta, slog } = require("@/v2/ptrs/services/ptrs.service");
const ptrsService = require("@/v2/ptrs/services/ptrs.service");
const tmPtrsService = require("@/v2/ptrs/services/tablesAndMaps.ptrs.service");

module.exports = {
  getMap,
  saveMap,
  getSample,
  getUnifiedSample,
  buildMappedDataset,
};

// TODO: future: allow external transaction but only from beginTransactionWithCustomerContext

/**
 * GET /api/v2/ptrs/:id/map
 * Returns existing column map (if any) and inferred headers to assist UI mapping.
 */
async function getMap(req, res, next) {
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

    // Confirm the PTRS run exists and belongs to this tenant
    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    const map = await tmPtrsService.getColumnMap({ customerId, ptrsId });
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
      map.customFields = maybeParse(map.customFields);
    }
    const { headers, total, headerMeta } = await tmPtrsService.getImportSample({
      customerId,
      ptrsId,
      limit: 10,
      offset: 0,
    });

    // slog.info("getColumnMap", {
    //   id: map?.id,
    //   joinsType: typeof map?.joins,
    //   joinsRaw: map?.joins,
    // });

    // safeLog("[PTRS controller.getMap] map + header meta", {
    //   id: map?.id || null,
    //   hasMap: !!map,
    //   joinsType: typeof map?.joins,
    //   joinsRaw: map?.joins,
    //   mappingsKeys: map?.mappings ? Object.keys(map.mappings) : [],
    //   headersCount: Array.isArray(headers) ? headers.length : 0,
    //   headerSample: Array.isArray(headers) ? headers.slice(0, 10) : [],
    //   hasHeaderMeta: !!headerMeta,
    // });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2GetMap",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: { hasMap: !!map, total },
    });

    res.status(200).json({
      status: "success",
      data: { map, headers, headerMeta },
    });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS v2 map", {
      action: "PtrsV2GetMap",
      ptrsId,
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
 * POST /api/v2/ptrs/:id/map
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
  const ptrsId = req.params.id;
  const {
    mappings,
    extras = null,
    fallbacks = null,
    defaults = null,
    joins = null,
    rowRules = null,
    profileId = null,
    customFields = null,
  } = req.body || {};

  slog.info(
    "[PTRS v2 saveMap] body",
    safeMeta({
      ptrsId,
      mappingsKeys: Object.keys(req.body.mappings || {}),
      joins: req.body.joins,
    })
  );

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

    // Confirm the PTRS run exists and belongs to this tenant
    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    // Best-effort validation: warn but don't block if headers slightly differ (case/space)
    const { headers } = await tmPtrsService.getImportSample({
      customerId,
      ptrsId,
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
          ptrsId,
          customerId,
          missing,
        }
      );
    }

    const saved = await tmPtrsService.saveColumnMap({
      customerId,
      ptrsId,
      mappings,
      extras,
      fallbacks,
      defaults,
      joins,
      rowRules,
      profileId,
      customFields,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2SaveMap",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: { keys: Object.keys(mappings).length },
    });

    res.status(200).json({ status: "success", data: saved });
  } catch (error) {
    logger.logEvent("error", "Error saving PTRS v2 map", {
      action: "PtrsV2SaveMap",
      ptrsId,
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
 * POST /api/v2/ptrs/:id/map/build-mapped
 * Builds and persists the mapped + joined dataset into PtrsMappedRow for this PTRS run.
 */
async function buildMappedDataset(req, res, next) {
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

    // Confirm the PTRS run exists and belongs to this tenant
    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    slog.info(
      "[PTRS v2 buildMappedDataset] begin",
      safeMeta({ customerId, ptrsId, userId })
    );

    const result = await tmPtrsService.buildMappedDatasetForPtrs({
      customerId,
      ptrsId,
      actorId: userId || null,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2BuildMappedDataset",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: {
        rowsPersisted: result?.count || 0,
        headersCount: Array.isArray(result?.headers)
          ? result.headers.length
          : 0,
      },
    });

    slog.info(
      "[PTRS v2 buildMappedDataset] complete",
      safeMeta({
        customerId,
        ptrsId,
        rowsPersisted: result?.count || 0,
      })
    );

    return res.status(200).json({
      status: "success",
      data: {
        count: result?.count || 0,
        headers: result?.headers || [],
      },
    });
  } catch (error) {
    logger.logEvent("error", "Error building PTRS v2 mapped dataset", {
      action: "PtrsV2BuildMappedDataset",
      ptrsId,
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
 * GET /api/v2/ptrs/:id/sample?limit=10&offset=0
 * Returns a small window of staged rows + total count + inferred headers.
 */
async function getSample(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const limit = Math.min(parseInt(req.query.limit || "10", 10), 200);
  const offset = Math.max(parseInt(req.query.offset || "0", 10), 0);

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    // Confirm the PTRS run exists and belongs to this tenant
    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    const { rows, total, headers, headerMeta } =
      await tmPtrsService.getImportSample({
        customerId,
        ptrsId,
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
      entityId: ptrsId,
      details: { limit, offset, returned: rows.length, total },
    });

    res.status(200).json({
      status: "success",
      data: { rows, total, headers, headerMeta, limit, offset },
    });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS v2 sample", {
      action: "PtrsV2GetSample",
      ptrsId,
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
 * GET /api/v2/ptrs/:id/unified-sample?limit=10&offset=0
 * Returns a small window of main rows + unified headers/examples merged from all datasets.
 */
async function getUnifiedSample(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const limit = Math.min(parseInt(req.query.limit || "10", 10), 200);
  const offset = Math.max(parseInt(req.query.offset || "0", 10), 0);

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    // Confirm the PTRS run exists and belongs to this tenant
    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    const { rows, total, headers, headerMeta } =
      await ptrsService.getUnifiedSample({
        customerId,
        ptrsId,
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
      entityId: ptrsId,
      details: { limit, offset, returned: rows.length, total },
    });

    return res.status(200).json({
      status: "success",
      data: { rows, total, headers, headerMeta, limit, offset },
    });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS v2 unified sample", {
      action: "PtrsV2GetUnifiedSample",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

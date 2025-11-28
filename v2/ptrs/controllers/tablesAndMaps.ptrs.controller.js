const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const { safeLog, safeMeta, slog } = require("@/v2/ptrs/services/ptrs.service");
const ptrsService = require("@/v2/ptrs/services/ptrs.service");
const tmPtrsService = require("@/v2/ptrs/services/tablesAndMaps.ptrs.service");

module.exports = {
  getMap,
  saveMap,
};

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
    }
    const { headers, total, headerMeta } = await tmPtrsService.getImportSample({
      customerId,
      ptrsId,
      limit: 10,
      offset: 0,
    });

    slog.info("getColumnMap", {
      id: map?.id,
      joinsType: typeof map?.joins,
      joinsRaw: map?.joins,
    });

    safeLog("[PTRS controller.getMap] map + header meta", {
      id: map?.id || null,
      hasMap: !!map,
      joinsType: typeof map?.joins,
      joinsRaw: map?.joins,
      mappingsKeys: map?.mappings ? Object.keys(map.mappings) : [],
      headersCount: Array.isArray(headers) ? headers.length : 0,
      headerSample: Array.isArray(headers) ? headers.slice(0, 10) : [],
      hasHeaderMeta: !!headerMeta,
    });

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

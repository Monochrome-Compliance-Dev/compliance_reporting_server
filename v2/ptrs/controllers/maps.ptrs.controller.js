const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const { safeLog } = require("@/v2/ptrs/controllers/ptrs.controller");
const { safeMeta, slog } = require("@/v2/ptrs/services/ptrs.service");
const ptrsService = require("@/v2/ptrs/services/ptrs.service");
const tmPtrsService = require("@/v2/ptrs/services/maps.ptrs.service");

module.exports = {
  getMap,
  saveMap,
  getSample,
  buildMappedDataset,
  getFieldMap,
  saveFieldMap,
  listPtrsWithMap,
};

// TODO: future: allow external transaction but only from beginTransactionWithCustomerContext

function maybeParse(value) {
  if (!value) return value;

  if (typeof value === "object") return value;

  if (typeof value === "string") {
    try {
      return JSON.parse(value);
    } catch {
      return value;
    }
  }

  return value;
}

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

    const map = await tmPtrsService.getMap({ customerId, ptrsId });

    if (map) {
      map.extras = maybeParse(map.extras);
      map.fallbacks = maybeParse(map.fallbacks);
      map.defaults = maybeParse(map.defaults);
      map.rowRules = maybeParse(map.rowRules);
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
      data: {
        map,
        headers,
        headerMeta,
      },
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

  const body = req.body || {};

  const {
    mappings = null,
    extras = null,
    profileId = null,
    defaults = null,
    fallbacks = null,
    rowRules = null,
  } = body;

  const hasProfileId = !!profileId;
  const hasMappingsObject =
    mappings && typeof mappings === "object" && !Array.isArray(mappings);

  // Only update joins/customFields when explicitly provided.
  // Prevent MapPanel saves from stomping joins/customFields.

  slog.info(
    "[PTRS v2 saveMap] body",
    safeMeta({
      ptrsId,
      mappingsKeys: hasMappingsObject ? Object.keys(mappings) : [],
      hasProfileId,
    }),
  );

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    if (!hasProfileId && !hasMappingsObject) {
      return res
        .status(400)
        .json({ status: "error", message: "mappings object is required" });
    }

    if (hasProfileId && mappings !== null && !hasMappingsObject) {
      return res.status(400).json({
        status: "error",
        message: "mappings must be an object or null for profile-backed saves",
      });
    }

    // Confirm the PTRS run exists and belongs to this tenant
    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    if (hasMappingsObject) {
      // Best-effort validation: warn but don't block if headers slightly differ (case/space)
      // IMPORTANT: do NOT call getImportSample here (it is expensive).
      // Validate against dataset metadata across all datasets instead.
      const { headers } = await tmPtrsService.getAllDatasetHeaderInfo({
        customerId,
        ptrsId,
      });

      const norm = (s) =>
        String(s || "")
          .toLowerCase()
          .replace(/\s+/g, "");

      const headerSet = new Set((headers || []).map(norm));
      const missing = Object.keys(mappings).filter(
        (src) => !headerSet.has(norm(src)),
      );

      if (missing.length) {
        logger.info(
          "PTRS v2 saveMap: some mapping headers not found exactly in dataset headers",
          {
            action: "PtrsV2SaveMap",
            ptrsId,
            customerId,
            missing,
          },
        );
      }
    }

    const saved = await tmPtrsService.saveColumnMap({
      customerId,
      ptrsId,
      mappings,
      extras,
      fallbacks,
      defaults,
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
      details: { keys: hasMappingsObject ? Object.keys(mappings).length : 0 },
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
 * GET /api/v2/ptrs/:id/field-map?profileId=...
 * Returns profile-scoped canonical field mappings.
 */
async function getFieldMap(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const profileId = req.query.profileId || null;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    if (!profileId) {
      return res
        .status(400)
        .json({ status: "error", message: "profileId is required" });
    }

    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    const fieldMap = await tmPtrsService.getFieldMap({
      customerId,
      ptrsId,
      profileId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2GetFieldMap",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: {
        profileId,
        count: Array.isArray(fieldMap) ? fieldMap.length : 0,
      },
    });

    return res.status(200).json({
      status: "success",
      data: { fieldMap },
    });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS v2 field map", {
      action: "PtrsV2GetFieldMap",
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
 * POST /api/v2/ptrs/:id/field-map
 * Body:
 * {
 *   profileId: string,
 *   fieldMap: Array<{
 *     canonicalField: string,
 *     sourceRole: string,
 *     sourceColumn?: string,
 *     transformType?: string,
 *     transformConfig?: object,
 *     meta?: object
 *   }>
 * }
 */
async function saveFieldMap(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  const { profileId = null, fieldMap = null } = req.body || {};

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    if (!profileId) {
      return res
        .status(400)
        .json({ status: "error", message: "profileId is required" });
    }
    if (!Array.isArray(fieldMap)) {
      return res
        .status(400)
        .json({ status: "error", message: "fieldMap array is required" });
    }

    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    const saved = await tmPtrsService.saveFieldMap({
      customerId,
      ptrsId,
      profileId,
      fieldMap,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2SaveFieldMap",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: {
        profileId,
        count: Array.isArray(saved) ? saved.length : 0,
      },
    });

    return res
      .status(200)
      .json({ status: "success", data: { fieldMap: saved } });
  } catch (error) {
    logger.logEvent("error", "Error saving PTRS v2 field map", {
      action: "PtrsV2SaveFieldMap",
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
      safeMeta({ customerId, ptrsId, userId }),
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
      }),
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
 * GET /api/v2/ptrs/compatible-maps
 * Returns PTRS runs that have a saved column map, including mapMeta for compatibility filtering.
 */
async function listPtrsWithMap(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const profileId = req.query.profileId || null;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const result = await tmPtrsService.listCompatibleMaps({
      customerId,
      profileId,
    });
    const items = Array.isArray(result?.items)
      ? result.items
      : Array.isArray(result)
        ? result
        : [];

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ListCompatibleMaps",
      entity: "Ptrs",
      entityId: null,
      details: { profileId, count: Array.isArray(items) ? items.length : 0 },
    });

    return res.status(200).json({ status: "success", data: { items } });
  } catch (error) {
    return next(error);
  }
}

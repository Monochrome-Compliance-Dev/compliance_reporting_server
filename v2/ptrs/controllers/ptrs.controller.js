const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const ptrsService = require("@/v2/ptrs/services/ptrs.service");

function safeMeta(meta) {
  try {
    return JSON.parse(JSON.stringify(meta, _svcReplacer()));
  } catch {
    return { note: "unserialisable meta" };
  }
}

const slog = {
  info: (msg, meta) => logger?.info?.(msg, safeMeta(meta)),
  warn: (msg, meta) => logger?.warn?.(msg, safeMeta(meta)),
  error: (msg, meta) => logger?.error?.(msg, safeMeta(meta)),
  debug: (msg, meta) => logger?.debug?.(msg, safeMeta(meta)),
};

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
    if (typeof s === "string" && s.length > maxLen) {
      return s.slice(0, maxLen) + "...[truncated]";
    }
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
 * POST /api/v2/ptrs
 * Creates a new PTRS run record (tbl_ptrs) for the current customer.
 * Body (JSON): {
 *   profileId?: string,
 *   label?: string,
 *   periodStart?: string (YYYY-MM-DD),
 *   periodEnd?: string (YYYY-MM-DD),
 *   reportingEntityName?: string
 * }
 */
async function createPtrs(req, res, next) {
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

    const { profileId, label, periodStart, periodEnd, reportingEntityName } =
      req.body || {};

    const ptrs = await ptrsService.createPtrs({
      customerId,
      profileId,
      label,
      periodStart,
      periodEnd,
      reportingEntityName,
      createdBy: userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2CreatePtrs",
      entity: "Ptrs",
      entityId: ptrs.id,
      details: {
        profileId: ptrs.profileId || null,
        periodStart: ptrs.periodStart || null,
        periodEnd: ptrs.periodEnd || null,
        label: ptrs.label || null,
      },
    });

    return res.status(201).json({ status: "success", data: ptrs });
  } catch (error) {
    logger.logEvent("error", "Error creating PTRS v2 record", {
      action: "PtrsV2CreatePtrs",
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
 * POST /api/v2/ptrs/:id/import
 * Accepts:
 *  - text/csv body
 *  - multipart/form-data (file field named "file")
 */
async function importCsv(req, res, next) {
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

    // Choose input source:
    const isTextCsv = (req.headers["content-type"] || "").includes("text/csv");
    const fileBuffer = req.file?.buffer;
    const fileMeta =
      req.file && fileBuffer
        ? {
            originalName: req.file.originalname || null,
            mimeType: req.file.mimetype || null,
            sizeBytes: typeof req.file.size === "number" ? req.file.size : null,
          }
        : null;

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
        ptrsId,
        stream: req, // readable
        fileMeta: fileMeta || {
          originalName: `ptrs-${ptrsId}.csv`,
          mimeType: "text/csv",
          sizeBytes: null,
        },
      });
    } else {
      // Parse the in-memory buffer (if using Multer)
      const { Readable } = require("stream");
      const stream = Readable.from(fileBuffer);
      rowsInserted = await ptrsService.importCsvStream({
        customerId,
        ptrsId,
        stream,
        fileMeta,
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
      entityId: ptrsId,
      details: { rowsInserted, durationMs },
    });

    res
      .status(200)
      .json({ status: "success", data: { rowsInserted, durationMs } });
  } catch (error) {
    logger.logEvent("error", "Error importing PTRS v2 CSV", {
      action: "PtrsV2ImportCsv",
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
 * GET /api/v2/ptrs/:id
 * Returns the ptrs/upload metadata for the tenant
 */
async function getPtrs(req, res, next) {
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

    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2GetPtrs",
      entity: "Ptrs",
      entityId: ptrsId,
      details: { exists: !!ptrs },
    });

    return res.status(200).json({ status: "success", data: ptrs });
  } catch (error) {
    return next(error);
  }
}

async function updatePtrs(req, res, next) {
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

    const {
      currentStep,
      label,
      periodStart,
      periodEnd,
      reportingEntityName,
      profileId,
      status,
      meta,
    } = req.body || {};

    const ptrs = await ptrsService.updatePtrs({
      customerId,
      ptrsId,
      currentStep,
      label,
      periodStart,
      periodEnd,
      reportingEntityName,
      profileId,
      status,
      meta,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2UpdatePtrs",
      entity: "Ptrs",
      entityId: ptrsId,
      details: {
        currentStep: ptrs.currentStep || null,
        status: ptrs.status || null,
      },
    });

    return res.status(200).json({ status: "success", data: ptrs });
  } catch (error) {
    logger.logEvent("error", "Error updating PTRS v2 record", {
      action: "PtrsV2UpdatePtrs",
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
 * POST /api/v2/ptrs/:id/stage
 * Body: { steps?: Array<...>, persist?: boolean, limit?: number }
 * When persist=true, writes staged rows and updates ptrs status.
 */
async function stagePtrs(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const {
    steps = [],
    persist = false,
    limit = 50,
    profileId = null,
  } = req.body || {};

  safeLog("[PTRS controller.stagePtrs] received", {
    customerId: req.effectiveCustomerId,
    ptrsId: req.params.id,
    body: req.body,
    profileId,
  });

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    safeLog("[PTRS controller.stagePtrs] invoking service", {
      steps,
      persist,
      limit,
      profileId,
    });

    // Confirm the PTRS run exists and belongs to this tenant
    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    const result = await ptrsService.stagePtrs({
      customerId,
      ptrsId,
      steps,
      persist: Boolean(persist),
      limit: Math.min(Number(limit) || 50, 500),
      userId,
      profileId,
    });

    safeLog("[PTRS controller.stagePtrs] result", result);

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: persist ? "PtrsV2StagePersist" : "PtrsV2StagePreview",
      entity: "PtrsStage",
      entityId: ptrsId,
      details: { stepCount: Array.isArray(steps) ? steps.length : 0, limit },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error staging PTRS v2 ptrs", {
      action: "PtrsV2StagePtrs",
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

// /**
//  * POST /api/v2/ptrs/:id/preview
//  * Body: {
//  *   steps?: Array<{
//  *     kind: "filter" | "derive" | "rename",
//  *     config: any
//  *   }>,
//  *   limit?: number
//  * }
//  * Returns: { sample: [], affectedCount: number }
//  */
// async function preview(req, res, next) {
//   const customerId = req.effectiveCustomerId;
//   const userId = req.auth?.id;
//   const ip = req.ip;
//   const device = req.headers["user-agent"];
//   const ptrsId = req.params.id;
//   const { steps = [], limit = 50 } = req.body || {};

//   try {
//     if (!customerId) {
//       return res
//         .status(400)
//         .json({ status: "error", message: "Customer ID missing" });
//     }
//     // Confirm the PTRS run exists and belongs to this tenant
// const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
// if (!ptrs) {
//   return res
//     .status(404)
//     .json({ status: "error", message: "Ptrs not found" });
// }

//     const result = await ptrsService.previewTransform({
//       customerId,
//       ptrsId,
//       steps,
//       limit: Math.min(Number(limit) || 50, 500),
//     });

//     await auditService.logEvent({
//       customerId,
//       userId,
//       ip,
//       device,
//       action: "PtrsV2Preview",
//       entity: "PtrsUpload",
//       entityId: ptrsId,
//       details: { stepCount: Array.isArray(steps) ? steps.length : 0, limit },
//     });

//     res.status(200).json({ status: "success", data: result });
//   } catch (error) {
//     logger.logEvent("error", "Error previewing PTRS v2 transform", {
//       action: "PtrsV2Preview",
//       ptrsId,
//       customerId,
//       userId,
//       error: error.message,
//       statusCode: error.statusCode || 500,
//       timestamp: new Date().toISOString(),
//     });
//     return next(error);
//   }
// }

/**
 * GET or POST /api/v2/ptrs/:id/stage/preview
 * Returns: { sample: [], affectedCount: number }
 */
async function getStagePreview(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  safeLog("[PTRS controller.getStagePreview] received", {
    customerId: req.effectiveCustomerId,
    ptrsId: req.params.id,
    body: req.body,
    query: req.query,
  });

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
    // Confirm the PTRS run exists and belongs to this tenant
    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    safeLog("[PTRS controller.getStagePreview] invoking service", {
      limit,
      profileId,
    });

    const result = await ptrsService.getStagePreview({
      customerId,
      ptrsId,
      limit,
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
      entityId: ptrsId,
      details: { limit },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger.logEvent("error", "Error getting PTRS v2 stage preview", {
      action: "PtrsV2StagePreview",
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
 * GET /api/v2/ptrs/:id/rules/preview?limit=50
 */
async function rulesPreview(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const limit = Math.min(parseInt(req.query.limit || "50", 10), 500);
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
    const out = await ptrsService.getRulesPreview({
      customerId,
      ptrsId,
      limit,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2RulesPreview",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: {
        limit,
        returned: Array.isArray(out?.rows) ? out.rows.length : 0,
      },
    });
    return res.status(200).json({ status: "success", data: out });
  } catch (error) {
    logger.logEvent("error", "Error previewing PTRS v2 rules", {
      action: "PtrsV2RulesPreview",
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
 * POST /api/v2/ptrs/:id/rules/apply
 * Body: { profileId? }
 */
async function rulesApply(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const profileId = req.body?.profileId || null;
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
    const out = await ptrsService.applyRulesAndPersist({
      customerId,
      ptrsId,
      profileId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2RulesApply",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: { persisted: out?.persisted ?? 0 },
    });
    return res.status(200).json({ status: "success", data: out });
  } catch (error) {
    logger.logEvent("error", "Error applying PTRS v2 rules", {
      action: "PtrsV2RulesApply",
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
 * GET /api/v2/ptrs
 * Returns a list of ptrss for the tenant
 */
async function listPtrs(req, res, next) {
  const customerId = req.effectiveCustomerId;
  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const items = await ptrsService.listPtrs({
      customerId,
    });

    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ListPtrs",
      entity: "Ptrs",
      entityId: null,
      details: { count: Array.isArray(items) ? items.length : 0 },
    });

    return res.status(200).json({ status: "success", data: { items } });
  } catch (error) {
    return next(error);
  }
}

/**
 * GET /api/v2/ptrs/with-map
 * Returns a list of ptrs for the tenant that have an associated column map.
 */
async function listPtrsWithMap(req, res, next) {
  const customerId = req.effectiveCustomerId;
  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const items = await ptrsService.listPtrsWithMap({
      customerId,
    });

    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ListPtrsWithMap",
      entity: "Ptrs",
      entityId: null,
      details: { count: Array.isArray(items) ? items.length : 0 },
    });

    return res.status(200).json({ status: "success", data: { items } });
  } catch (error) {
    return next(error);
  }
}

/**
 * GET /api/v2/ptrs/blueprint?profileId=veolia
 * Returns the generic blueprint optionally merged with a customer/profile overlay.
 */
async function getBlueprint(req, res, next) {
  try {
    const customerId = req.effectiveCustomerId || null;
    const userId = req.auth?.id || null;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const profileIdRaw = req.query.profileId || "";
    const profileId =
      typeof profileIdRaw === "string" ? profileIdRaw.trim() : "";

    const merged = await ptrsService.getBlueprint({
      customerId,
      profileId: profileId || null,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2GetBlueprint",
      entity: "PtrsBlueprint",
      entityId: profileId || "ptrsCalculationBlueprint",
      details: {
        hasProfile: !!profileId,
      },
    });

    return res.status(200).json({ status: "success", data: merged });
  } catch (error) {
    return next(error);
  }
}

async function listProfiles(req, res, next) {
  try {
    const customerId = req.query.customerId || req.effectiveCustomerId;
    const customerProfileId = req.query.customerProfileId || null;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const profiles = await ptrsService.listProfiles(
      customerId,
      customerProfileId
    );

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ListProfiles",
      entity: "PtrsProfile",
      entityId: null,
      details: {
        count: Array.isArray(profiles) ? profiles.length : 0,
        customerProfileId,
      },
    });

    res.status(200).json({ status: "success", data: { items: profiles } });
  } catch (error) {
    next(error);
  }
}

// /**
//  * POST /api/v2/ptrs/profiles
//  * Body: { profileId?, name, description?, isDefault?, config? }
//  */
// async function createProfile(req, res, next) {
//   const customerId = req.effectiveCustomerId || req.body?.customerId;
//   const userId = req.auth?.id;
//   const ip = req.ip;
//   const device = req.headers["user-agent"];
//   try {
//     if (!customerId) {
//       return res
//         .status(400)
//         .json({ status: "error", message: "Customer ID missing" });
//     }
//     const created = await ptrsService.createProfile({
//       customerId,
//       payload: req.body || {},
//       userId,
//     });
//     await auditService.logEvent({
//       customerId,
//       userId,
//       ip,
//       device,
//       action: "PtrsV2CreateProfile",
//       entity: "PtrsProfile",
//       entityId: created.id,
//       details: {
//         name: created.name,
//         profileId: created.profileId || created.id,
//       },
//     });
//     return res.status(201).json({ status: "success", data: created });
//   } catch (error) {
//     return next(error);
//   }
// }

// /**
//  * GET /api/v2/ptrs/profiles/:id
//  */
// async function getProfile(req, res, next) {
//   const customerId = req.effectiveCustomerId;
//   const profileId = req.params.id;
//   try {
//     if (!customerId) {
//       return res
//         .status(400)
//         .json({ status: "error", message: "Customer ID missing" });
//     }
//     const row = await ptrsService.getProfile({ customerId, profileId });
//     if (!row) {
//       return res
//         .status(404)
//         .json({ status: "error", message: "Profile not found" });
//     }
//     return res.status(200).json({ status: "success", data: row });
//   } catch (error) {
//     return next(error);
//   }
// }

// /**
//  * PATCH /api/v2/ptrs/profiles/:id
//  */
// async function updateProfile(req, res, next) {
//   const customerId = req.effectiveCustomerId;
//   const profileId = req.params.id;
//   const userId = req.auth?.id;
//   const ip = req.ip;
//   const device = req.headers["user-agent"];
//   try {
//     if (!customerId) {
//       return res
//         .status(400)
//         .json({ status: "error", message: "Customer ID missing" });
//     }
//     const updated = await ptrsService.updateProfile({
//       customerId,
//       profileId,
//       payload: req.body || {},
//       userId,
//     });
//     await auditService.logEvent({
//       customerId,
//       userId,
//       ip,
//       device,
//       action: "PtrsV2UpdateProfile",
//       entity: "PtrsProfile",
//       entityId: profileId,
//     });
//     return res.status(200).json({ status: "success", data: updated });
//   } catch (error) {
//     return next(error);
//   }
// }

// /**
//  * DELETE /api/v2/ptrs/profiles/:id
//  */
// async function deleteProfile(req, res, next) {
//   const customerId = req.effectiveCustomerId;
//   const profileId = req.params.id;
//   const userId = req.auth?.id;
//   const ip = req.ip;
//   const device = req.headers["user-agent"];
//   try {
//     if (!customerId) {
//       return res
//         .status(400)
//         .json({ status: "error", message: "Customer ID missing" });
//     }
//     const result = await ptrsService.deleteProfile({ customerId, profileId });
//     await auditService.logEvent({
//       customerId,
//       userId,
//       ip,
//       device,
//       action: "PtrsV2DeleteProfile",
//       entity: "PtrsProfile",
//       entityId: profileId,
//       details: { ok: result.ok === true },
//     });
//     return res.status(200).json({ status: "success", data: result });
//   } catch (error) {
//     return next(error);
//   }
// }

// GET /v2/ptrs/:id/rules
async function getRules(req, res, next) {
  try {
    const customerId = req.effectiveCustomerId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const ptrsId = req.params.id;

    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const map = await ptrsService.getMap({ ptrsId, customerId });
    const src = map?.map || map || {};
    const extras =
      typeof src.extras === "string"
        ? JSON.parse(src.extras)
        : src.extras || {};
    const rowRules =
      typeof src.rowRules === "string"
        ? JSON.parse(src.rowRules)
        : src.rowRules || [];
    const crossRowRules = extras?.__experimentalCrossRowRules || [];

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2GetRules",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: {
        rowRules: Array.isArray(rowRules) ? rowRules.length : 0,
        crossRowRules: Array.isArray(crossRowRules) ? crossRowRules.length : 0,
      },
    });

    return res.json({ data: { rowRules, crossRowRules } });
  } catch (err) {
    return next(err);
  }
}

// POST /v2/ptrs/:id/rules
/**
 * POST /api/v2/ptrs/:id/rules
 * Body: { rowRules?: [], crossRowRules?: [] }
 * Persist only rules — do NOT overwrite mappings/defaults/joins.
 */
async function saveRules(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const { rowRules = [], crossRowRules = [] } = req.body || {};

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

    // Persist rules only
    const updated = await ptrsService.updateRulesOnly({
      customerId,
      ptrsId,
      rowRules: Array.isArray(rowRules) ? rowRules : [],
      crossRowRules: Array.isArray(crossRowRules) ? crossRowRules : [],
      userId,
    });

    // Unwrap what we actually stored
    const extras =
      typeof updated.extras === "string"
        ? (() => {
            try {
              return JSON.parse(updated.extras);
            } catch {
              return {};
            }
          })()
        : updated.extras || {};
    const storedCross = (extras && extras.__experimentalCrossRowRules) || [];

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2SaveRules",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: {
        rowRules: Array.isArray(rowRules) ? rowRules.length : 0,
        crossRowRules: Array.isArray(crossRowRules) ? crossRowRules.length : 0,
      },
    });

    return res.status(200).json({
      status: "success",
      data: {
        rowRules: Array.isArray(updated.rowRules) ? updated.rowRules : [],
        crossRowRules: Array.isArray(storedCross) ? storedCross : [],
      },
    });
  } catch (error) {
    logger.logEvent("error", "Error saving PTRS v2 rules", {
      action: "PtrsV2SaveRules",
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

module.exports = {
  safeLog,
  createPtrs,
  importCsv,
  getPtrs,
  updatePtrs,
  stagePtrs,
  //   preview,
  getStagePreview,
  rulesPreview,
  rulesApply,
  listPtrs,
  listPtrsWithMap,
  getBlueprint,
  listProfiles,
  getRules,
  saveRules,
  //   // Profiles CRUD
  //   createProfile,
  //   getProfile,
  //   updateProfile,
  //   deleteProfile,
};

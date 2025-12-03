const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const ptrsService = require("@/v2/ptrs/services/ptrs.service");
const rulesService = require("@/v2/ptrs/services/rules.ptrs.service");

module.exports = {
  rulesPreview,
  rulesApply,
  getRules,
  saveRules,
  getProfileRules,
};

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
    const out = await rulesService.getRulesPreview({
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
    const out = await rulesService.applyRulesAndPersist({
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

// POST /v2/ptrs/:id/rules
/**
 * POST /api/v2/ptrs/:id/rules
 * Body: { ???? }
 * Persist only rules — do NOT overwrite mappings/defaults/joins???
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

    const profileId = ptrs.profileId || null;

    // Persist rules only into tbl_ptrs_ruleset
    await rulesService.updateRulesOnly({
      customerId,
      ptrsId,
      profileId,
      rowRules: Array.isArray(rowRules) ? rowRules : [],
      crossRowRules: Array.isArray(crossRowRules) ? crossRowRules : [],
      userId,
    });

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
        rowRules: Array.isArray(rowRules) ? rowRules : [],
        crossRowRules: Array.isArray(crossRowRules) ? crossRowRules : [],
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

/**
 * GET /api/v2/ptrs/:id/rules
 * Returns row-level rules and cross-row rules for the given PTRS run.???
 */
async function getRules(req, res, next) {
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

    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    const { rowRules, crossRowRules } = await rulesService.getRules({
      customerId,
      ptrsId,
    });

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

    return res.status(200).json({
      status: "success",
      data: { rowRules: rowRules || [], crossRowRules: crossRowRules || [] },
    });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS v2 rules", {
      action: "PtrsV2GetRules",
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
 * GET /api/v2/ptrs/id/rules/sources
 * Returns a list of previous saved rules under the same profile
 */
/**
 * GET /api/v2/ptrs/:id/rules/sources
 * Returns a list of previous saved rulesets under the same profile
 */
async function getProfileRules(req, res, next) {
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

    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    const profileId = ptrs.profileId || null;

    const rulesets = await rulesService.getProfileRules({
      customerId,
      profileId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2GetProfileRules",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: {
        count: Array.isArray(rulesets) ? rulesets.length : 0,
      },
    });

    return res.status(200).json({
      status: "success",
      data: Array.isArray(rulesets) ? rulesets : [],
    });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS profile rules", {
      action: "PtrsV2GetProfileRules",
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const ptrsService = require("@/v2/ptrs/services/ptrs.service");
const exclusionsService = require("@/v2/ptrs/services/exclusions.ptrs.service");

module.exports = {
  exclusionsApply,
  exclusionsPreview,
  exclusionKeywordsList,
  exclusionKeywordsCreate,
  exclusionKeywordsDelete,
  exclusionKeywordsUpdate,
};

/**
 * POST /api/v2/ptrs/:id/exclusions/apply
 * Body: { profileId? }
 */
async function exclusionsApply(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const profileId = req.body?.profileId || null;
  const category = req.body?.category || "all";

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

    const out = await exclusionsService.applyExclusionsAndPersist({
      customerId,
      ptrsId,
      profileId,
      category,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ExclusionsApply",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: {
        persisted: out?.persisted ?? 0,
      },
    });

    return res.status(200).json({ status: "success", data: out });
  } catch (error) {
    logger.logEvent("error", "Error applying PTRS v2 exclusions", {
      action: "PtrsV2ExclusionsApply",
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
 * GET /api/v2/ptrs/:id/exclusions/preview?category=gov&limit=10&profileId=...
 */
async function exclusionsPreview(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  const category = req.query?.category || "all";
  const limit = req.query?.limit;
  const profileId = req.query?.profileId || null;

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

    const out = await exclusionsService.previewExclusions({
      customerId,
      ptrsId,
      profileId,
      category,
      limit,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ExclusionsPreview",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: { category, limit: Number(limit) || null },
    });

    return res.status(200).json({ status: "success", data: out });
  } catch (error) {
    logger.logEvent("error", "Error previewing PTRS v2 exclusions", {
      action: "PtrsV2ExclusionsPreview",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

async function exclusionKeywordsList(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const profileId = req.query?.profileId || null;

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

    const out = await exclusionsService.listKeywordExclusions({
      customerId,
      profileId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ExclusionKeywordsList",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: { count: Array.isArray(out) ? out.length : 0 },
    });

    return res.status(200).json({ status: "success", data: { rows: out } });
  } catch (error) {
    logger.logEvent("error", "Error listing PTRS v2 exclusion keywords", {
      action: "PtrsV2ExclusionKeywordsList",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

async function exclusionKeywordsCreate(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  const profileId = req.body?.profileId || null;
  const keyword = req.body?.keyword || "";
  const field = req.body?.field || null;
  const matchType = req.body?.matchType || null;
  const notes = req.body?.notes || null;

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
    if (!field) {
      return res
        .status(400)
        .json({ status: "error", message: "field is required" });
    }
    if (!matchType) {
      return res
        .status(400)
        .json({ status: "error", message: "matchType is required" });
    }

    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    const out = await exclusionsService.createKeywordExclusion({
      customerId,
      profileId,
      keyword,
      field,
      matchType,
      notes,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ExclusionKeywordsCreate",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: { keyword: out?.keyword || keyword },
    });

    return res.status(200).json({ status: "success", data: out });
  } catch (error) {
    logger.logEvent("error", "Error creating PTRS v2 exclusion keyword", {
      action: "PtrsV2ExclusionKeywordsCreate",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

// PUT /api/v2/ptrs/:id/exclusions/keywords/:keywordId
async function exclusionKeywordsUpdate(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const keywordId = req.params.keywordId;
  const { keyword, field, matchType, notes } = req.body;
  const profileId = req.body?.profileId || null;

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
    if (!keywordId) {
      return res
        .status(400)
        .json({ status: "error", message: "keywordId is required" });
    }
    if (!field) {
      return res
        .status(400)
        .json({ status: "error", message: "field is required" });
    }
    if (!matchType) {
      return res
        .status(400)
        .json({ status: "error", message: "matchType is required" });
    }

    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    const out = await exclusionsService.updateKeywordExclusion({
      customerId,
      profileId,
      keywordId,
      keyword,
      field,
      matchType,
      notes,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ExclusionKeywordsUpdate",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: { keywordId, keyword },
    });

    return res.status(200).json({ status: "success", data: out });
  } catch (error) {
    logger.logEvent("error", "Error updating PTRS v2 exclusion keyword", {
      action: "PtrsV2ExclusionKeywordsUpdate",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

async function exclusionKeywordsDelete(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  const profileId = req.query?.profileId || null;
  const keywordId = req.params.keywordId;

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
    if (!keywordId) {
      return res
        .status(400)
        .json({ status: "error", message: "keywordId is required" });
    }

    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res
        .status(404)
        .json({ status: "error", message: "Ptrs not found" });
    }

    const out = await exclusionsService.deleteKeywordExclusion({
      customerId,
      profileId,
      keywordId,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ExclusionKeywordsDelete",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: { keywordId },
    });

    return res.status(200).json({ status: "success", data: out });
  } catch (error) {
    logger.logEvent("error", "Error deleting PTRS v2 exclusion keyword", {
      action: "PtrsV2ExclusionKeywordsDelete",
      ptrsId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
    });
    return next(error);
  }
}

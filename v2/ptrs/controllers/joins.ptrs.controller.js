const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const ptrsService = require("@/v2/ptrs/services/ptrs.service");
const joinsService = require("@/v2/ptrs/services/joins.ptrs.service");
/**
 * Joins + custom fields controller.
 *
 * Contract (NO wrapper envelope):
 *  GET /api/v2/ptrs/:id/joins -> { joins: { conditions: [] }, customFields: [], profileId: string|null }
 *  PUT /api/v2/ptrs/:id/joins -> same shape
 */
module.exports = {
  getJoins,
  saveJoins,
  listCompatibleJoins,
};

async function getJoins(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  try {
    if (!customerId) {
      return res.status(400).json({ message: "Customer ID missing" });
    }

    // Confirm the PTRS run exists and belongs to this tenant
    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res.status(404).json({ message: "Ptrs not found" });
    }

    const { joins, customFields, profileId } = await joinsService.getJoins({
      customerId,
      ptrsId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2GetJoins",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: {
        conditions: Array.isArray(joins.conditions)
          ? joins.conditions.length
          : 0,
        customFields: Array.isArray(customFields) ? customFields.length : 0,
      },
    });

    return res.status(200).json({ joins, customFields, profileId });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS v2 joins", {
      action: "PtrsV2GetJoins",
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

async function listCompatibleJoins(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  try {
    if (!customerId) {
      return res.status(400).json({ message: "Customer ID missing" });
    }

    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res.status(404).json({ message: "Ptrs not found" });
    }

    const result = await joinsService.listCompatibleJoins({
      customerId,
      ptrsId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2ListCompatibleJoins",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: {
        items: Array.isArray(result?.items) ? result.items.length : 0,
      },
    });

    return res.status(200).json(result || { items: [] });
  } catch (error) {
    logger.logEvent("error", "Error listing PTRS v2 compatible joins", {
      action: "PtrsV2ListCompatibleJoins",
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

async function saveJoins(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  const {
    joins = null,
    customFields = null,
    profileId = null,
  } = req.body || {};

  try {
    if (!customerId) {
      return res.status(400).json({ message: "Customer ID missing" });
    }

    // Strict new-world validation
    if (
      !joins ||
      typeof joins !== "object" ||
      !Array.isArray(joins.conditions)
    ) {
      return res.status(400).json({
        message: "Invalid joins payload (expected { conditions: [] })",
      });
    }
    if (!Array.isArray(customFields)) {
      return res
        .status(400)
        .json({ message: "Invalid customFields payload (expected array)" });
    }

    // Confirm the PTRS run exists and belongs to this tenant
    const ptrs = await ptrsService.getPtrs({ customerId, ptrsId });
    if (!ptrs) {
      return res.status(404).json({ message: "Ptrs not found" });
    }

    const saved = await joinsService.saveJoins({
      customerId,
      ptrsId,
      joins,
      customFields,
      profileId,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2SaveJoins",
      entity: "PtrsUpload",
      entityId: ptrsId,
      details: {
        conditions: Array.isArray(joins.conditions)
          ? joins.conditions.length
          : 0,
        customFields: Array.isArray(customFields) ? customFields.length : 0,
      },
    });

    // Return canonical shape (no wrapper)
    return res.status(200).json(saved);
  } catch (error) {
    logger.logEvent("error", "Error saving PTRS v2 joins", {
      action: "PtrsV2SaveJoins",
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

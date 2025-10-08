const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const express = require("express");
const router = express.Router();
const validateRequest = require("@/middleware/validate-request");
const authorise = require("@/middleware/authorise");
const requirePulse = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "pulse",
});
const resourceService = require("./resource.service");
const {
  resourceCreateSchema,
  resourceUpdateSchema,
  resourcePatchSchema,
} = require("./resource.validator");

// routes
router.get("/", requirePulse, getAll);
router.get("/:id", requirePulse, getById);
router.post("/", requirePulse, validateRequest(resourceCreateSchema), create);
router.put("/:id", requirePulse, validateRequest(resourceUpdateSchema), update);
router.patch("/:id", requirePulse, validateRequest(resourcePatchSchema), patch);
router.delete("/:id", requirePulse, _delete);
// Resource Allocation (utilisation) route
router.get("/resource-utilisation", requirePulse, getUtilisation);

module.exports = router;

async function getAll(req, res, next) {
  try {
    const customerId = req.effectiveCustomerId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const resources = await resourceService.getAll({
      customerId,
      order: [["createdAt", "DESC"]],
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetAllResources",
      entity: "Resource",
      details: {
        count: Array.isArray(resources) ? resources.length : undefined,
      },
    });
    res.json({ status: "success", data: resources });
  } catch (error) {
    logger.logEvent("error", "Error fetching all resources", {
      action: "GetAllResources",
      userId: req.auth?.id,
      customerId: req.effectiveCustomerId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getById(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const resource = await resourceService.getById({ id, customerId });
    if (resource) {
      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "GetResourceById",
        entity: "Resource",
        entityId: id,
      });
      res.json({ status: "success", data: resource });
    } else {
      res.status(404).json({ status: "error", message: "Resource not found" });
    }
  } catch (error) {
    logger.logEvent("error", "Error fetching resource by ID", {
      action: "GetResourceById",
      id,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function create(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const resource = await resourceService.create({
      data: req.body,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "CreateResource",
      entity: "Resource",
      entityId: resource.id,
    });
    res.status(201).json({ status: "success", data: resource });
  } catch (error) {
    logger.logEvent("error", "Error creating resource", {
      action: "CreateResource",
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function update(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const resource = await resourceService.update({
      id,
      data: req.body,
      customerId,
      userId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "UpdateResource",
      entity: "Resource",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: resource });
  } catch (error) {
    logger.logEvent("error", "Error updating resource", {
      action: "UpdateResource",
      id,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function patch(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const resource = await resourceService.patch({
      id,
      data: req.body,
      customerId,
      userId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PatchResource",
      entity: "Resource",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: resource });
  } catch (error) {
    logger.logEvent("error", "Error patching resource", {
      action: "PatchResource",
      id,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function _delete(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    await resourceService.delete({ id, customerId, userId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DeleteResource",
      entity: "Resource",
      entityId: id,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error deleting resource", {
      action: "DeleteResource",
      id,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getUtilisation(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const { from, to, includeNonBillable } = req.query;

  try {
    const data = await resourceService.getUtilisation({
      customerId,
      from,
      to,
      includeNonBillable: includeNonBillable === "true",
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetResourceUtilisation",
      entity: "ResourceUtilisation",
      details: { count: Array.isArray(data) ? data.length : undefined },
    });

    res.json({ status: "success", data });
  } catch (error) {
    logger.logEvent("error", "Error fetching resource utilisation", {
      action: "GetResourceUtilisation",
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

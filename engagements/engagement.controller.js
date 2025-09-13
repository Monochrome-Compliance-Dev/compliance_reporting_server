const auditService = require("../audit/audit.service");
const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const engagementService = require("./engagement.service");
const {
  engagementCreateSchema,
  engagementUpdateSchema,
  engagementPatchSchema,
} = require("./engagement.validator");

const requirePulse = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "pulse",
});

router.get("/", requirePulse, getAll);
router.get("/:id", requirePulse, getById);
router.post("/", requirePulse, validateRequest(engagementCreateSchema), create);
router.put(
  "/:id",
  requirePulse,
  validateRequest(engagementUpdateSchema),
  update
);
router.patch(
  "/:id",
  requirePulse,
  validateRequest(engagementPatchSchema),
  patch
);
router.delete("/:id", requirePulse, _delete);

module.exports = router;

async function getAll(req, res, next) {
  try {
    const customerId = req.auth?.customerId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const engagements = await engagementService.getAll({
      customerId,
      order: [["createdAt", "DESC"]],
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetAllEngagements",
      entity: "Engagement",
      details: {
        count: Array.isArray(engagements) ? engagements.length : undefined,
      },
    });
    res.json({ status: "success", data: engagements });
  } catch (error) {
    logger.logEvent("error", "Error fetching all engagements", {
      action: "GetAllEngagements",
      userId: req.auth?.id,
      customerId: req.auth?.customerId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getById(req, res, next) {
  const id = req.params.id;
  const customerId = req.auth?.customerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const engagement = await engagementService.getById({ id, customerId });
    if (engagement) {
      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "GetEngagementById",
        entity: "Engagement",
        entityId: id,
      });
      res.json({ status: "success", data: engagement });
    } else {
      res
        .status(404)
        .json({ status: "error", message: "Engagement not found" });
    }
  } catch (error) {
    logger.logEvent("error", "Error fetching engagement by ID", {
      action: "GetEngagementById",
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
  const customerId = req.auth?.customerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const engagement = await engagementService.create({
      data: req.body,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "CreateEngagement",
      entity: "Engagement",
      entityId: engagement.id,
      details: { status: engagement.status },
    });
    res.status(201).json({ status: "success", data: engagement });
  } catch (error) {
    logger.logEvent("error", "Error creating engagement", {
      action: "CreateEngagement",
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
  const customerId = req.auth?.customerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const engagement = await engagementService.update({
      id,
      data: req.body,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "UpdateEngagement",
      entity: "Engagement",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: engagement });
  } catch (error) {
    logger.logEvent("error", "Error updating engagement", {
      action: "UpdateEngagement",
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
  const customerId = req.auth?.customerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    if (!customerId)
      return res.status(400).json({ message: "Customer ID missing" });
    const engagement = await engagementService.patch({
      id,
      data: req.body,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PatchEngagement",
      entity: "Engagement",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: engagement });
  } catch (error) {
    logger.logEvent("error", "Error patching engagement", {
      action: "PatchEngagement",
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
  const customerId = req.auth?.customerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    if (!customerId)
      return res.status(400).json({ message: "Customer ID missing" });
    await engagementService.delete({ id, customerId, userId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DeleteEngagement",
      entity: "Engagement",
      entityId: id,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error deleting engagement", {
      action: "DeleteEngagement",
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

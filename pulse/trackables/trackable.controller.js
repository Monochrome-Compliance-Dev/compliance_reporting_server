const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const express = require("express");
const router = express.Router();
const validateRequest = require("@/middleware/validate-request");
const authorise = require("@/middleware/authorise");
const trackableService = require("./trackable.service");
const {
  trackableCreateSchema,
  trackableUpdateSchema,
  trackablePatchSchema,
} = require("./trackable.validator");

const requirePulse = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "pulse",
});

router.get("/", requirePulse, getAll);
router.get("/:id", requirePulse, getById);
router.post("/", requirePulse, validateRequest(trackableCreateSchema), create);
router.put(
  "/:id",
  requirePulse,
  validateRequest(trackableUpdateSchema),
  update
);
router.patch(
  "/:id",
  requirePulse,
  validateRequest(trackablePatchSchema),
  patch
);
router.delete("/:id", requirePulse, _delete);

module.exports = router;

async function getAll(req, res, next) {
  try {
    const customerId = req.effectiveCustomerId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const status = req.query.status;
    const trackables = await trackableService.getAll({
      customerId,
      status,
      order: [["createdAt", "DESC"]],
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetAllTrackables",
      entity: "Trackable",
      details: {
        count: Array.isArray(trackables) ? trackables.length : undefined,
        status,
      },
    });
    res.json({ status: "success", data: trackables });
  } catch (error) {
    logger.logEvent("error", "Error fetching all trackables", {
      action: "GetAllTrackables",
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
    const trackable = await trackableService.getById({ id, customerId });
    if (trackable) {
      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "GetTrackableById",
        entity: "Trackable",
        entityId: id,
      });
      res.json({ status: "success", data: trackable });
    } else {
      res.status(404).json({ status: "error", message: "Trackable not found" });
    }
  } catch (error) {
    logger.logEvent("error", "Error fetching trackable by ID", {
      action: "GetTrackableById",
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
    const trackable = await trackableService.create({
      data: req.body,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "CreateTrackable",
      entity: "Trackable",
      entityId: trackable.id,
      details: { status: trackable.status },
    });
    res.status(201).json({ status: "success", data: trackable });
  } catch (error) {
    logger.logEvent("error", "Error creating trackable", {
      action: "CreateTrackable",
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
    const trackable = await trackableService.update({
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
      action: "UpdateTrackable",
      entity: "Trackable",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: trackable });
  } catch (error) {
    logger.logEvent("error", "Error updating trackable", {
      action: "UpdateTrackable",
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
    const trackable = await trackableService.patch({
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
      action: "PatchTrackable",
      entity: "Trackable",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: trackable });
  } catch (error) {
    logger.logEvent("error", "Error patching trackable", {
      action: "PatchTrackable",
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
    await trackableService.delete({ id, customerId, userId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DeleteTrackable",
      entity: "Trackable",
      entityId: id,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error deleting trackable", {
      action: "DeleteTrackable",
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

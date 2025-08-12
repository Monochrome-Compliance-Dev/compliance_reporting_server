const auditService = require("../audit/audit.service");
const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const ptrsService = require("./ptrs.service");
const { ptrsSchema } = require("./ptrs.validator");

// routes
router.get("/", authorise(), getAll);
router.get("/:id", authorise(), getById);
router.post("/", authorise(), validateRequest(ptrsSchema), create);
router.put("/:id", authorise(), validateRequest(ptrsSchema), update);
router.patch("/:id", authorise(), patch);
router.delete("/:id", authorise(), _delete);

module.exports = router;

async function getAll(req, res, next) {
  try {
    const clientId = req.auth?.clientId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const ptrs = await ptrsService.getAll({
      clientId,
      order: [["createdAt", "DESC"]],
    });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetAllPtrs",
      entity: "PtrsReport",
      details: { count: Array.isArray(ptrs) ? ptrs.length : undefined },
    });
    res.json({ status: "success", data: ptrs });
  } catch (error) {
    logger.logEvent("error", "Error fetching all ptrs", {
      action: "GetAllPtrs",
      userId: req.auth?.id,
      clientId: req.auth?.clientId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getById(req, res, next) {
  const id = req.params.id;
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const ptrs = await ptrsService.getById({ id, clientId });
    if (ptrs) {
      await auditService.logEvent({
        clientId,
        userId,
        ip,
        device,
        action: "GetPtrsById",
        entity: "PtrsReport",
        entityId: id,
      });
      res.json({ status: "success", data: ptrs });
    } else {
      res.status(404).json({ status: "error", message: "Ptrs not found" });
    }
  } catch (error) {
    logger.logEvent("error", "Error fetching ptrs by ID", {
      action: "GetPtrsById",
      id,
      clientId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function create(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const ptrs = await ptrsService.create({ data: req.body, clientId });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "CreatePtrs",
      entity: "PtrsReport",
      entityId: ptrs.id,
      details: { status: ptrs.status },
    });
    res.status(201).json({ status: "success", data: ptrs });
  } catch (error) {
    logger.logEvent("error", "Error creating ptrs", {
      action: "CreatePtrs",
      clientId,
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
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const ptrs = await ptrsService.update({ id, data: req.body, clientId });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "UpdatePtrs",
      entity: "PtrsReport",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: ptrs });
  } catch (error) {
    logger.logEvent("error", "Error updating ptrs", {
      action: "UpdatePtrs",
      id,
      clientId,
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
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    if (!clientId) {
      return res.status(400).json({ message: "Client ID missing" });
    }
    const ptrs = await ptrsService.patch({ id, data: req.body, clientId });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "PatchPtrs",
      entity: "PtrsReport",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: ptrs });
  } catch (error) {
    logger.logEvent("error", "Error patching ptrs", {
      action: "PatchPtrs",
      id,
      clientId,
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
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    if (!clientId) {
      return res.status(400).json({ message: "Client ID missing" });
    }
    await ptrsService.delete({ id, clientId, userId });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "DeletePtrs",
      entity: "PtrsReport",
      entityId: id,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error deleting ptrs", {
      action: "DeletePtrs",
      id,
      clientId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

const auditService = require("../audit/audit.service");
const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const ptrsService = require("./ptrs.service");
const { ptrsSchema } = require("./ptrs.validator");

const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// routes
router.get("/", requirePtrs, getAll);
router.get("/:id", requirePtrs, getById);
router.post("/", requirePtrs, validateRequest(ptrsSchema), create);
router.put("/:id", requirePtrs, validateRequest(ptrsSchema), update);
router.patch("/:id", requirePtrs, patch);
router.delete("/:id", requirePtrs, _delete);

module.exports = router;

async function getAll(req, res, next) {
  try {
    const customerId = req.effectiveCustomerId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const ptrs = await ptrsService.getAll({
      customerId,
      order: [["createdAt", "DESC"]],
    });
    await auditService.logEvent({
      customerId,
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
    const ptrs = await ptrsService.getById({ id, customerId });
    if (ptrs) {
      await auditService.logEvent({
        customerId,
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
    const ptrs = await ptrsService.create({ data: req.body, customerId });
    await auditService.logEvent({
      customerId,
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
    const ptrs = await ptrsService.update({ id, data: req.body, customerId });
    await auditService.logEvent({
      customerId,
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
    if (!customerId) {
      return res.status(400).json({ message: "Customer ID missing" });
    }
    const ptrs = await ptrsService.patch({ id, data: req.body, customerId });
    await auditService.logEvent({
      customerId,
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
    if (!customerId) {
      return res.status(400).json({ message: "Customer ID missing" });
    }
    await ptrsService.delete({ id, customerId, userId });
    await auditService.logEvent({
      customerId,
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
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

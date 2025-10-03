const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const express = require("express");
const router = express.Router();
const validateRequest = require("@/middleware/validate-request");
const authorise = require("@/middleware/authorise");
const clientService = require("./client.service");
const {
  clientCreateSchema,
  clientUpdateSchema,
  clientPatchSchema,
} = require("./client.validator");

// middleware for pulse feature
const requirePulse = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "pulse",
});

// routes
router.get("/", requirePulse, getAll);
router.get("/:id", requirePulse, getById);
router.post("/", requirePulse, validateRequest(clientCreateSchema), create);
router.put("/:id", requirePulse, validateRequest(clientUpdateSchema), update);
router.patch("/:id", requirePulse, validateRequest(clientPatchSchema), patch);
router.delete("/:id", requirePulse, _delete);

module.exports = router;

async function getAll(req, res, next) {
  try {
    const customerId = req.effectiveCustomerId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const clients = await clientService.getAll({
      customerId,
      order: [["createdAt", "DESC"]],
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetAllClients",
      entity: "Client",
      details: { count: Array.isArray(clients) ? clients.length : undefined },
    });
    res.json({ status: "success", data: clients });
  } catch (error) {
    logger.logEvent("error", "Error fetching all clients", {
      action: "GetAllClients",
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
    const client = await clientService.getById({ id, customerId });
    if (client) {
      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "GetClientById",
        entity: "Client",
        entityId: id,
      });
      res.json({ status: "success", data: client });
    } else {
      res.status(404).json({
        status: "not_found",
        reason: "client_not_found",
        message: "Client not found",
      });
    }
  } catch (error) {
    logger.logEvent("error", "Error fetching client by ID", {
      action: "GetClientById",
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
    const client = await clientService.create({ data: req.body, customerId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "CreateClient",
      entity: "Client",
      entityId: client.id,
    });
    res.status(201).json({ status: "success", data: client });
  } catch (error) {
    logger.logEvent("error", "Error creating client", {
      action: "CreateClient",
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
    const client = await clientService.update({
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
      action: "UpdateClient",
      entity: "Client",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: client });
  } catch (error) {
    logger.logEvent("error", "Error updating client", {
      action: "UpdateClient",
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
    const client = await clientService.patch({
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
      action: "PatchClient",
      entity: "Client",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: client });
  } catch (error) {
    logger.logEvent("error", "Error patching client", {
      action: "PatchClient",
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
    await clientService.delete({ id, customerId, userId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DeleteClient",
      entity: "Client",
      entityId: id,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error deleting client", {
      action: "DeleteClient",
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

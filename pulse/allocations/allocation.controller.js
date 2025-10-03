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
const allocationService = require("./allocation.service");
const {
  allocationCreateSchema,
  allocationUpdateSchema,
  allocationPatchSchema,
} = require("./allocation.validator");

router.get("/", requirePulse, getAll);
router.get("/:id", requirePulse, getById);
router.post("/", requirePulse, validateRequest(allocationCreateSchema), create);
router.put(
  "/:id",
  requirePulse,
  validateRequest(allocationUpdateSchema),
  update
);
router.patch(
  "/:id",
  requirePulse,
  validateRequest(allocationPatchSchema),
  patch
);
router.delete("/:id", requirePulse, _delete);

module.exports = router;

async function getAll(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const { budgetLineId } = req.query;
  try {
    const allocations = await allocationService.getAll({
      customerId,
      budgetLineId,
      order: [["createdAt", "DESC"]],
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: budgetLineId ? "GetAllocationsByBudgetLine" : "GetAllAllocations",
      entity: "Allocation",
      details: {
        budgetLineId: budgetLineId || undefined,
        count: Array.isArray(allocations) ? allocations.length : undefined,
      },
    });
    res.json({ status: "success", data: allocations });
  } catch (error) {
    logger.logEvent("error", "Error fetching allocations", {
      action: "GetAllocations",
      userId: req.auth?.id,
      customerId,
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
    const allocation = await allocationService.getById({ id, customerId });
    if (allocation) {
      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "GetAllocationById",
        entity: "Allocation",
        entityId: id,
      });
      res.json({ status: "success", data: allocation });
    } else {
      res.status(404).json({
        status: "not_found",
        reason: "allocation_not_found",
        message: "Allocation not found",
      });
    }
  } catch (error) {
    logger.logEvent("error", "Error fetching allocation by ID", {
      action: "GetAllocationById",
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
    const allocation = await allocationService.create({
      data: req.body,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "CreateAllocation",
      entity: "Allocation",
      entityId: allocation.id,
    });
    res.status(201).json({ status: "success", data: allocation });
  } catch (error) {
    logger.logEvent("error", "Error creating allocation", {
      action: "CreateAllocation",
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
    const allocation = await allocationService.update({
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
      action: "UpdateAllocation",
      entity: "Allocation",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: allocation });
  } catch (error) {
    logger.logEvent("error", "Error updating allocation", {
      action: "UpdateAllocation",
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
    const allocation = await allocationService.patch({
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
      action: "PatchAllocation",
      entity: "Allocation",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: allocation });
  } catch (error) {
    logger.logEvent("error", "Error patching allocation", {
      action: "PatchAllocation",
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
    await allocationService.delete({ id, customerId, userId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DeleteAllocation",
      entity: "Allocation",
      entityId: id,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error deleting allocation", {
      action: "DeleteAllocation",
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

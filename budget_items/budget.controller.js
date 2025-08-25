const auditService = require("../audit/audit.service");
const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const budgetService = require("./budget.service");
const {
  budgetItemCreateSchema,
  budgetItemUpdateSchema,
  budgetItemPatchSchema,
} = require("./budget.validator");

router.get("/", authorise(), getAll);
router.get("/:id", authorise(), getById);
router.post("/", authorise(), validateRequest(budgetItemCreateSchema), create);
router.put(
  "/:id",
  authorise(),
  validateRequest(budgetItemUpdateSchema),
  update
);
router.patch(
  "/:id",
  authorise(),
  validateRequest(budgetItemPatchSchema),
  patch
);
router.delete("/:id", authorise(), _delete);

module.exports = router;

async function getAll(req, res, next) {
  try {
    const customerId = req.auth?.customerId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { engagementId } = req.query;

    const items = engagementId
      ? await budgetService.listByEngagement({
          engagementId,
          customerId,
          order: [["createdAt", "DESC"]],
        })
      : await budgetService.getAll({
          customerId,
          order: [["createdAt", "DESC"]],
        });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: engagementId ? "GetBudgetItemsByEngagement" : "GetAllBudgetItems",
      entity: "BudgetItem",
      details: {
        engagementId: engagementId || undefined,
        count: Array.isArray(items) ? items.length : undefined,
      },
    });
    res.json({ status: "success", data: items });
  } catch (error) {
    logger.logEvent("error", "Error fetching budget items", {
      action: "GetBudgetItems",
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
    const item = await budgetService.getById({ id, customerId });
    if (item) {
      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "GetBudgetItemById",
        entity: "BudgetItem",
        entityId: id,
      });
      res.json({ status: "success", data: item });
    } else {
      res
        .status(404)
        .json({ status: "error", message: "Budget item not found" });
    }
  } catch (error) {
    logger.logEvent("error", "Error fetching budget item by ID", {
      action: "GetBudgetItemById",
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
    const item = await budgetService.create({ data: req.body, customerId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "CreateBudgetItem",
      entity: "BudgetItem",
      entityId: item.id,
    });
    res.status(201).json({ status: "success", data: item });
  } catch (error) {
    logger.logEvent("error", "Error creating budget item", {
      action: "CreateBudgetItem",
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
    const item = await budgetService.update({
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
      action: "UpdateBudgetItem",
      entity: "BudgetItem",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: item });
  } catch (error) {
    logger.logEvent("error", "Error updating budget item", {
      action: "UpdateBudgetItem",
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
    const item = await budgetService.patch({
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
      action: "PatchBudgetItem",
      entity: "BudgetItem",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: item });
  } catch (error) {
    logger.logEvent("error", "Error patching budget item", {
      action: "PatchBudgetItem",
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
    await budgetService.delete({ id, customerId, userId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DeleteBudgetItem",
      entity: "BudgetItem",
      entityId: id,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error deleting budget item", {
      action: "DeleteBudgetItem",
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

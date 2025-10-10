const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const express = require("express");
const validateRequest = require("@/middleware/validate-request");
const authorise = require("@/middleware/authorise");

const requirePulse = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "pulse",
});

// Services & validators for BOTH entities
const budgetService = require("./budget.service"); // unified service (items + budgets)
const {
  budgetItemCreateSchema,
  budgetItemUpdateSchema,
  budgetItemPatchSchema,
  budgetCreateSchema,
  budgetUpdateSchema,
  budgetPatchSchema,
  sectionCreateSchema,
  sectionUpdateSchema,
  sectionPatchSchema,
} = require("./budget.validator");

const router = express.Router();
const items = express.Router();
const budgets = express.Router();
const sections = express.Router();

// Mount sub-routers
router.use("/budget-items", items);
router.use("/budgets", budgets);
router.use("/budget-sections", sections);

// New World: link budget to trackable via POST body
budgets.post("/link", requirePulse, async (req, res, next) => {
  const { trackableId, budgetId } = req.body || {};
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    if (!customerId)
      return res.status(400).json({ message: "Customer ID missing" });
    if (!trackableId || !budgetId) {
      return res.status(400).json({
        status: "error",
        message: "trackableId and budgetId are required",
      });
    }

    const budget = await budgetService.budgets.getById({
      id: budgetId,
      customerId,
    });
    if (!budget)
      return res
        .status(404)
        .json({ status: "error", message: "Budget not found" });
    if (budget.trackableId) {
      return res
        .status(409)
        .json({ status: "error", message: "Budget already linked" });
    }

    const updated = await budgetService.budgets.linkToTrackable({
      id: budgetId,
      trackableId: trackableId,
      customerId,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "LinkBudgetToTrackable",
      entity: "Budget",
      entityId: budgetId,
      details: { trackableId },
    });

    return res.json({ status: "success", data: updated });
  } catch (error) {
    logger.logEvent("error", "Error linking budget to trackable", {
      action: "LinkBudgetToTrackable",
      trackableId,
      budgetId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
});

module.exports = router;

// -------- Budget Items routes (/budget-items) --------
items.get("/", requirePulse, getAllItems);
items.get("/:id", requirePulse, getItemById);
items.post(
  "/",
  requirePulse,
  validateRequest(budgetItemCreateSchema),
  createItem
);
items.put(
  "/:id",
  requirePulse,
  validateRequest(budgetItemUpdateSchema),
  updateItem
);
items.patch(
  "/:id",
  requirePulse,
  validateRequest(budgetItemPatchSchema),
  patchItem
);
items.delete("/:id", requirePulse, deleteItem);

// -------- Budget Sections routes (/budget-sections) --------
sections.patch(
  "/:sectionId",
  requirePulse,
  // Inject auth context for validation/update
  (req, _res, next) => {
    req.body = { ...req.body, updatedBy: req.auth?.id };
    next();
  },
  validateRequest(sectionPatchSchema),
  updateSection
);
sections.put(
  "/:sectionId",
  requirePulse,
  // Inject auth context for validation/update
  (req, _res, next) => {
    req.body = { ...req.body, updatedBy: req.auth?.id };
    next();
  },
  validateRequest(sectionUpdateSchema),
  putSection
);
sections.delete("/:sectionId", requirePulse, deleteSection);

// -------- Budget Sections via /budgets/:id/sections --------
budgets.get("/:id/sections", requirePulse, listSectionsByBudget);
// Resolve active/linked budget for a given trackable
budgets.get("/active", requirePulse, async (req, res, next) => {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const { trackableId } = req.query || {};

  try {
    if (!trackableId) {
      return res
        .status(400)
        .json({ status: "error", message: "trackableId is required" });
    }

    const row = await budgetService.budgets.getActiveByTrackable({
      trackableId,
      customerId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetActiveBudgetByTrackable",
      entity: "Budget",
      details: { trackableId, found: !!row },
    });

    return res.json({ status: "success", data: row });
  } catch (error) {
    logger.logEvent("error", "Error resolving active budget by trackable", {
      action: "GetActiveBudgetByTrackable",
      trackableId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
});
budgets.post(
  "/:id/sections",
  requirePulse,
  // Inject route/auth context so validation sees required fields
  (req, _res, next) => {
    req.body = {
      ...req.body,
      budgetId: req.params.id,
      createdBy: req.auth?.id,
    };
    next();
  },
  validateRequest(sectionCreateSchema),
  createSectionUnderBudget
);

// Link an existing budget to an trackable (used by BudgetBuilder "Link" UI)
router.post(
  "/trackables/:trackableId/link-budget/:budgetId",
  requirePulse,
  async (req, res, next) => {
    const { trackableId, budgetId } = req.params;
    const customerId = req.effectiveCustomerId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    try {
      if (!customerId)
        return res.status(400).json({ message: "Customer ID missing" });

      // Ensure budget exists and is owned by this customer
      const budget = await budgetService.budgets.getById({
        id: budgetId,
        customerId,
      });
      if (!budget) {
        return res
          .status(404)
          .json({ status: "error", message: "Budget not found" });
      }

      // Prevent linking if it's already linked
      if (budget.trackableId) {
        return res.status(409).json({
          status: "error",
          message: "Budget already linked to an trackable",
        });
      }

      // Patch only the trackableId (no full validation like name/version required)
      const updated = await budgetService.budgets.linkToTrackable({
        id: budgetId,
        trackableId,
        customerId,
        userId,
      });

      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "LinkBudgetToTrackable",
        entity: "Budget",
        entityId: budgetId,
        details: { trackableId },
      });

      return res.json({ status: "success", data: updated });
    } catch (error) {
      logger.logEvent("error", "Error linking budget to trackable", {
        action: "LinkBudgetToTrackable",
        trackableId,
        budgetId,
        customerId,
        userId,
        error: error.message,
        statusCode: error.statusCode || 500,
        timestamp: new Date().toISOString(),
      });
      return next(error);
    }
  }
);

// -------- Budgets routes (/budgets) --------
budgets.get("/", requirePulse, getAllBudgets);
budgets.get("/:id", requirePulse, getBudgetById);
budgets.post(
  "/",
  requirePulse,
  validateRequest(budgetCreateSchema),
  createBudget
);
budgets.put(
  "/:id",
  requirePulse,
  validateRequest(budgetUpdateSchema),
  updateBudget
);
budgets.patch(
  "/:id",
  requirePulse,
  validateRequest(budgetPatchSchema),
  patchBudget
);
budgets.delete("/:id", requirePulse, deleteBudget);

async function getAllItems(req, res, next) {
  try {
    const customerId = req.effectiveCustomerId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { trackableId, budgetId } = req.query;

    const items = budgetId
      ? await budgetService.budgetItems.listByBudget({
          budgetId,
          customerId,
          order: [["createdAt", "DESC"]],
        })
      : trackableId
        ? await budgetService.budgetItems.listByTrackable({
            trackableId,
            customerId,
            order: [["createdAt", "DESC"]],
          })
        : await budgetService.budgetItems.getAll({
            customerId,
            order: [["createdAt", "DESC"]],
          });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: budgetId
        ? "GetBudgetItemsByBudget"
        : trackableId
          ? "GetBudgetItemsByTrackable"
          : "GetAllBudgetItems",
      entity: "BudgetItem",
      details: {
        trackableId: trackableId || undefined,
        budgetId: budgetId || undefined,
        count: Array.isArray(items) ? items.length : undefined,
      },
    });
    res.json({ status: "success", data: items });
  } catch (error) {
    logger.logEvent("error", "Error fetching budget items", {
      action: "GetBudgetItems",
      userId: req.auth?.id,
      customerId: req.effectiveCustomerId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getItemById(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const item = await budgetService.budgetItems.getById({ id, customerId });
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

async function createItem(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const item = await budgetService.budgetItems.create({
      data: req.body,
      customerId,
    });
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

async function updateItem(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const item = await budgetService.budgetItems.update({
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

async function patchItem(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const item = await budgetService.budgetItems.patch({
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

async function deleteItem(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    await budgetService.budgetItems.delete({ id, customerId, userId });

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

// ===== Sections Handlers =====
async function listSectionsByBudget(req, res, next) {
  const budgetId = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const rows = await budgetService.budgetSections.listByBudget({
      budgetId,
      customerId,
      order: [
        ["order", "ASC"],
        ["createdAt", "ASC"],
      ],
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetSectionsByBudget",
      entity: "BudgetSection",
      details: {
        budgetId,
        count: Array.isArray(rows) ? rows.length : undefined,
      },
    });
    res.json({ status: "success", data: rows });
  } catch (error) {
    logger.logEvent("error", "Error fetching budget sections", {
      action: "GetSectionsByBudget",
      budgetId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function createSectionUnderBudget(req, res, next) {
  const budgetId = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const payload = {
      ...req.body,
      budgetId,
      customerId,
      createdBy: userId,
    };
    const row = await budgetService.budgetSections.create({
      data: payload,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "CreateBudgetSection",
      entity: "BudgetSection",
      entityId: row.id,
      details: { budgetId },
    });
    res.status(201).json({ status: "success", data: row });
  } catch (error) {
    logger.logEvent("error", "Error creating budget section", {
      action: "CreateBudgetSection",
      budgetId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function updateSection(req, res, next) {
  const sectionId = req.params.sectionId;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const row = await budgetService.budgetSections.update({
      id: sectionId,
      data: { ...req.body, customerId, updatedBy: userId },
      customerId,
      userId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PatchBudgetSection",
      entity: "BudgetSection",
      entityId: sectionId,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: row });
  } catch (error) {
    logger.logEvent("error", "Error updating budget section", {
      action: "PatchBudgetSection",
      id: sectionId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function putSection(req, res, next) {
  const sectionId = req.params.sectionId;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const row = await budgetService.budgetSections.update({
      id: sectionId,
      data: { ...req.body, customerId, updatedBy: userId },
      customerId,
      userId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "UpdateBudgetSection",
      entity: "BudgetSection",
      entityId: sectionId,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: row });
  } catch (error) {
    logger.logEvent("error", "Error putting budget section", {
      action: "UpdateBudgetSection",
      id: sectionId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function deleteSection(req, res, next) {
  const sectionId = req.params.sectionId;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    await budgetService.budgetSections.delete({
      id: sectionId,
      customerId,
      userId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DeleteBudgetSection",
      entity: "BudgetSection",
      entityId: sectionId,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error deleting budget section", {
      action: "DeleteBudgetSection",
      id: sectionId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getAllBudgets(req, res, next) {
  try {
    const customerId = req.effectiveCustomerId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { unlinked, trackableId } = req.query;
    const where = {};
    if (String(unlinked) === "true") where.trackableId = null;
    if (trackableId) where.trackableId = trackableId;

    const rows = await budgetService.budgets.getAll({
      customerId,
      where,
      order: [["createdAt", "DESC"]],
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetBudgets",
      entity: "Budget",
      details: { count: Array.isArray(rows) ? rows.length : undefined },
    });
    res.json({ status: "success", data: rows });
  } catch (error) {
    logger.logEvent("error", "Error fetching budgets", {
      action: "GetBudgets",
      userId: req.auth?.id,
      customerId: req.auth?.customerId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getBudgetById(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const row = await budgetService.budgets.getById({ id, customerId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetBudgetById",
      entity: "Budget",
      entityId: id,
    });
    res.json({ status: "success", data: row });
  } catch (error) {
    logger.logEvent("error", "Error fetching budget by ID", {
      action: "GetBudgetById",
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

async function createBudget(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const row = await budgetService.budgets.create({
      data: req.body,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "CreateBudget",
      entity: "Budget",
      entityId: row.id,
    });
    res.status(201).json({ status: "success", data: row });
  } catch (error) {
    logger.logEvent("error", "Error creating budget", {
      action: "CreateBudget",
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function updateBudget(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const row = await budgetService.budgets.update({
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
      action: "UpdateBudget",
      entity: "Budget",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: row });
  } catch (error) {
    logger.logEvent("error", "Error updating budget", {
      action: "UpdateBudget",
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

async function patchBudget(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const row = await budgetService.budgets.patch({
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
      action: "PatchBudget",
      entity: "Budget",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: row });
  } catch (error) {
    logger.logEvent("error", "Error patching budget", {
      action: "PatchBudget",
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

async function deleteBudget(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    await budgetService.budgets.delete({ id, customerId, userId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "SoftDeleteBudget",
      entity: "Budget",
      entityId: id,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error soft-deleting budget", {
      action: "SoftDeleteBudget",
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

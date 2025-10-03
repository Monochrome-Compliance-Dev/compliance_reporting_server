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
const contributionService = require("./contribution.service");
const {
  contributionCreateSchema,
  contributionUpdateSchema,
  contributionPatchSchema,
} = require("./contribution.validator");

router.get("/", requirePulse, getAll);
router.get("/:id", requirePulse, getById);
router.post(
  "/",
  requirePulse,
  validateRequest(contributionCreateSchema),
  create
);
router.put(
  "/:id",
  requirePulse,
  validateRequest(contributionUpdateSchema),
  update
);
router.patch(
  "/:id",
  requirePulse,
  validateRequest(contributionPatchSchema),
  patch
);
router.delete("/:id", requirePulse, _delete);

module.exports = router;

async function getAll(req, res, next) {
  try {
    const customerId = req.effectiveCustomerId;
    const { budgetLineId, resourceId } = req.query || {};
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const contributions = await contributionService.getAll({
      customerId,
      budgetLineId,
      resourceId,
      order: [["createdAt", "DESC"]],
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetAllContributions",
      entity: "Contribution",
      details: {
        count: Array.isArray(contributions) ? contributions.length : undefined,
        budgetLineId,
        resourceId,
      },
    });
    res.json({ status: "success", data: contributions });
  } catch (error) {
    logger.logEvent("error", "Error fetching all contributions", {
      action: "GetAllContributions",
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
    const contribution = await contributionService.getById({ id, customerId });
    if (contribution) {
      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "GetContributionById",
        entity: "Contribution",
        entityId: id,
      });
      res.json({ status: "success", data: contribution });
    } else {
      res
        .status(404)
        .json({ status: "error", message: "Contribution not found" });
    }
  } catch (error) {
    logger.logEvent("error", "Error fetching contribution by ID", {
      action: "GetContributionById",
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
    const contribution = await contributionService.create({
      data: req.body,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "CreateContribution",
      entity: "Contribution",
      entityId: contribution.id,
      details: { status: contribution.status },
    });
    res.status(201).json({ status: "success", data: contribution });
  } catch (error) {
    logger.logEvent("error", "Error creating contribution", {
      action: "CreateContribution",
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
    const contribution = await contributionService.update({
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
      action: "UpdateContribution",
      entity: "Contribution",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: contribution });
  } catch (error) {
    logger.logEvent("error", "Error updating contribution", {
      action: "UpdateContribution",
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
    if (!customerId)
      return res.status(400).json({ message: "Customer ID missing" });
    const contribution = await contributionService.patch({
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
      action: "PatchContribution",
      entity: "Contribution",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: contribution });
  } catch (error) {
    logger.logEvent("error", "Error patching contribution", {
      action: "PatchContribution",
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
    if (!customerId)
      return res.status(400).json({ message: "Customer ID missing" });
    await contributionService.delete({ id, customerId, userId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DeleteContribution",
      entity: "Contribution",
      entityId: id,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error deleting contribution", {
      action: "DeleteContribution",
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

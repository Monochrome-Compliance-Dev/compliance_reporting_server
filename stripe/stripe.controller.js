const auditService = require("../audit/audit.service");
const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const stripeService = require("./stripe.service");
const {
  stripeUserCreateSchema,
  stripeUserUpdateSchema,
  stripeUserPatchSchema,
} = require("./stripe.validator");

// routes
router.get("/", authorise(), getAll);
router.get("/:id", authorise(), getById);
router.post("/", authorise(), validateRequest(stripeUserCreateSchema), create);
router.put(
  "/:id",
  authorise(),
  validateRequest(stripeUserUpdateSchema),
  update
);
router.patch(
  "/:id",
  authorise(),
  validateRequest(stripeUserPatchSchema),
  patch
);
router.delete("/:id", authorise(), _delete);

module.exports = router;

async function getAll(req, res, next) {
  try {
    const customerId = req.effectiveCustomerId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const stripeUsers = await stripeService.getAll({
      customerId,
      order: [["createdAt", "DESC"]],
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetAllStripeUsers",
      entity: "StripeUser",
      details: {
        count: Array.isArray(stripeUsers) ? stripeUsers.length : undefined,
      },
    });
    res.json({ status: "success", data: stripeUsers });
  } catch (error) {
    logger.logEvent("error", "Error fetching all stripe users", {
      action: "GetAllStripeUsers",
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
    const stripeUser = await stripeService.getById({ id, customerId });
    if (stripeUser) {
      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "GetStripeUserById",
        entity: "StripeUser",
        entityId: id,
      });
      res.json({ status: "success", data: stripeUser });
    } else {
      res
        .status(404)
        .json({ status: "error", message: "Stripe user not found" });
    }
  } catch (error) {
    logger.logEvent("error", "Error fetching stripe user by ID", {
      action: "GetStripeUserById",
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
    const stripeUser = await stripeService.create({
      data: req.body,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "CreateStripeUser",
      entity: "StripeUser",
      entityId: stripeUser.id,
    });
    res.status(201).json({ status: "success", data: stripeUser });
  } catch (error) {
    logger.logEvent("error", "Error creating stripe user", {
      action: "CreateStripeUser",
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
    const stripeUser = await stripeService.update({
      id,
      data: req.body,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "UpdateStripeUser",
      entity: "StripeUser",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: stripeUser });
  } catch (error) {
    logger.logEvent("error", "Error updating stripe user", {
      action: "UpdateStripeUser",
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
    const stripeUser = await stripeService.patch({
      id,
      data: req.body,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PatchStripeUser",
      entity: "StripeUser",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: stripeUser });
  } catch (error) {
    logger.logEvent("error", "Error patching stripe user", {
      action: "PatchStripeUser",
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
    await stripeService.delete({ id, customerId, userId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DeleteStripeUser",
      entity: "StripeUser",
      entityId: id,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error deleting stripe user", {
      action: "DeleteStripeUser",
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

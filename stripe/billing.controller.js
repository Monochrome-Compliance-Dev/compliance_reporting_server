const express = require("express");
const router = express.Router();
const authorise = require("../middleware/authorise");
const validateRequest = require("../middleware/validate-request");
const auditService = require("../audit/audit.service");
const { logger } = require("../helpers/logger");
const billingService = require("./billing.service");
const Joi = require("../middleware/joiSanitizer");

// simple inline validator
const checkoutSchema = Joi.object({
  customerId: Joi.string().length(10).required(),
  userId: Joi.string().length(10).required(),
  seats: Joi.number().integer().min(1).default(1),
  planCode: Joi.string().max(64).default("launch"), // or "standard"
}).required();

router.post(
  "/checkout-session",
  authorise(false),
  validateRequest(checkoutSchema),
  createCheckoutSession
);
router.get("/verify-session", verifySession); // for /welcome
router.post("/portal-session", authorise(), createPortalSession);
router.post(
  "/webhook",
  express.raw({ type: "application/json" }),
  stripeWebhook
); // no authorise()

module.exports = router;

async function createCheckoutSession(req, res, next) {
  const { customerId, userId, seats, planCode } = req.body;
  try {
    const session = await billingService.createCheckoutSession({
      customerId,
      userId,
      seats,
      planCode,
      req,
    });
    await auditService.logEvent({
      action: "CreateCheckoutSession",
      customerId,
      userId,
      ip: req.ip,
      device: req.headers["user-agent"],
      details: { planCode, seats },
    });
    res
      .status(201)
      .json({ status: "success", data: { id: session.id, url: session.url } });
  } catch (error) {
    logger.logEvent("error", "Checkout session error", {
      action: "CreateCheckoutSession",
      customerId,
      userId,
      error: error.message,
    });
    next(error);
  }
}

async function verifySession(req, res, next) {
  try {
    const { session_id } = req.query;
    const summary = await billingService.verifySession({
      sessionId: session_id,
    });
    res.json({ status: "success", data: summary });
  } catch (error) {
    next(error);
  }
}

async function createPortalSession(req, res, next) {
  try {
    const { customerId } = req.auth; // tenant of current user
    const url = await billingService.createPortalSession({
      customerId,
      returnUrl: process.env.BILLING_PORTAL_RETURN_URL,
    });
    res.json({ status: "success", data: { url } });
  } catch (error) {
    next(error);
  }
}

async function stripeWebhook(req, res, next) {
  try {
    await billingService.handleWebhook({
      rawBody: req.body,
      sig: req.headers["stripe-signature"],
    });
    res.status(200).send("ok");
  } catch (error) {
    // must return 400 to stop Stripe retry storm on signature failures
    return res
      .status(error.statusCode || 400)
      .send(`Webhook Error: ${error.message}`);
  }
}

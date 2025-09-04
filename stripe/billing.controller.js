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
router.get("/entitlements", authorise(), getEntitlements);
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

    // Audit with concrete entity + entityId (DB requires entity NOT NULL)
    try {
      await auditService.logEvent({
        action: "CreateCheckoutSession",
        entity: "stripe.checkout_session",
        entityId: session.id,
        customerId,
        userId,
        ip: req.ip,
        device: req.headers["user-agent"],
        details: { planCode, seats },
      });
    } catch (auditErr) {
      logger.logEvent("error", "Failed to create audit event", {
        action: "CreateCheckoutSession",
        customerId,
        userId,
        error: auditErr?.message,
      });
    }

    // Return plain shape the FE expects
    return res.status(201).json({ id: session.id, url: session.url });
  } catch (error) {
    // Attempt to log a failure audit event with a concrete entity
    try {
      await auditService.logEvent({
        action: "CreateCheckoutSessionFailed",
        entity: "stripe.checkout_session",
        entityId: null,
        customerId,
        userId,
        ip: req.ip,
        device: req.headers["user-agent"],
        details: { error: error?.message, planCode, seats },
      });
    } catch (_) {
      // swallow audit failure on error path
    }
    logger.logEvent("error", "Checkout session error", {
      action: "CreateCheckoutSession",
      customerId,
      userId,
      error: error.message,
    });
    return next(error);
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

async function getEntitlements(req, res, next) {
  try {
    const { customerId } = req.auth;
    const data = await billingService.getEntitlements({ customerId });
    return res.json({ status: "success", data });
  } catch (error) {
    next(error);
  }
}

async function createPortalSession(req, res, next) {
  try {
    const { customerId } = req.auth; // tenant of current user

    // Resolve a safe, non-empty return URL
    const fallbackBase =
      process.env.APP_PUBLIC_URL ||
      process.env.FRONTEND_URL ||
      process.env.APP_BASE_URL ||
      "http://localhost:3000";

    const bodyReturnUrl = req.body && req.body.returnUrl;
    const returnUrl =
      bodyReturnUrl && String(bodyReturnUrl).trim()
        ? String(bodyReturnUrl).trim()
        : `${fallbackBase}/welcome`;

    const url = await billingService.createPortalSession({
      customerId,
      returnUrl,
    });
    res.json({ status: "success", data: { url } });
  } catch (error) {
    next(error);
  }
}

async function stripeWebhook(req, res, next) {
  try {
    logger.logEvent("info", "Stripe webhook received", {
      action: "StripeWebhookReceived",
      hasSignature: Boolean(req.headers["stripe-signature"]),
      contentType: req.headers["content-type"],
    });
    const result = await billingService.handleWebhook({
      rawBody: req.body,
      sig: req.headers["stripe-signature"],
    });

    // Audit in the controller (pattern consistency)
    try {
      await auditService.logEvent({
        action: result?.processed
          ? "StripeWebhookHandled"
          : "StripeWebhookDuplicate",
        entity: "stripe.webhook",
        entityId: String(result?.eventId || "").slice(0, 10),
        details: { type: result?.type, reason: result?.reason },
      });
    } catch (e) {
      logger.logEvent("error", "Failed to create audit event", {
        action: "StripeWebhookAudit",
        error: e?.message,
      });
    }

    return res.status(200).send("ok");
  } catch (error) {
    // must return 400 to stop Stripe retry storm on signature failures
    return res
      .status(error.statusCode || 400)
      .send(`Webhook Error: ${error.message}`);
  }
}

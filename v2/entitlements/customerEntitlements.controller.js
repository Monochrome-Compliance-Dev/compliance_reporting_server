const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const validateRequest = require("@/middleware/validate-request");
const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const entitlementsService = require("./customerEntitlements.service");

// Restrict v2 customer entitlements admin to Boss roles
const requireBoss = authorise({
  roles: ["Boss"],
});

const {
  entitlementsUpdateSchema,
} = require("./customerEntitlements.validator");

// Helper to extract a user id from the request (auth shape can vary)
function getUserId(req) {
  return (
    (req.auth && req.auth.id) ||
    (req.user && req.user.id) ||
    (req.user && req.user.sub) ||
    null
  );
}

// GET /api/v2/customers/:customerId/entitlements
router.get("/:customerId/entitlements", requireBoss, async (req, res) => {
  try {
    const { customerId } = req.params;
    const entitlements = await entitlementsService.listByCustomer({
      customerId,
    });
    const userId = getUserId(req);
    const ip = req.ip;
    const device = req.headers["user-agent"];
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetCustomerEntitlementsV2",
      entity: "FeatureEntitlement",
      details: {
        count: Array.isArray(entitlements) ? entitlements.length : undefined,
      },
    });
    return res.json(entitlements);
  } catch (err) {
    const status = err.status || 500;
    const message = err.message || "Failed to list customer entitlements";
    logger.logEvent("error", "Error listing v2 customer entitlements", {
      action: "GetCustomerEntitlementsV2",
      userId: getUserId(req),
      customerId: req.params.customerId,
      error: err.message,
      statusCode: status,
      timestamp: new Date().toISOString(),
    });
    return res.status(status).json({ status: "error", message });
  }
});

// PUT /api/v2/customers/:customerId/entitlements
// Body: { features: [{ feature, enabled }, ...] }
router.put(
  "/:customerId/entitlements",
  requireBoss,
  validateRequest(entitlementsUpdateSchema),
  async (req, res) => {
    try {
      const { customerId } = req.params;
      const userId = getUserId(req);
      const { features } = req.body || {};

      const entitlements = await entitlementsService.setForCustomer({
        customerId,
        features,
        userId,
      });
      const ip = req.ip;
      const device = req.headers["user-agent"];
      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "UpdateCustomerEntitlementsV2",
        entity: "FeatureEntitlement",
        details: {
          updates: Array.isArray(features)
            ? features.map((f) => ({
                feature: f.feature,
                enabled: f.enabled,
              }))
            : undefined,
        },
      });

      return res.json({
        status: "success",
        data: entitlements,
      });
    } catch (err) {
      const status = err.status || 500;
      const message = err.message || "Failed to update customer entitlements";
      logger.logEvent("error", "Error updating v2 customer entitlements", {
        action: "UpdateCustomerEntitlementsV2",
        userId: getUserId(req),
        customerId: req.params.customerId,
        error: err.message,
        statusCode: status,
        timestamp: new Date().toISOString(),
      });
      return res.status(status).json({ status: "error", message });
    }
  }
);

module.exports = router;

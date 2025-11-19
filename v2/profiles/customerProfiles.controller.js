const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const validateRequest = require("@/middleware/validate-request");
const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const profilesService = require("./customerProfiles.service");
const {
  customerProfileCreateSchema,
  customerProfileUpdateSchema,
} = require("./customerProfiles.validator");

const requireBoss = authorise({
  roles: ["Boss"],
});

function getUserId(req) {
  return (
    (req.auth && req.auth.id) ||
    (req.user && req.user.id) ||
    (req.user && req.user.sub) ||
    null
  );
}

// GET /api/v2/customers/:customerId/profiles
router.get("/:customerId/profiles", requireBoss, async (req, res) => {
  try {
    const { customerId } = req.params;
    const profiles = await profilesService.listByCustomer({ customerId });

    const userId = getUserId(req);
    const ip = req.ip;
    const device = req.headers["user-agent"];

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "ListCustomerProfilesV2",
      entity: "CustomerProfile",
      details: {
        count: Array.isArray(profiles) ? profiles.length : undefined,
      },
    });

    return res.json(profiles);
  } catch (err) {
    const status = err.status || 500;
    const message = err.message || "Failed to list customer profiles";

    logger.logEvent("error", "Error listing v2 customer profiles", {
      action: "ListCustomerProfilesV2",
      userId: getUserId(req),
      customerId: req.params.customerId,
      error: message,
      statusCode: status,
      timestamp: new Date().toISOString(),
    });

    return res.status(status).json({ status: "error", message });
  }
});

// POST /api/v2/customers/:customerId/profiles
router.post(
  "/:customerId/profiles",
  requireBoss,
  validateRequest(customerProfileCreateSchema),
  async (req, res) => {
    try {
      const { customerId } = req.params;
      const userId = getUserId(req);

      const profile = await profilesService.createProfile({
        customerId,
        data: req.body,
        userId,
      });

      const ip = req.ip;
      const device = req.headers["user-agent"];

      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "CreateCustomerProfileV2",
        entity: "CustomerProfile",
        entityId: profile.id,
        details: {
          name: profile.name,
          product: profile.product,
        },
      });

      return res.status(201).json({
        status: "success",
        data: profile,
      });
    } catch (err) {
      const status = err.status || 500;
      const message = err.message || "Failed to create customer profile";

      logger.logEvent("error", "Error creating v2 customer profile", {
        action: "CreateCustomerProfileV2",
        userId: getUserId(req),
        customerId: req.params.customerId,
        error: message,
        statusCode: status,
        timestamp: new Date().toISOString(),
      });

      return res.status(status).json({ status: "error", message });
    }
  }
);

// PUT /api/v2/customers/:customerId/profiles/:profileId
router.put(
  "/:customerId/profiles/:profileId",
  requireBoss,
  validateRequest(customerProfileUpdateSchema),
  async (req, res) => {
    try {
      const { customerId, profileId } = req.params;
      const userId = getUserId(req);

      const profile = await profilesService.updateProfile({
        customerId,
        profileId,
        data: req.body,
        userId,
      });

      const ip = req.ip;
      const device = req.headers["user-agent"];

      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "UpdateCustomerProfileV2",
        entity: "CustomerProfile",
        entityId: profile.id,
        details: {
          updates: Object.keys(req.body || {}),
        },
      });

      return res.json({
        status: "success",
        data: profile,
      });
    } catch (err) {
      const status = err.status || 500;
      const message = err.message || "Failed to update customer profile";

      logger.logEvent("error", "Error updating v2 customer profile", {
        action: "UpdateCustomerProfileV2",
        userId: getUserId(req),
        customerId: req.params.customerId,
        error: message,
        statusCode: status,
        timestamp: new Date().toISOString(),
      });

      return res.status(status).json({ status: "error", message });
    }
  }
);

// DELETE /api/v2/customers/:customerId/profiles/:profileId
router.delete(
  "/:customerId/profiles/:profileId",
  requireBoss,
  async (req, res) => {
    try {
      const { customerId, profileId } = req.params;
      const userId = getUserId(req);

      const result = await profilesService.deleteProfile({
        customerId,
        profileId,
        userId,
      });

      const ip = req.ip;
      const device = req.headers["user-agent"];

      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "DeleteCustomerProfileV2",
        entity: "CustomerProfile",
        entityId: profileId,
      });

      return res.json({
        status: "success",
        message: result.message,
      });
    } catch (err) {
      const status = err.status || 500;
      const message = err.message || "Failed to delete customer profile";

      logger.logEvent("error", "Error deleting v2 customer profile", {
        action: "DeleteCustomerProfileV2",
        userId: getUserId(req),
        customerId: req.params.customerId,
        error: message,
        statusCode: status,
        timestamp: new Date().toISOString(),
      });

      return res.status(status).json({ status: "error", message });
    }
  }
);

module.exports = router;

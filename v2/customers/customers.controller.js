const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const validateRequest = require("@/middleware/validate-request");
const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
// Restrict v2 customer admin to Boss roles
const requireBoss = authorise({
  roles: ["Boss"],
});

const {
  customerCreateSchema,
  customerUpdateSchema,
} = require("./customers.validator");

const customerService = require("./customers.service");

// Helper to extract a user id from the request (auth shape can vary)
function getUserId(req) {
  return (
    (req.auth && req.auth.id) ||
    (req.user && req.user.id) ||
    (req.user && req.user.sub) ||
    null
  );
}

// GET /api/v2/customers
// Returns an array of customers
router.get("/", requireBoss, async (req, res) => {
  try {
    const customers = await customerService.listCustomers();
    const userId = getUserId(req);
    const ip = req.ip;
    const device = req.headers["user-agent"];
    await auditService.logEvent({
      customerId: "-R1o0gWoZX",
      userId,
      ip,
      device,
      action: "ListCustomersV2",
      entity: "Customer",
      details: {
        count: Array.isArray(customers) ? customers.length : undefined,
      },
    });
    return res.json(customers);
  } catch (err) {
    const status = err.status || 500;
    const message = err.message || "Failed to list customers";
    logger.logEvent("error", "Error listing v2 customers", {
      action: "ListCustomersV2",
      userId: getUserId(req),
      customerId: "-R1o0gWoZX",
      error: err.message,
      statusCode: status,
      timestamp: new Date().toISOString(),
    });
    return res.status(status).json({ status: "error", message });
  }
});

// POST /api/v2/customers
// Creates a customer and returns { status, data }
router.post(
  "/",
  requireBoss,
  validateRequest(customerCreateSchema),
  async (req, res) => {
    try {
      const userId = getUserId(req);
      const customer = await customerService.createCustomer({
        data: req.body,
        userId,
      });
      const ip = req.ip;
      const device = req.headers["user-agent"];
      await auditService.logEvent({
        customerId: customer.id,
        userId,
        ip,
        device,
        action: "CreateCustomerV2",
        entity: "Customer",
        entityId: customer.id,
        details: {
          businessName: customer.businessName,
          abn: customer.abn,
        },
      });
      return res.status(201).json({
        status: "success",
        data: customer,
      });
    } catch (err) {
      const status = err.status || 500;
      const message = err.message || "Failed to create customer";
      logger.logEvent("error", "Error creating v2 customer", {
        action: "CreateCustomerV2",
        userId: getUserId(req),
        customerId: null,
        error: err.message,
        statusCode: status,
        timestamp: new Date().toISOString(),
      });
      return res.status(status).json({ status: "error", message });
    }
  }
);

// PUT /api/v2/customers/:id
// Updates a customer and returns { status, data }
router.put(
  "/:id",
  requireBoss,
  validateRequest(customerUpdateSchema),
  async (req, res) => {
    try {
      const userId = getUserId(req);
      const { id } = req.params;
      const customer = await customerService.updateCustomer({
        id,
        data: req.body,
        userId,
      });
      const ip = req.ip;
      const device = req.headers["user-agent"];
      await auditService.logEvent({
        customerId: customer.id,
        userId,
        ip,
        device,
        action: "UpdateCustomerV2",
        entity: "Customer",
        entityId: customer.id,
        details: {
          updates: Object.keys(req.body || {}),
        },
      });
      return res.json({
        status: "success",
        data: customer,
      });
    } catch (err) {
      const status = err.status || 500;
      const message = err.message || "Failed to update customer";
      logger.logEvent("error", "Error updating v2 customer", {
        action: "UpdateCustomerV2",
        userId: getUserId(req),
        customerId: req.params.id,
        error: err.message,
        statusCode: status,
        timestamp: new Date().toISOString(),
      });
      return res.status(status).json({ status: "error", message });
    }
  }
);

// DELETE /api/v2/customers/:id
// Deletes a customer and returns { status, message }
router.delete("/:id", requireBoss, async (req, res) => {
  try {
    const { id } = req.params;
    const result = await customerService.deleteCustomer({ id });
    const userId = getUserId(req);
    const ip = req.ip;
    const device = req.headers["user-agent"];
    await auditService.logEvent({
      customerId: id,
      userId,
      ip,
      device,
      action: "DeleteCustomerV2",
      entity: "Customer",
      entityId: id,
    });
    return res.json({
      status: "success",
      message: result.message,
    });
  } catch (err) {
    const status = err.status || 500;
    const message = err.message || "Failed to delete customer";
    logger.logEvent("error", "Error deleting v2 customer", {
      action: "DeleteCustomerV2",
      userId: getUserId(req),
      customerId: req.params.id,
      error: err.message,
      statusCode: status,
      timestamp: new Date().toISOString(),
    });
    return res.status(status).json({ status: "error", message });
  }
});

module.exports = router;

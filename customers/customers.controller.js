const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const customerService = require("./customer.service");
const { logger } = require("../helpers/logger");
const {
  customerSchema,
  customerUpdateSchema,
} = require("./customer.validator");
const { tenantContext } = require("../middleware/tenantContext");

// routes
router.get("/", getAll);
router.get("/:id", authorise(), getById);
router.get(
  "/:id/entitlements",
  authorise(),
  tenantContext({ loadEntitlements: true, enforceMapping: true }),
  getEntitlements
);
router.get(
  "/customers-by-access/:id",
  authorise(),
  tenantContext({ loadEntitlements: true, enforceMapping: true }),
  getCustomersByAccess
);
router.get(
  "/:id/customer-entitlements",
  authorise(),
  tenantContext({ loadEntitlements: true, enforceMapping: true }),
  getCustomerEntitlements
);
router.post("/", authorise(), validateRequest(customerSchema), create);
router.put(
  "/:id",
  authorise(["Admin", "Boss"]),
  validateRequest(customerUpdateSchema),
  update
);
router.delete("/:id", authorise(["Admin", "Boss"]), _delete);

module.exports = router;

function getAll(req, res, next) {
  logger.logEvent("info", "Retrieved all customers", {
    action: "GetAllCustomers",
    ip: req.ip,
    device: req.headers["user-agent"],
  });
  customerService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getById(req, res, next) {
  logger.logEvent("info", "Retrieved customer by ID", {
    action: "GetCustomer",
    customerId: req.auth?.customerId,
    userId: req.auth?.id,
    ip: req.ip,
    device: req.headers["user-agent"],
  });
  customerService
    .getById({ id: req.params.id })
    .then((customer) => (customer ? res.json(customer) : res.sendStatus(404)))
    .catch(next);
}

function getEntitlements(req, res, next) {
  const requestedCustomerId = req.params.id;

  // Use effective tenant resolved by tenantContext. If it doesn't match the path, deny.
  const effectiveId = req.tenantCustomerId || req.auth?.customerId;
  if (requestedCustomerId !== effectiveId) {
    logger.logEvent("warn", "Forbidden entitlements request (mismatch)", {
      action: "GetEntitlements",
      requestedCustomerId,
      effectiveCustomerId: effectiveId,
      userId: req.auth?.id,
      ip: req.ip,
      device: req.headers["user-agent"],
    });
    return res.status(403).json({
      status: "forbidden",
      reason: "no_customer_access",
      message: "You don’t have access to the requested customer.",
    });
  }

  logger.logEvent("info", "Retrieving feature entitlements for customer", {
    action: "GetEntitlements",
    customerId: requestedCustomerId,
    userId: req.auth?.id,
    ip: req.ip,
    device: req.headers["user-agent"],
  });

  customerService
    .getEntitlements({ customerId: requestedCustomerId })
    .then((result) => res.json({ status: "success", data: result }))
    .catch(next);
}

async function getCustomerEntitlements(req, res, next) {
  try {
    const effectiveId = req.tenantCustomerId;
    const entitlements = customerService.getEntitlements({
      customerId: effectiveId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "getCustomerEntitlements",
      entity: "CustomerEntitlements",
      details: {
        count: Array.isArray(entitlements) ? entitlements.length : undefined,
      },
    });
    res.json({ status: "success", data: entitlements });
  } catch (error) {
    logger.logEvent("error", "Error fetching all customer entitlements", {
      action: "getCustomerEntitlements",
      userId: req.auth?.id,
      customerId: effectiveId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

function getCustomersByAccess(req, res, next) {
  const userId = req.params.id;
  const effectiveId = req.tenantCustomerId || req.auth?.customerId;
  logger.logEvent("info", "Retrieving customers by user access", {
    action: "GetCustomersByAccess",
    requestedUserId: userId,
    userId: req.auth?.id,
    ip: req.ip,
    device: req.headers["user-agent"],
  });
  customerService
    .getCustomersByAccess(
      {
        customerId: effectiveId,
      },
      userId
    )
    .then((customers) => res.json(customers))
    .catch(next);
}

function create(req, res, next) {
  logger.logEvent("info", "Creating customer", {
    action: "CreateCustomer",
    customerId: req.auth?.customerId,
    userId: req.auth?.id,
    ip: req.ip,
    device: req.headers["user-agent"],
    payload: req.body,
  });
  customerService
    .create({
      data: req.body,
      userId: req.auth?.id,
    })
    .then((customer) => {
      logger.logEvent("info", "Customer created via API", {
        action: "CreateCustomer",
        customerId: customer.id,
      });
      res.status(201).json({ status: "success", data: customer });
    })
    .catch((error) => {
      logger.logEvent("error", "Error creating customer", {
        action: "CreateCustomer",
        error: error.message,
      });
      next(error); // Pass the error to the global error handler
    });
}

function update(req, res, next) {
  logger.logEvent("info", "Updating customer", {
    action: "UpdateCustomer",
    customerId: req.auth?.customerId,
    userId: req.auth?.id,
    ip: req.ip,
    device: req.headers["user-agent"],
    payload: req.body,
  });
  customerService
    .update({
      id: req.params.id,
      data: req.body,
      userId: req.auth?.id,
    })
    .then((customer) => {
      logger.logEvent("info", "Customer updated via API", {
        action: "UpdateCustomer",
        customerId: req.params.id,
        paymentConfirmed: req.body?.paymentConfirmed,
      });
      res.json({ status: "success", data: customer });
    })
    .catch(next);
}

function _delete(req, res, next) {
  logger.logEvent("info", "Deleting customer", {
    action: "DeleteCustomer",
    customerId: req.auth?.customerId,
    userId: req.auth?.id,
    ip: req.ip,
    device: req.headers["user-agent"],
  });
  customerService
    .delete({ id: req.params.id })
    .then(() => {
      logger.logEvent("warn", "Customer deleted via API", {
        action: "DeleteCustomer",
        customerId: req.params.id,
      });
      res.json({ status: "success", message: "Customer deleted successfully" });
    })
    .catch((error) => {
      logger.logEvent("error", "Error deleting customer", {
        action: "DeleteCustomer",
        error: error.message,
      });
      next(error); // Pass the error to the global error handler
    });
}

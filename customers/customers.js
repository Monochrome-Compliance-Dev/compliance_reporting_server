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

// routes
router.get("/", getAll);
router.get("/:id", authorise(), getById);
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
    .getById(req.params.id)
    .then((customer) => (customer ? res.json(customer) : res.sendStatus(404)))
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
    .create(req.body)
    .then((customer) => {
      logger.logEvent("info", "Customer created via API", {
        action: "CreateCustomer",
        customerId: customer.id,
      });
      res.json(customer);
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
    .update(req.params.id, req.body)
    .then((customer) => {
      logger.logEvent("info", "Customer updated via API", {
        action: "UpdateCustomer",
        customerId: req.params.id,
        paymentConfirmed: req.body?.paymentConfirmed,
      });
      res.json(customer);
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
    .delete(req.params.id)
    .then(() => {
      logger.logEvent("warn", "Customer deleted via API", {
        action: "DeleteCustomer",
        customerId: req.params.id,
      });
      res.json({ message: "Customer deleted successfully" });
    })
    .catch((error) => {
      logger.logEvent("error", "Error deleting customer", {
        action: "DeleteCustomer",
        error: error.message,
      });
      next(error); // Pass the error to the global error handler
    });
}

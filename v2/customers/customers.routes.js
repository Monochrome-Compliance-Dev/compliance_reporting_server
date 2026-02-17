const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const validateRequest = require("@/middleware/validate-request");

const {
  customerCreateSchema,
  customerUpdateSchema,
} = require("./customers.validator");

const customersController = require("./customers.controller");

// Restrict customer admin + "act on behalf of" to Boss only
const requireBoss = authorise({
  roles: ["Boss"],
});

// GET /api/v2/customers
router.get("/", requireBoss, customersController.listCustomers);

// GET /api/v2/customers/access
router.get("/access", requireBoss, customersController.getCustomersByAccess);

// POST /api/v2/customers
router.post(
  "/",
  requireBoss,
  validateRequest(customerCreateSchema),
  customersController.createCustomer,
);

// PUT /api/v2/customers/:id
router.put(
  "/:id",
  requireBoss,
  validateRequest(customerUpdateSchema),
  customersController.updateCustomer,
);

// DELETE /api/v2/customers/:id
router.delete("/:id", requireBoss, customersController.deleteCustomer);

module.exports = router;

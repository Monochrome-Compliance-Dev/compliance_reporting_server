const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const validateController = require("@/v2/ptrs/controllers/validate.ptrs.controller");

const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Validate
router.post("/:id/validate", requirePtrs, validateController.runValidate);
router.get("/:id/validate", requirePtrs, validateController.getValidate);

// Validate summary (aggregated view to drive Validate UI)
router.get(
  "/:id/validate/summary",
  requirePtrs,
  validateController.getValidateSummary
);

module.exports = router;

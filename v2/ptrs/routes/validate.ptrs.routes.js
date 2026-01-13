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

// Stage row exclusion (MVP data quality escape hatch)
router.post(
  "/:id/stage-rows/:stageRowId/exclude",
  requirePtrs,
  validateController.setStageRowExclusion
);

// Fetch a single staged row (for Validate UI inspection)
router.get(
  "/:id/stage-rows/:stageRowId",
  requirePtrs,
  validateController.getStageRow
);

// Validate summary (aggregated view to drive Validate UI)
router.get(
  "/:id/validate/summary",
  requirePtrs,
  validateController.getValidateSummary
);

module.exports = router;

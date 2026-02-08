const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const exclusionsController = require("@/v2/ptrs/controllers/exclusions.ptrs.controller");

// Restrict to users who have the ptrs feature (same as Rules)
const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Exclusions (apply)
router.post(
  "/:id/exclusions/apply",
  requirePtrs,
  exclusionsController.exclusionsApply,
);

// Exclusions (preview)
router.get(
  "/:id/exclusions/preview",
  requirePtrs,
  exclusionsController.exclusionsPreview,
);

// Exclusions (apply)
router.post(
  "/:id/exclusions/apply",
  requirePtrs,
  exclusionsController.exclusionsApply,
);

module.exports = router;

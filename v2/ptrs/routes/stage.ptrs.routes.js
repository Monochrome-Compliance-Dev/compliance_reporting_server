const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const stageController = require("@/v2/ptrs/controllers/stage.ptrs.controller");

// Restrict to users who have the ptrs feature (same as v1)
const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Stage normalized rows for a ptrs
router.post("/:id/stage", requirePtrs, stageController.stagePtrs);

// Preview staged rows (read-only, small page)
router.get("/:id/stage/preview", requirePtrs, stageController.getStagePreview);

module.exports = router;

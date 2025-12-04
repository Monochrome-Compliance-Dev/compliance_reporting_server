const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const tmController = require("@/v2/ptrs/controllers/tablesAndMaps.ptrs.controller");

// Restrict to users who have the ptrs feature (same as v1)
const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Column mapping (get/save)
router.get("/:id/map", requirePtrs, tmController.getMap);
router.post("/:id/map", requirePtrs, tmController.saveMap);
// Build and persist the mapped + joined dataset for this PTRS run
router.post(
  "/:id/map/build-mapped",
  requirePtrs,
  tmController.buildMappedDataset
);

// Peek at staged rows
router.get("/:id/sample", requirePtrs, tmController.getSample);

// Unified headers + examples across main and supporting datasets
// router.get("/:id/unified-sample", requirePtrs, tmController.getUnifiedSample);

module.exports = router;

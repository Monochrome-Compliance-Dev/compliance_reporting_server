const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const ptrsController = require("@/v2/ptrs/controllers/ptrs.controller");

// Restrict to users who have the ptrs feature (same as v1)
const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// v2 PTRS routes
// Create an upload metadata record (returns upload id and metadata)
router.post("/uploads", requirePtrs, ptrsController.createUpload);
router.post("/uploads/:id/import", requirePtrs, ptrsController.importCsv);

// Peek at staged rows
router.get("/uploads/:id/sample", requirePtrs, ptrsController.getSample);

// Column mapping
router.get("/uploads/:id/map", requirePtrs, ptrsController.getMap);
router.post("/uploads/:id/map", requirePtrs, ptrsController.saveMap);

// Preview transformed sample (no mutation)
router.post("/uploads/:id/preview", requirePtrs, ptrsController.preview);

module.exports = router;

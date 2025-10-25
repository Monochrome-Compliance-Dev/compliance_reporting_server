const express = require("express");
const router = express.Router();

const multer = require("multer");
const upload = multer(); // in-memory storage for multipart/form-data

const authorise = require("@/middleware/authorise");
const ptrsController = require("@/v2/ptrs/controllers/ptrs.controller");

// Restrict to users who have the ptrs feature (same as v1)
const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// v2 PTRS routes
// Create a run metadata record (returns run id and metadata)
router.post("/runs", requirePtrs, ptrsController.createUpload);

// Upload the CSV for a run (multipart or text/csv)
router.post(
  "/runs/:id/import",
  requirePtrs,
  upload.single("file"),
  ptrsController.importCsv
);

// Peek at staged rows
router.get("/runs/:id/sample", requirePtrs, ptrsController.getSample);

// Column mapping (get/save)
router.get("/runs/:id/map", requirePtrs, ptrsController.getMap);
router.post("/runs/:id/map", requirePtrs, ptrsController.saveMap);

// Preview transformed sample (no mutation)
router.post("/runs/:id/preview", requirePtrs, ptrsController.preview);

// List runs (optionally filter to those that already have a saved column map)
router.get("/runs", requirePtrs, ptrsController.listRuns);

// Return the generic blueprint, optionally merged with a profile (e.g., ?profileId=veolia)
router.get("/blueprint", requirePtrs, ptrsController.getBlueprint);

module.exports = router;

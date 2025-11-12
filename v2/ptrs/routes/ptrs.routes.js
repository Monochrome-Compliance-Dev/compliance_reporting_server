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
router.post("/runs", requirePtrs, ptrsController.createRun);

// Read a single run by id
router.get("/runs/:id", requirePtrs, ptrsController.getRun);

// Upload the CSV for a run (multipart or text/csv)
router.post(
  "/runs/:id/import",
  requirePtrs,
  upload.single("file"),
  ptrsController.importCsv
);

// Stage normalized rows for a run
router.post("/runs/:id/stage", requirePtrs, ptrsController.stageRun);

// Preview staged rows (read-only, small page)
router.get(
  "/runs/:id/stage/preview",
  requirePtrs,
  ptrsController.getStagePreview
);

// Peek at staged rows
router.get("/runs/:id/sample", requirePtrs, ptrsController.getSample);

// Column mapping (get/save)
router.get("/runs/:id/map", requirePtrs, ptrsController.getMap);
router.post("/runs/:id/map", requirePtrs, ptrsController.saveMap);

// Datasets: upload/list/delete additional files for a run (vendor master, terms, etc.)
router.post(
  "/runs/:id/datasets",
  requirePtrs,
  upload.single("file"),
  ptrsController.addDataset
);
router.get("/runs/:id/datasets", requirePtrs, ptrsController.listDatasets);
router.delete(
  "/runs/:id/datasets/:datasetId",
  requirePtrs,
  ptrsController.removeDataset
);
// Dataset sample (used for per-dataset header examples in FE)
router.get(
  "/datasets/:datasetId/sample",
  requirePtrs,
  ptrsController.getDatasetSample
);

// Unified headers + examples across main and supporting datasets
router.get(
  "/runs/:id/unified-sample",
  requirePtrs,
  ptrsController.getUnifiedSample
);

// Preview transformed sample (no mutation)
router.post("/runs/:id/preview", requirePtrs, ptrsController.preview);

// Rules (preview/apply)
router.get("/runs/:id/rules/preview", requirePtrs, ptrsController.rulesPreview);
router.post("/runs/:id/rules/apply", requirePtrs, ptrsController.rulesApply);
// Rules (get/save) — FE writes rules without resending mappings
router.get("/runs/:id/rules", requirePtrs, ptrsController.getRules);
router.post("/runs/:id/rules", requirePtrs, ptrsController.saveRules);

// List runs (optionally filter to those that already have a saved column map)
router.get("/runs", requirePtrs, ptrsController.listRuns);

// Return the generic blueprint, optionally merged with a profile (e.g., ?profileId=veolia)
router.get("/blueprint", requirePtrs, ptrsController.getBlueprint);

// List all PTRS profiles for a customer

router.get("/profiles", requirePtrs, ptrsController.listProfiles);
// Profiles CRUD
router.post("/profiles", requirePtrs, ptrsController.createProfile);
router.get("/profiles/:id", requirePtrs, ptrsController.getProfile);
router.patch("/profiles/:id", requirePtrs, ptrsController.updateProfile);
router.put("/profiles/:id", requirePtrs, ptrsController.updateProfile);
router.delete("/profiles/:id", requirePtrs, ptrsController.deleteProfile);

module.exports = router;

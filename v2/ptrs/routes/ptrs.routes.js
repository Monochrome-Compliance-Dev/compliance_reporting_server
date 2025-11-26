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

// Return the generic blueprint, optionally merged with a profile (e.g., ?profileId=veolia)
router.get("/blueprint", requirePtrs, ptrsController.getBlueprint);

// List ptrs
router.get("", requirePtrs, ptrsController.listPtrs);

// Create a ptrs metadata record (returns ptrs id and metadata)
router.post("", requirePtrs, ptrsController.createPtrs);

// Read a single ptrs by id
router.get("/:id", requirePtrs, ptrsController.getPtrs);

// Update currentStep when user progresses through steps
router.put("/:id", requirePtrs, ptrsController.updatePtrs);

// Upload the CSV for a ptrs (multipart or text/csv)
router.post(
  "/:id/import",
  requirePtrs,
  upload.single("file"),
  ptrsController.importCsv
);

// // Stage normalized rows for a ptrs
// router.post("/:id/stage", requirePtrs, ptrsController.stagePtrs);

// Preview staged rows (read-only, small page)
router.get("/:id/stage/preview", requirePtrs, ptrsController.getStagePreview);

// Peek at staged rows
router.get("/:id/sample", requirePtrs, ptrsController.getSample);

// Column mapping (get/save)
router.get("/:id/map", requirePtrs, ptrsController.getMap);
router.post("/:id/map", requirePtrs, ptrsController.saveMap);

// Datasets: upload/list/delete additional files for a ptrs (vendor master, terms, etc.)
router.post(
  "/:id/datasets",
  requirePtrs,
  upload.single("file"),
  ptrsController.addDataset
);
router.get("/:id/datasets", requirePtrs, ptrsController.listDatasets);
router.delete(
  "/:id/datasets/:datasetId",
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
router.get("/:id/unified-sample", requirePtrs, ptrsController.getUnifiedSample);

// // Preview transformed sample (no mutation)
// router.post("/:id/preview", requirePtrs, ptrsController.preview);

// // Rules (preview/apply)
// router.get("/:id/rules/preview", requirePtrs, ptrsController.rulesPreview);
// router.post("/:id/rules/apply", requirePtrs, ptrsController.rulesApply);
// // Rules (get/save) — FE writes rules without resending mappings
// router.get("/:id/rules", requirePtrs, ptrsController.getRules);
// router.post("/:id/rules", requirePtrs, ptrsController.saveRules);

// List all PTRS profiles for a customer
// Profiles CRUD
router.get("/profiles", requirePtrs, ptrsController.listProfiles);
// router.post("/profiles", requirePtrs, ptrsController.createProfile);
// router.get("/profiles/:id", requirePtrs, ptrsController.getProfile);
// router.patch("/profiles/:id", requirePtrs, ptrsController.updateProfile);
// router.put("/profiles/:id", requirePtrs, ptrsController.updateProfile);
// router.delete("/profiles/:id", requirePtrs, ptrsController.deleteProfile);

module.exports = router;

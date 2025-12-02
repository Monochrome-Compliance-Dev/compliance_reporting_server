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
router.get("/with-map", requirePtrs, ptrsController.listPtrsWithMap);

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

// // Preview transformed sample (no mutation)
// router.post("/:id/preview", requirePtrs, ptrsController.preview);

// Rules (preview/apply)
router.get("/:id/rules/preview", requirePtrs, ptrsController.rulesPreview);
router.post("/:id/rules/apply", requirePtrs, ptrsController.rulesApply);
// Rules (get/save) — FE writes rules without resending mappings
router.get("/:id/rules", requirePtrs, ptrsController.getRules);
router.post("/:id/rules", requirePtrs, ptrsController.saveRules);

// List all PTRS profiles for a customer
// Profiles CRUD
router.get("/profiles", requirePtrs, ptrsController.listProfiles);
// router.post("/profiles", requirePtrs, ptrsController.createProfile);
// router.get("/profiles/:id", requirePtrs, ptrsController.getProfile);
// router.patch("/profiles/:id", requirePtrs, ptrsController.updateProfile);
// router.put("/profiles/:id", requirePtrs, ptrsController.updateProfile);
// router.delete("/profiles/:id", requirePtrs, ptrsController.deleteProfile);

// New sub routes
const dataRoutes = require("@/v2/ptrs/routes/data.ptrs.routes");
const tablesAndmapsRoutes = require("@/v2/ptrs/routes/tablesAndmaps.ptrs.routes");
const stageRoutes = require("@/v2/ptrs/routes/stage.ptrs.routes");

// --- mount the new slices ---
// datasets (supporting files)
router.use("/", dataRoutes);

// tables + map (mappings, joins, header meta)
router.use("/", tablesAndmapsRoutes);

// staging
router.use("/", stageRoutes);

module.exports = router;

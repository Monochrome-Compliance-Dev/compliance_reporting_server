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
router.post(
  "/runs/:id/import",
  requirePtrs,
  upload.single("file"),
  ptrsController.importCsv
);

// Peek at staged rows
router.get("/runs/:id/sample", requirePtrs, ptrsController.getSample);

// Column mapping
router.get("/runs/:id/map", requirePtrs, ptrsController.getMap);
router.post("/runs/:id/map", requirePtrs, ptrsController.saveMap);

// Preview transformed sample (no mutation)
router.post("/runs/:id/preview", requirePtrs, ptrsController.preview);

module.exports = router;

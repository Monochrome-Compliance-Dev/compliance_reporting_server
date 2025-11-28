const express = require("express");
const router = express.Router();

const multer = require("multer");
const upload = multer(); // in-memory storage for multipart/form-data

const authorise = require("@/middleware/authorise");
const ptrsController = require("@/v2/ptrs/controllers/data.ptrs.controller");

const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

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

module.exports = router;

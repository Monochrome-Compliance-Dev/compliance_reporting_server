const express = require("express");
const router = express.Router();

const {
  uploadCsv,
} = require("@/v2/ptrs/middleware/csv-upload.ptrs.middleware");

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
  uploadCsv.single("file"),
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

module.exports = router;

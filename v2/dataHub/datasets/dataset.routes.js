const express = require("express");
const multer = require("multer");

const authorise = require("@/middleware/authorise");
const datasetController = require("./dataset.controller");

const router = express.Router();

const requireDataHub = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

const upload = multer(); // in-memory storage for multipart/form-data

router.get("/runs/:runId", requireDataHub, datasetController.listDatasets);

router.post(
  "/runs/:runId",
  requireDataHub,
  upload.single("file"),
  datasetController.createDataset,
);

router.get(
  "/runs/:runId/sample",
  requireDataHub,
  datasetController.getDatasetSample,
);

router.get(
  "/runs/:runId/:datasetId",
  requireDataHub,
  datasetController.getDataset,
);

router.delete(
  "/runs/:runId/:datasetId",
  requireDataHub,
  datasetController.deleteDataset,
);

module.exports = router;

const express = require("express");
const multer = require("multer");

const authorise = require("@/middleware/authorise");
const datasetController = require("./dataset.controller");

const router = express.Router();

const requireDataHub = authorise({
  roles: ["Admin", "Boss", "User"],
  features: ["dataHub", "ptrs"],
});

const upload = multer(); // in-memory storage for multipart/form-data

router.get("", requireDataHub, datasetController.listDatasets);

router.post(
  "",
  requireDataHub,
  upload.single("file"),
  datasetController.createDataset,
);

router.get("/:id/sample", requireDataHub, datasetController.getDatasetSample);

router.get("/:id", requireDataHub, datasetController.getDataset);

router.delete("/:id", requireDataHub, datasetController.deleteDataset);

module.exports = router;

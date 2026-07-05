const express = require("express");
const multer = require("multer");

const { createDataController } = require("@/platform/data/data.controller");

function createDataRouter({ PlatformDataDataset } = {}) {
  const router = express.Router();
  const upload = multer({ storage: multer.memoryStorage() });
  const controller = createDataController({ PlatformDataDataset });

  router.post("/datasets", upload.single("file"), controller.createDataset);

  return router;
}

module.exports = {
  createDataRouter,
};

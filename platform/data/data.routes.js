const express = require("express");
const multer = require("multer");

const authorise = require("@/middleware/authorise");
const { createDataController } = require("@/platform/data/data.controller");
const identityService = require("@/platform/identity/identity.service");

function createDataRouter({ PlatformDataDataset } = {}) {
  const router = express.Router();
  const upload = multer({ storage: multer.memoryStorage() });
  const controller = createDataController({ PlatformDataDataset });
  const requirePlatformAccess = authorise({
    roles: ["Admin", "Boss", "User"],
  });

  router.post(
    "/datasets",
    requirePlatformAccess,
    identityService.attachExecutionContext,
    upload.single("file"),
    controller.createDataset,
  );

  return router;
}

module.exports = {
  createDataRouter,
};

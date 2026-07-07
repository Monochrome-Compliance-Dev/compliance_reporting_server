const fs = require("fs");
const os = require("os");
const path = require("path");
const express = require("express");
const multer = require("multer");

const authorise = require("@/middleware/authorise");
const { createDataController } = require("@/platform/data/data.controller");
const identityService = require("@/platform/identity/identity.service");

const DATA_UPLOAD_TEMP_DIRECTORY = path.join(
  os.tmpdir(),
  "mc-platform-data-uploads",
);

function createDataUploadStorage() {
  return multer.diskStorage({
    destination(req, file, callback) {
      fs.mkdirSync(DATA_UPLOAD_TEMP_DIRECTORY, { recursive: true });
      callback(null, DATA_UPLOAD_TEMP_DIRECTORY);
    },
    filename(req, file, callback) {
      const safeOriginalName = path.basename(file.originalname || "upload.csv");
      callback(null, `${Date.now()}-${safeOriginalName}`);
    },
  });
}

function createDataRouter({
  PlatformDataDataset,
  PlatformDataWorkingDataset,
  PlatformDataWorkingDatasetActivity,
} = {}) {
  const router = express.Router();
  const upload = multer({ storage: createDataUploadStorage() });
  const controller = createDataController({
    PlatformDataDataset,
    PlatformDataWorkingDataset,
    PlatformDataWorkingDatasetActivity,
  });
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

  router.post(
    "/working-datasets",
    requirePlatformAccess,
    identityService.attachExecutionContext,
    controller.createWorkingDataset,
  );

  router.post(
    "/working-datasets/:workingDatasetId/edit-lease",
    requirePlatformAccess,
    identityService.attachExecutionContext,
    controller.acquireWorkingDatasetEditLease,
  );

  router.post(
    "/working-datasets/:workingDatasetId/edit-lease/renew",
    requirePlatformAccess,
    identityService.attachExecutionContext,
    controller.renewWorkingDatasetEditLease,
  );

  router.delete(
    "/working-datasets/:workingDatasetId/edit-lease",
    requirePlatformAccess,
    identityService.attachExecutionContext,
    controller.releaseWorkingDatasetEditLease,
  );

  return router;
}

module.exports = {
  createDataRouter,
};

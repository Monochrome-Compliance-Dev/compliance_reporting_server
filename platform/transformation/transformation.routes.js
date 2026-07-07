const express = require("express");
const authorise = require("@/middleware/authorise");

const {
  createTransformationController,
} = require("@/platform/transformation/transformation.controller");
const identityService = require("@/platform/identity/identity.service");

function createTransformationRouter(models = {}) {
  const router = express.Router();
  const controller = createTransformationController(models);

  const requirePlatformAccess = authorise({
    roles: ["Admin", "Boss", "User"],
  });

  router.post(
    "/working-datasets/:workingDatasetId/materialise",
    requirePlatformAccess,
    identityService.attachExecutionContext,
    controller.materialiseWorkingDataset,
  );

  return router;
}

module.exports = {
  createTransformationRouter,
};

const transformationService = require("@/platform/transformation/transformation.service");

function getExecutionContext(req) {
  if (req.executionContext) {
    return req.executionContext;
  }

  return {
    actorId: req.user?.id || req.user?.userId,
    role: req.user?.role,
    customerId: req.user?.customerId,
  };
}

function createTransformationController(models = {}) {
  const { PlatformDataWorkingDataset, PlatformDataWorkingDatasetActivity } =
    models;

  async function acquireWorkingDatasetEditorLease(req, res, next) {
    try {
      const result =
        await transformationService.acquireWorkingDatasetEditorLease({
          executionContext: getExecutionContext(req),
          params: req.params,
          body: req.body,
          PlatformDataWorkingDataset,
        });

      return res.status(200).json(result);
    } catch (error) {
      return next(error);
    }
  }

  async function finaliseWorkingDataset(req, res, next) {
    try {
      const result = await transformationService.finaliseWorkingDataset({
        executionContext: getExecutionContext(req),
        params: req.params,
        body: req.body,
        PlatformDataWorkingDataset,
        PlatformDataWorkingDatasetActivity,
      });

      return res.status(200).json(result);
    } catch (error) {
      return next(error);
    }
  }

  async function renewWorkingDatasetEditorLease(req, res, next) {
    try {
      const result = await transformationService.renewWorkingDatasetEditorLease(
        {
          executionContext: getExecutionContext(req),
          params: req.params,
          body: req.body,
          PlatformDataWorkingDataset,
        },
      );

      return res.status(200).json(result);
    } catch (error) {
      return next(error);
    }
  }

  async function materialiseWorkingDataset(req, res, next) {
    try {
      const result = await transformationService.materialiseWorkingDataset({
        executionContext: getExecutionContext(req),
        params: req.params,
        body: req.body,
        PlatformDataWorkingDataset,
        PlatformDataWorkingDatasetActivity,
      });

      return res.status(200).json(result);
    } catch (error) {
      return next(error);
    }
  }

  return {
    acquireWorkingDatasetEditorLease,
    finaliseWorkingDataset,
    materialiseWorkingDataset,
    renewWorkingDatasetEditorLease,
  };
}

module.exports = {
  createTransformationController,
};

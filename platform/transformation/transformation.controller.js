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
    materialiseWorkingDataset,
  };
}

module.exports = {
  createTransformationController,
};

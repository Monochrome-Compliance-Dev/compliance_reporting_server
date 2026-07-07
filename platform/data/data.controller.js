const dataService = require("@/platform/data/data.service");

function getExecutionContext(req) {
  return req.executionContext;
}

function createDataController({
  PlatformDataDataset,
  PlatformDataWorkingDataset,
  PlatformDataWorkingDatasetActivity,
} = {}) {
  async function createDataset(req, res, next) {
    try {
      const result = await dataService.createDataset({
        executionContext: getExecutionContext(req),
        body: req.body,
        file: req.file,
        PlatformDataDataset,
      });

      return res.status(201).json(result);
    } catch (error) {
      return next(error);
    }
  }

  async function createWorkingDataset(req, res, next) {
    try {
      const result = await dataService.createWorkingDataset({
        executionContext: getExecutionContext(req),
        body: req.body,
        PlatformDataDataset,
        PlatformDataWorkingDataset,
        PlatformDataWorkingDatasetActivity,
      });

      return res.status(201).json(result);
    } catch (error) {
      return next(error);
    }
  }

  async function acquireWorkingDatasetEditLease(req, res, next) {
    try {
      const result = await dataService.acquireWorkingDatasetEditLease({
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

  async function renewWorkingDatasetEditLease(req, res, next) {
    try {
      const result = await dataService.renewWorkingDatasetEditLease({
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

  async function releaseWorkingDatasetEditLease(req, res, next) {
    try {
      const result = await dataService.releaseWorkingDatasetEditLease({
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

  async function finaliseWorkingDataset(req, res, next) {
    try {
      const result = await dataService.finaliseWorkingDataset({
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
    acquireWorkingDatasetEditLease,
    createDataset,
    createWorkingDataset,
    finaliseWorkingDataset,
    releaseWorkingDatasetEditLease,
    renewWorkingDatasetEditLease,
  };
}

module.exports = {
  createDataController,
};

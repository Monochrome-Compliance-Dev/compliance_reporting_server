const dataService = require("@/platform/data/data.service");

function getExecutionContext(req) {
  return req.executionContext;
}

function createDataController({ PlatformDataDataset } = {}) {
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

  return {
    createDataset,
  };
}

module.exports = {
  createDataController,
};

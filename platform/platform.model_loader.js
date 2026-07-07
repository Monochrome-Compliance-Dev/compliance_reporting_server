const PlatformDataDatasetModel = require("@/platform/data/models/platform_data_dataset");
const PlatformDataWorkingDatasetModel = require("@/platform/data/models/platform_data_working_dataset");
const PlatformDataWorkingDatasetActivityModel = require("@/platform/data/models/platform_data_working_dataset_activity");

function loadModel(sequelize, modelName, defineModel) {
  if (sequelize.models?.[modelName]) {
    return sequelize.models[modelName];
  }

  return defineModel(sequelize);
}

function loadPlatformModels(sequelize) {
  if (!sequelize) {
    throw new Error("Sequelize instance is required to load platform models.");
  }

  const PlatformDataDataset = loadModel(
    sequelize,
    "PlatformDataDataset",
    PlatformDataDatasetModel,
  );

  const PlatformDataWorkingDataset = loadModel(
    sequelize,
    "PlatformDataWorkingDataset",
    PlatformDataWorkingDatasetModel,
  );

  const PlatformDataWorkingDatasetActivity = loadModel(
    sequelize,
    "PlatformDataWorkingDatasetActivity",
    PlatformDataWorkingDatasetActivityModel,
  );

  return {
    PlatformDataDataset,
    PlatformDataWorkingDataset,
    PlatformDataWorkingDatasetActivity,
  };
}

module.exports = {
  loadPlatformModels,
};

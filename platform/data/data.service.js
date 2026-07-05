const { getNanoid } = require("@/helpers/nanoid_helper");

const acquisitionService = require("@/platform/data/acquisition.service");
const datasetRepository = require("@/platform/data/dataset.repository");
const datasetService = require("@/platform/data/dataset.service");
const fileStorageService = require("@/platform/data/file-storage.service");

function createError(message, status = 500) {
  const error = new Error(message);
  error.status = status;
  return error;
}

function hasValue(value) {
  return value !== undefined && value !== null && value !== "";
}

function requireValue(value, message) {
  if (!hasValue(value)) {
    throw createError(message);
  }
}

async function createDataset({
  executionContext,
  body,
  file,
  PlatformDataDataset,
}) {
  requireValue(
    executionContext,
    "executionContext is required for Data dataset creation.",
  );
  requireValue(
    PlatformDataDataset,
    "PlatformDataDataset model is required for Data dataset creation.",
  );

  const command = acquisitionService.buildDatasetCreationCommand({
    executionContext,
    body,
    file,
  });

  const datasetId = getNanoid(10);

  const storageResult = await fileStorageService.storeDatasetFile({
    customerId: command.customerId,
    datasetId,
    buffer: command.file.buffer,
  });

  const datasetResponse = datasetService.createImmutableDatasetFromCommand({
    command,
    datasetId,
    storageResult,
  });

  const persistedDataset = await datasetRepository.createDatasetRecord({
    PlatformDataDataset,
    dataset: {
      ...datasetResponse.dataset,
      actor: command.actor,
    },
  });

  return {
    success: true,
    dataset: persistedDataset,
  };
}

module.exports = {
  createDataset,
};

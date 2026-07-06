const { getNanoid } = require("@/helpers/nanoid_helper");
const { scanFileBuffer } = require("@/middleware/virus-scan");

const acquisitionService = require("@/platform/data/acquisition.service");
const auditService = require("@/platform/audit/audit.service");
const datasetRepository = require("@/platform/data/dataset.repository");
const datasetService = require("@/platform/data/dataset.service");
const fileStorageService = require("@/platform/data/file-storage.service");
const securityService = require("@/platform/security/security.service");

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

  let securityObservation;

  try {
    securityObservation = securityService.enforceDataDatasetCreation({
      datasetId,
      actor: command.actor,
      customerId: command.customerId,
    });
  } catch (error) {
    await auditService.recordDataDatasetAudit({
      datasetId,
      outcome: "denied",
      actor: command.actor,
      securityObservation: error.securityObservation,
      error,
    });

    throw error;
  }

  try {
    await scanFileBuffer(command.file.buffer, command.file.originalFileName);
  } catch (error) {
    await auditService.recordDataDatasetAudit({
      datasetId,
      outcome: "denied",
      actor: command.actor,
      securityObservation,
      error,
    });

    throw error;
  }

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

  await auditService.recordDataDatasetAudit({
    datasetId,
    outcome: "success",
    actor: command.actor,
    securityObservation,
  });

  return {
    success: true,
    dataset: persistedDataset,
  };
}

module.exports = {
  createDataset,
};

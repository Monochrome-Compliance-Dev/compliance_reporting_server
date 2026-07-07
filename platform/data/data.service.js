const { getNanoid } = require("@/helpers/nanoid_helper");
const { scanFile } = require("@/middleware/virus-scan");

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
    await scanFile(command.file.path, command.file.originalFileName);
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
    sourceFilePath: command.file.path,
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

async function createWorkingDataset({
  executionContext,
  body,
  PlatformDataDataset,
  PlatformDataWorkingDataset,
  PlatformDataWorkingDatasetActivity,
}) {
  requireValue(
    executionContext,
    "executionContext is required for working dataset creation.",
  );
  requireValue(
    PlatformDataDataset,
    "PlatformDataDataset model is required for working dataset creation.",
  );
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required for working dataset creation.",
  );
  requireValue(
    PlatformDataWorkingDatasetActivity,
    "PlatformDataWorkingDatasetActivity model is required for working dataset creation.",
  );
  requireValue(body, "body is required for working dataset creation.");
  requireValue(
    body.sourceDatasetId,
    "sourceDatasetId is required for working dataset creation.",
  );
  requireValue(
    body.profileId,
    "profileId is required for working dataset creation.",
  );
  requireValue(
    body.workingName,
    "workingName is required for working dataset creation.",
  );
  requireValue(
    executionContext.customerId,
    "customerId is required for working dataset creation.",
  );

  const workingDatasetId = getNanoid(10);
  const actor = {
    id: executionContext.actorId,
    role: executionContext.role,
    customerId: executionContext.customerId,
  };

  let securityObservation;

  try {
    securityObservation = securityService.enforceDataDatasetCreation({
      datasetId: workingDatasetId,
      actor,
      customerId: executionContext.customerId,
    });
  } catch (error) {
    await auditService.recordDataDatasetAudit({
      datasetId: workingDatasetId,
      outcome: "denied",
      actor,
      securityObservation: error.securityObservation,
      error,
    });

    throw error;
  }

  const sourceDataset = await datasetRepository.getDatasetRecordById({
    PlatformDataDataset,
    datasetId: body.sourceDatasetId,
    customerId: executionContext.customerId,
    profileId: body.profileId,
  });

  if (sourceDataset.status !== "available") {
    throw createError(
      "source dataset is not available for working data creation.",
      409,
    );
  }

  const workingDataset = await datasetRepository.createWorkingDatasetRecord({
    PlatformDataWorkingDataset,
    sourceDataset,
    workingDataset: {
      workingDatasetId,
      sourceDatasetId: sourceDataset.datasetId,
      customerId: executionContext.customerId,
      profileId: body.profileId,
      workingName: body.workingName,
      currentStepNumber: body.currentStepNumber || 1,
      actor,
    },
  });

  const activity = await datasetRepository.createWorkingDatasetActivityRecord({
    PlatformDataWorkingDatasetActivity,
    activity: {
      customerId: executionContext.customerId,
      profileId: body.profileId,
      workingDatasetId,
      activityType: "working_dataset_created",
      stepNumber: workingDataset.currentStepNumber,
      summary: `Created working dataset ${body.workingName}`,
      details: {
        sourceDatasetId: sourceDataset.datasetId,
      },
      relatedCapability: "data",
      relatedRecordId: workingDatasetId,
      actor,
    },
  });

  await auditService.recordDataDatasetAudit({
    datasetId: workingDatasetId,
    outcome: "success",
    actor,
    securityObservation,
  });

  return {
    success: true,
    workingDataset,
    activity,
  };
}

module.exports = {
  createDataset,
  createWorkingDataset,
};

const csvInspectionService = require("@/platform/data/csv-inspection.service");
const datasetContract = require("@/platform/data/dataset.contract");

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

function validateStorageResult(storageResult) {
  requireValue(
    storageResult,
    "storage result is required for dataset creation.",
  );
  requireValue(
    storageResult.storedFileName,
    "storedFileName is required for dataset creation.",
  );
  requireValue(
    storageResult.storagePath,
    "storagePath is required for dataset creation.",
  );
}

function createImmutableDatasetFromCommand({
  command,
  datasetId,
  storageResult,
}) {
  requireValue(command, "dataset creation command is required.");
  requireValue(datasetId, "datasetId is required for dataset creation.");
  validateStorageResult(storageResult);
  requireValue(
    command.customerId,
    "customerId is required for dataset creation.",
  );
  requireValue(
    command.profileId,
    "profileId is required for dataset creation.",
  );
  requireValue(
    command.datasetType,
    "datasetType is required for dataset creation.",
  );
  requireValue(
    command.sourceType,
    "sourceType is required for dataset creation.",
  );
  requireValue(
    command.sourceName,
    "sourceName is required for dataset creation.",
  );
  requireValue(command.file, "file is required for dataset creation.");
  requireValue(
    command.file.originalFileName,
    "originalFileName is required for dataset creation.",
  );
  requireValue(
    command.file.mimeType,
    "mimeType is required for dataset creation.",
  );
  requireValue(
    command.file.fileSize,
    "fileSize is required for dataset creation.",
  );
  requireValue(
    command.file.path,
    "file path is required for dataset creation.",
  );

  const csvInspection = csvInspectionService.inspectCsvFile(command.file.path);

  const dataset = {
    datasetId,
    customerId: command.customerId,
    profileId: command.profileId,
    datasetType: command.datasetType,
    sourceType: command.sourceType,
    sourceName: command.sourceName,
    originalFileName: command.file.originalFileName,
    storedFileName: storageResult.storedFileName,
    storagePath: storageResult.storagePath,
    mimeType: command.file.mimeType,
    fileSize: command.file.fileSize,
    headers: csvInspection.headers,
    headersCount: csvInspection.headersCount,
    rowsCount: csvInspection.rowsCount,
    status: "available",
    isImmutable: true,
    createdAt: new Date().toISOString(),
  };

  return datasetContract.buildDatasetCreationResponse(dataset);
}

module.exports = {
  createImmutableDatasetFromCommand,
};

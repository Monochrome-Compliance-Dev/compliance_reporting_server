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

function normaliseDatasetRecord(record) {
  const plainRecord =
    typeof record?.get === "function" ? record.get({ plain: true }) : record;

  requireValue(plainRecord, "dataset record is required.");

  return {
    datasetId: plainRecord.id,
    customerId: plainRecord.customerId,
    profileId: plainRecord.profileId,
    datasetType: plainRecord.datasetType,
    sourceType: plainRecord.sourceType,
    sourceName: plainRecord.sourceName,
    originalFileName: plainRecord.originalFileName,
    storedFileName: plainRecord.storedFileName,
    storagePath: plainRecord.storagePath,
    mimeType: plainRecord.mimeType,
    fileSize: plainRecord.fileSize,
    headers: plainRecord.headers,
    headersCount: plainRecord.headersCount,
    rowsCount: plainRecord.rowsCount,
    status: plainRecord.status,
    isImmutable: true,
    createdAt:
      plainRecord.createdAt instanceof Date
        ? plainRecord.createdAt.toISOString()
        : plainRecord.createdAt,
  };
}

async function createDatasetRecord({ PlatformDataDataset, dataset }) {
  requireValue(PlatformDataDataset, "PlatformDataDataset model is required.");
  requireValue(dataset, "dataset is required for persistence.");
  requireValue(dataset.datasetId, "datasetId is required for persistence.");
  requireValue(dataset.customerId, "customerId is required for persistence.");
  requireValue(dataset.profileId, "profileId is required for persistence.");
  requireValue(dataset.datasetType, "datasetType is required for persistence.");
  requireValue(dataset.sourceType, "sourceType is required for persistence.");
  requireValue(dataset.sourceName, "sourceName is required for persistence.");
  requireValue(
    dataset.originalFileName,
    "originalFileName is required for persistence.",
  );
  requireValue(dataset.storagePath, "storagePath is required for persistence.");
  requireValue(dataset.mimeType, "mimeType is required for persistence.");
  requireValue(dataset.fileSize, "fileSize is required for persistence.");
  requireValue(dataset.headers, "headers is required for persistence.");
  requireValue(
    dataset.headersCount,
    "headersCount is required for persistence.",
  );
  requireValue(dataset.rowsCount, "rowsCount is required for persistence.");
  requireValue(dataset.status, "status is required for persistence.");
  requireValue(dataset.actor?.id, "actor id is required for persistence.");

  const storedFileName = `${dataset.datasetId}.csv`;

  const record = await PlatformDataDataset.create({
    id: dataset.datasetId,
    customerId: dataset.customerId,
    profileId: dataset.profileId,
    datasetType: dataset.datasetType,
    sourceType: dataset.sourceType,
    sourceName: dataset.sourceName,
    originalFileName: dataset.originalFileName,
    storedFileName,
    storagePath: dataset.storagePath,
    mimeType: dataset.mimeType,
    fileSize: dataset.fileSize,
    headers: dataset.headers,
    headersCount: dataset.headersCount,
    rowsCount: dataset.rowsCount,
    status: dataset.status,
    detectedCoverage: dataset.detectedCoverage || {},
    meta: {
      ...(dataset.meta || {}),
      headers: dataset.headers,
      rowsCount: dataset.rowsCount,
      uploadedAt: dataset.createdAt,
    },
    uploadedBy: dataset.actor.id,
    createdBy: dataset.actor.id,
    updatedBy: dataset.actor.id,
  });

  return normaliseDatasetRecord(record);
}

module.exports = {
  createDatasetRecord,
  normaliseDatasetRecord,
};

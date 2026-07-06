const { withCustomerTransaction } = require("@/helpers/customerTransaction");

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

function normaliseInteger(value, fieldName) {
  const numberValue = Number(value);

  if (!Number.isInteger(numberValue) || numberValue < 0) {
    throw createError(`${fieldName} must be a non-negative integer.`);
  }

  return numberValue;
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
    fileSize: normaliseInteger(plainRecord.fileSize, "fileSize"),
    headers: plainRecord.headers,
    headersCount: normaliseInteger(plainRecord.headersCount, "headersCount"),
    rowsCount: normaliseInteger(plainRecord.rowsCount, "rowsCount"),
    status: plainRecord.status,
    isImmutable: true,
    createdAt:
      plainRecord.createdAt instanceof Date
        ? plainRecord.createdAt.toISOString()
        : plainRecord.createdAt,
  };
}

function normaliseWorkingDatasetRecord(record) {
  const plainRecord =
    typeof record?.get === "function" ? record.get({ plain: true }) : record;

  requireValue(plainRecord, "working dataset record is required.");

  const lineage = plainRecord.meta?.lineage;

  requireValue(lineage, "working dataset lineage is required.");
  requireValue(
    lineage.sourceDatasetId,
    "working dataset sourceDatasetId lineage is required.",
  );

  return {
    workingDatasetId: plainRecord.id,
    sourceDatasetId: lineage.sourceDatasetId,
    customerId: plainRecord.customerId,
    profileId: plainRecord.profileId,
    workingName: plainRecord.sourceName,
    datasetType: plainRecord.datasetType,
    sourceType: plainRecord.sourceType,
    headers: plainRecord.headers,
    headersCount: normaliseInteger(plainRecord.headersCount, "headersCount"),
    rowsCount: normaliseInteger(plainRecord.rowsCount, "rowsCount"),
    status: plainRecord.status,
    lineage,
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

  const payload = {
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
  };

  return withCustomerTransaction(dataset.customerId, async (transaction) => {
    const record = await PlatformDataDataset.create(payload, { transaction });
    return normaliseDatasetRecord(record);
  });
}

async function getDatasetRecordById({
  PlatformDataDataset,
  datasetId,
  customerId,
  profileId,
}) {
  requireValue(PlatformDataDataset, "PlatformDataDataset model is required.");
  requireValue(datasetId, "datasetId is required.");
  requireValue(customerId, "customerId is required.");
  requireValue(profileId, "profileId is required.");

  return withCustomerTransaction(customerId, async (transaction) => {
    const record = await PlatformDataDataset.findOne({
      where: {
        id: datasetId,
        customerId,
        profileId,
      },
      transaction,
    });

    if (!record) {
      throw createError(
        "source dataset was not found for working data creation.",
        404,
      );
    }

    return normaliseDatasetRecord(record);
  });
}

async function createWorkingDatasetRecord({
  PlatformDataDataset,
  sourceDataset,
  workingDataset,
}) {
  requireValue(PlatformDataDataset, "PlatformDataDataset model is required.");
  requireValue(sourceDataset, "sourceDataset is required.");
  requireValue(workingDataset, "workingDataset is required.");
  requireValue(
    workingDataset.workingDatasetId,
    "workingDatasetId is required for persistence.",
  );
  requireValue(
    workingDataset.sourceDatasetId,
    "sourceDatasetId is required for persistence.",
  );
  requireValue(
    workingDataset.customerId,
    "customerId is required for persistence.",
  );
  requireValue(
    workingDataset.profileId,
    "profileId is required for persistence.",
  );
  requireValue(
    workingDataset.workingName,
    "workingName is required for persistence.",
  );
  requireValue(
    workingDataset.actor?.id,
    "actor id is required for persistence.",
  );

  const lineage = {
    sourceDatasetId: workingDataset.sourceDatasetId,
    createdFrom: "immutable_dataset",
  };

  const payload = {
    id: workingDataset.workingDatasetId,
    customerId: workingDataset.customerId,
    profileId: workingDataset.profileId,
    datasetType: sourceDataset.datasetType,
    sourceType: "working_copy",
    sourceName: workingDataset.workingName,
    originalFileName: sourceDataset.originalFileName,
    storedFileName: sourceDataset.storedFileName,
    storagePath: sourceDataset.storagePath,
    mimeType: sourceDataset.mimeType,
    fileSize: sourceDataset.fileSize,
    headers: sourceDataset.headers,
    headersCount: sourceDataset.headersCount,
    rowsCount: sourceDataset.rowsCount,
    status: "available",
    detectedCoverage: {},
    meta: {
      lineage,
      sourceDatasetId: workingDataset.sourceDatasetId,
      workingName: workingDataset.workingName,
    },
    uploadedBy: workingDataset.actor.id,
    createdBy: workingDataset.actor.id,
    updatedBy: workingDataset.actor.id,
  };

  return withCustomerTransaction(
    workingDataset.customerId,
    async (transaction) => {
      const record = await PlatformDataDataset.create(payload, { transaction });
      return normaliseWorkingDatasetRecord(record);
    },
  );
}

module.exports = {
  createDatasetRecord,
  createWorkingDatasetRecord,
  getDatasetRecordById,
  normaliseDatasetRecord,
  normaliseWorkingDatasetRecord,
};

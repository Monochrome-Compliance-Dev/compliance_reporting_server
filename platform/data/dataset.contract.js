const REQUIRED_DATASET_FIELDS = [
  "datasetId",
  "customerId",
  "profileId",
  "datasetType",
  "sourceType",
  "sourceName",
  "originalFileName",
  "storagePath",
  "mimeType",
  "fileSize",
  "headers",
  "headersCount",
  "rowsCount",
  "status",
  "isImmutable",
  "createdAt",
];

const FORBIDDEN_DATASET_FIELDS = [
  "ptrsId",
  "mapping",
  "linking",
  "exclusions",
  "reportingPeriod",
  "sbi",
  "metrics",
  "dashboard",
];

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

function assertNoForbiddenFields(dataset) {
  const forbiddenField = FORBIDDEN_DATASET_FIELDS.find((field) =>
    Object.prototype.hasOwnProperty.call(dataset, field),
  );

  if (forbiddenField) {
    throw createError(`${forbiddenField} is not allowed in dataset contract.`);
  }
}

function assertHeaders(dataset) {
  if (!Array.isArray(dataset.headers)) {
    throw createError("headers must be an array.");
  }

  if (dataset.headers.length === 0) {
    throw createError("headers must include at least one header.");
  }

  if (dataset.headersCount !== dataset.headers.length) {
    throw createError("headersCount must match headers length.");
  }
}

function assertRowsCount(dataset) {
  if (!Number.isInteger(dataset.rowsCount) || dataset.rowsCount < 0) {
    throw createError("rowsCount must be a non-negative integer.");
  }
}

function assertFileSize(dataset) {
  if (!Number.isInteger(dataset.fileSize) || dataset.fileSize < 0) {
    throw createError("fileSize must be a non-negative integer.");
  }
}

function assertImmutable(dataset) {
  if (dataset.isImmutable !== true) {
    throw createError("isImmutable must be true for dataset contract.");
  }
}

function validateDataset(dataset) {
  requireValue(dataset, "dataset is required.");
  assertNoForbiddenFields(dataset);

  REQUIRED_DATASET_FIELDS.forEach((field) => {
    requireValue(dataset[field], `${field} is required for dataset contract.`);
  });

  assertHeaders(dataset);
  assertRowsCount(dataset);
  assertFileSize(dataset);
  assertImmutable(dataset);
}

function buildDatasetCreationResponse(dataset) {
  validateDataset(dataset);

  return {
    success: true,
    dataset: {
      datasetId: dataset.datasetId,
      customerId: dataset.customerId,
      profileId: dataset.profileId,
      datasetType: dataset.datasetType,
      sourceType: dataset.sourceType,
      sourceName: dataset.sourceName,
      originalFileName: dataset.originalFileName,
      storagePath: dataset.storagePath,
      mimeType: dataset.mimeType,
      fileSize: dataset.fileSize,
      headers: dataset.headers,
      headersCount: dataset.headersCount,
      rowsCount: dataset.rowsCount,
      status: dataset.status,
      isImmutable: dataset.isImmutable,
      createdAt: dataset.createdAt,
    },
  };
}

module.exports = {
  buildDatasetCreationResponse,
};

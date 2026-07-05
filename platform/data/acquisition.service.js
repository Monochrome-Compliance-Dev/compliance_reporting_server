const ALLOWED_DATASET_TYPES = ["payment", "invoice", "supplier", "other"];
const ALLOWED_CSV_MIME_TYPES = [
  "text/csv",
  "application/csv",
  "application/vnd.ms-excel",
];

function createError(message, status = 400) {
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

function normaliseString(value) {
  return String(value || "").trim();
}

function normaliseBody(body = {}) {
  return {
    sourceName: normaliseString(body.sourceName),
    datasetType: normaliseString(body.datasetType),
    profileId: normaliseString(body.profileId),
  };
}

function assertExecutionContext(executionContext) {
  requireValue(
    executionContext?.actorId,
    "actorId is required for dataset creation.",
  );
  requireValue(
    executionContext?.customerId,
    "customerId is required for dataset creation.",
  );
}

function assertDatasetType(datasetType) {
  requireValue(datasetType, "datasetType is required for dataset creation.");

  if (!ALLOWED_DATASET_TYPES.includes(datasetType)) {
    throw createError("datasetType is not supported for dataset creation.");
  }
}

function hasCsvExtension(fileName) {
  return normaliseString(fileName).toLowerCase().endsWith(".csv");
}

function assertCsvFile(file) {
  requireValue(file, "file is required for dataset creation.");
  requireValue(
    file.originalname,
    "file.originalname is required for dataset creation.",
  );
  requireValue(
    file.mimetype,
    "file.mimetype is required for dataset creation.",
  );
  requireValue(file.size, "file.size is required for dataset creation.");

  if (!Number.isInteger(file.size) || file.size <= 0) {
    throw createError("file.size must be a positive integer.");
  }

  const mimetype = normaliseString(file.mimetype).toLowerCase();

  if (
    !ALLOWED_CSV_MIME_TYPES.includes(mimetype) &&
    !hasCsvExtension(file.originalname)
  ) {
    throw createError("file must be a CSV file.");
  }
}

function buildDatasetCreationCommand({ executionContext, body, file }) {
  assertExecutionContext(executionContext);

  const normalisedBody = normaliseBody(body);

  requireValue(
    normalisedBody.sourceName,
    "sourceName is required for dataset creation.",
  );
  assertDatasetType(normalisedBody.datasetType);
  requireValue(
    normalisedBody.profileId,
    "profileId is required for dataset creation.",
  );
  assertCsvFile(file);

  return {
    actor: {
      id: executionContext.actorId,
      role: executionContext.role || null,
      customerId: executionContext.customerId,
    },
    customerId: executionContext.customerId,
    profileId: normalisedBody.profileId,
    datasetType: normalisedBody.datasetType,
    sourceType: "csv_upload",
    sourceName: normalisedBody.sourceName,
    file: {
      originalFileName: file.originalname,
      mimeType: normaliseString(file.mimetype).toLowerCase(),
      fileSize: file.size,
      buffer: file.buffer,
    },
  };
}

module.exports = {
  buildDatasetCreationCommand,
};

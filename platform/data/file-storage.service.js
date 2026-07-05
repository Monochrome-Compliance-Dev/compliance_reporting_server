const fs = require("fs/promises");
const path = require("path");

const DEFAULT_STORAGE_ROOT = path.resolve(process.cwd(), "storage", "data_hub");

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

function requireBuffer(buffer) {
  if (!Buffer.isBuffer(buffer)) {
    throw createError("file buffer is required for dataset storage.");
  }

  if (buffer.length === 0) {
    throw createError("file buffer must not be empty for dataset storage.");
  }
}

function buildStoredFileName(datasetId) {
  requireValue(datasetId, "datasetId is required for stored file name.");

  return `${datasetId}.csv`;
}

function buildDatasetStorageDirectory({
  storageRoot = DEFAULT_STORAGE_ROOT,
  customerId,
}) {
  requireValue(storageRoot, "storageRoot is required for dataset storage.");
  requireValue(customerId, "customerId is required for dataset storage.");

  return path.join(storageRoot, customerId, "datasets");
}

function buildDatasetStoragePath({
  storageRoot = DEFAULT_STORAGE_ROOT,
  customerId,
  datasetId,
}) {
  const storageDirectory = buildDatasetStorageDirectory({
    storageRoot,
    customerId,
  });

  return path.join(storageDirectory, buildStoredFileName(datasetId));
}

async function storeDatasetFile({
  storageRoot = DEFAULT_STORAGE_ROOT,
  customerId,
  datasetId,
  buffer,
}) {
  requireValue(customerId, "customerId is required for dataset storage.");
  requireValue(datasetId, "datasetId is required for dataset storage.");
  requireBuffer(buffer);

  const storageDirectory = buildDatasetStorageDirectory({
    storageRoot,
    customerId,
  });
  const storedFileName = buildStoredFileName(datasetId);
  const storagePath = path.join(storageDirectory, storedFileName);

  await fs.mkdir(storageDirectory, { recursive: true });
  await fs.writeFile(storagePath, buffer, { flag: "wx" });

  return {
    storedFileName,
    storagePath,
  };
}

module.exports = {
  buildDatasetStorageDirectory,
  buildDatasetStoragePath,
  buildStoredFileName,
  storeDatasetFile,
};

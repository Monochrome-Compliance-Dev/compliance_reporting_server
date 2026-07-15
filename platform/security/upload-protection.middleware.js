const path = require("path");
const multer = require("multer");
const { logger } = require("@/helpers/logger");

const DEFAULT_MAX_FILE_SIZE_BYTES = 100 * 1024 * 1024;
const DEFAULT_MAX_FIELD_COUNT = 10;

const ALLOWED_CSV_MIME_TYPES = [
  "text/csv",
  "application/csv",
  "application/vnd.ms-excel",
];

function createUploadError(message, code, status = 400) {
  const error = new Error(message);

  error.code = code;
  error.status = status;

  return error;
}

function hasCsvExtension(fileName) {
  return path.extname(String(fileName || "")).toLowerCase() === ".csv";
}

function isAllowedCsvMimeType(mimeType) {
  return ALLOWED_CSV_MIME_TYPES.includes(
    String(mimeType || "")
      .trim()
      .toLowerCase(),
  );
}

function csvFileFilter(request, file, callback) {
  if (!hasCsvExtension(file.originalname)) {
    callback(
      createUploadError(
        "Only CSV files may be uploaded.",
        "invalid_upload_type",
      ),
    );
    return;
  }

  if (!isAllowedCsvMimeType(file.mimetype)) {
    callback(
      createUploadError(
        "Only CSV files may be uploaded.",
        "invalid_upload_type",
      ),
    );
    return;
  }

  callback(null, true);
}

function createUploadProtection({
  storage,
  maxFileSizeBytes = DEFAULT_MAX_FILE_SIZE_BYTES,
  maxFieldCount = DEFAULT_MAX_FIELD_COUNT,
} = {}) {
  if (!storage) {
    throw new Error("storage is required for upload protection.");
  }

  if (!Number.isInteger(maxFileSizeBytes) || maxFileSizeBytes <= 0) {
    throw new Error(
      "maxFileSizeBytes must be a positive integer for upload protection.",
    );
  }

  if (!Number.isInteger(maxFieldCount) || maxFieldCount <= 0) {
    throw new Error(
      "maxFieldCount must be a positive integer for upload protection.",
    );
  }

  return multer({
    storage,
    fileFilter: csvFileFilter,
    limits: {
      fileSize: maxFileSizeBytes,
      files: 1,
      fields: maxFieldCount,
    },
  }).single("file");
}

function uploadProtectionErrorHandler(error, request, response, next) {
  if (!error) {
    next();
    return;
  }

  let status;
  let code;
  let message;
  let reason;

  if (error instanceof multer.MulterError) {
    reason = error.code;

    if (error.code === "LIMIT_FILE_SIZE") {
      status = 413;
      code = "UPLOAD_TOO_LARGE";
      message = "Uploaded file exceeds the permitted size.";
    } else {
      status = 400;
      code = "INVALID_UPLOAD";
      message = "Upload does not meet the permitted file requirements.";
    }
  } else if (error.code === "invalid_upload_type") {
    status = error.status || 400;
    code = "INVALID_UPLOAD_TYPE";
    message = error.message;
    reason = error.code;
  } else {
    next(error);
    return;
  }

  logger.logEvent("warn", "Upload rejected", {
    action: "UploadRejected",
    requestId: request.id || null,
    method: request.method,
    path: request.originalUrl,
    fileName: request.file?.originalname || null,
    contentType: request.headers["content-type"] || null,
    contentLength: request.headers["content-length"] || null,
    reason,
  });

  response.status(status).json({
    status: "error",
    code,
    message,
    requestId: request.id || null,
  });
}

module.exports = {
  createUploadProtection,
  uploadProtectionErrorHandler,
};

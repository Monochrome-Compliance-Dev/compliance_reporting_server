const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const multer = require("multer");

const TEMP_UPLOAD_DIR = path.resolve(
  process.cwd(),
  "storage",
  "ptrs_uploads",
  "tmp",
);

fs.mkdirSync(TEMP_UPLOAD_DIR, { recursive: true });

const storage = multer.diskStorage({
  destination: (_request, _file, callback) => {
    callback(null, TEMP_UPLOAD_DIR);
  },
  filename: (_request, file, callback) => {
    const extension = path.extname(file.originalname || "").toLowerCase();
    callback(null, `${Date.now()}-${crypto.randomUUID()}${extension}`);
  },
});

function csvFileFilter(_request, file, callback) {
  const fileName = String(file.originalname || "").toLowerCase();
  const mimeType = String(file.mimetype || "").toLowerCase();
  const isCsv = fileName.endsWith(".csv") || mimeType.includes("csv");

  if (!isCsv) {
    const error = new Error("CSV files only — please export as CSV and retry");
    error.statusCode = 400;
    callback(error);
    return;
  }

  callback(null, true);
}

const uploadCsv = multer({ storage, fileFilter: csvFileFilter });

async function cleanupUploadedFile(file) {
  if (!file?.path) return;
  try {
    await fs.promises.unlink(file.path);
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
}

module.exports = {
  TEMP_UPLOAD_DIR,
  cleanupUploadedFile,
  uploadCsv,
};

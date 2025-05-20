const multer = require("multer");
const path = require("path");
const fs = require("fs");
const { logger } = require("../helpers/logger");

// Set temp directory
const uploadDir = path.join(__dirname, "../tmpUploads");

// Ensure the uploads directory exists
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

// Configure multer disk storage
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, uploadDir);
    logger.logEvent("info", "File destination resolved", {
      action: "UploadFile",
      uploadDir,
    });
  },
  filename: function (req, file, cb) {
    const uniqueSuffix = Date.now() + "-" + Math.round(Math.random() * 1e9);
    const safeName = file.originalname.replace(/[^a-zA-Z0-9.-]/g, "_");
    logger.logEvent("info", "Generated filename for upload", {
      action: "UploadFile",
      originalName: file.originalname,
      safeName,
      savedAs: uniqueSuffix + "-" + safeName,
    });
    cb(null, uniqueSuffix + "-" + safeName);
  },
});

const upload = multer({
  storage,
  limits: { fileSize: 2 * 1024 * 1024 }, // 2MB limit
});

module.exports = upload;

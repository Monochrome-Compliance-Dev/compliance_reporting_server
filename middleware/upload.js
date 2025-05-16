const NodeClam = require("clamscan");
const multer = require("multer");
const path = require("path");
const fs = require("fs");

// Ensure the uploads directory exists
const uploadDir = path.join(__dirname, "../uploads");
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

// Initialize antivirus scanner
const ClamScan = new NodeClam().init({
  removeInfected: true,
  quarantineInfected: false,
  scanLog: null,
  debugMode: false,
  fileList: null,
  scanRecursively: false,
  clamscan: {
    path: "/opt/homebrew/bin/clamscan",
    db: null,
    scanArchives: true,
    active: true,
  },
  preference: "clamscan",
});

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, uploadDir); // Use the ensured uploads directory
  },
  filename: (req, file, cb) => {
    const name = path.basename(
      file.originalname,
      path.extname(file.originalname)
    );
    const ext = path.extname(file.originalname);
    const safeName = name.replace(/[^a-z0-9_\-]/gi, "_");
    cb(null, `${safeName}-${Date.now()}${ext}`);
  },
});

const fileFilter = async (req, file, cb) => {
  console.info(`Upload attempt: ${file.originalname} (${file.mimetype})`);

  const allowedTypes = [
    "application/pdf",
    "text/csv",
    "application/vnd.ms-excel",
  ];
  const dangerousExts = [".exe", ".sh", ".bat", ".cmd", ".js"];
  const ext = path.extname(file.originalname).toLowerCase();

  if (!allowedTypes.includes(file.mimetype) || dangerousExts.includes(ext)) {
    return cb(
      new Error("Invalid file type. Only PDF and CSV files are allowed."),
      false
    );
  }

  const filePath = file.path;

  // Antivirus scan
  try {
    const clamscan = await ClamScan;
    const { isInfected } = await clamscan.isInfected(filePath);
    if (isInfected) {
      console.warn(`File rejected by antivirus: ${file.originalname}`);
      return cb(new Error("File failed antivirus scan."), false);
    }
    console.info(`File passed antivirus scan: ${file.originalname}`);
  } catch (err) {
    return cb(new Error("Antivirus scan failed."), false);
  }

  // Check for embedded scripts in CSV
  if (ext === ".csv") {
    try {
      const content = fs.readFileSync(filePath, "utf8").toLowerCase();
      if (
        content.includes("<script") ||
        content.includes("javascript:") ||
        content.includes("onerror=")
      ) {
        console.warn(`Embedded script detected in CSV: ${file.originalname}`);
        return cb(
          new Error("CSV file contains potentially dangerous scripts."),
          false
        );
      }
    } catch (err) {
      return cb(new Error("Unable to scan CSV content."), false);
    }
  }

  cb(null, true);
};

const upload = multer({
  storage,
  fileFilter,
  limits: { fileSize: 2 * 1024 * 1024 }, // 2MB limit
});

module.exports = upload;

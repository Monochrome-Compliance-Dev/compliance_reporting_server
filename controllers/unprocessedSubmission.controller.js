const { sendEmail } = require("../helpers/send-email");
const { scanFile } = require("../middleware/virus-scan");
const UnprocessedSubmission = require("./unprocessedSubmission.model");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const { logger } = require("../helpers/logger");

// Simple disk storage for now
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    const uploadDir = path.join(__dirname, "../uploads/fire_sale");
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    cb(null, uploadDir);
  },
  filename: function (req, file, cb) {
    const timestamp = new Date()
      .toISOString()
      .split(".")[0]
      .replaceAll(":", "-");
    const safeName = file.originalname.replace(/[^a-z0-9.\-_]/gi, "_");
    cb(null, `${timestamp}-${safeName}`);
  },
});

const upload = multer({
  storage,
  fileFilter: function (req, file, cb) {
    if (file.mimetype !== "text/csv") {
      return cb(new Error("Only CSV files are allowed"));
    }
    cb(null, true);
  },
});

// Note: virusScanMiddleware could be included here in future
const handleUnprocessedSubmission = async (req) => {
  try {
    console.log("Received file:", req.file);
    console.log("Body fields:", req.body);

    const { email, contactName, contactPhone, businessName, abn, fileType } =
      req.body;
    if (!email || !contactName || !req.file) {
      throw new Error("Missing required fields or file");
    }

    // Virus scan immediately after file is uploaded, before DB write
    const ext = path.extname(req.file.path).toLowerCase();
    await scanFile(req.file.path, ext, req.file.originalname);

    const submission = await UnprocessedSubmission.create({
      email,
      contactName,
      contactPhone,
      businessName,
      abn,
      fileType,
      filePath: req.file.path,
    });

    logger.logEvent("info", "UnprocessedSubmissionReceived", {
      id: submission.id,
      email,
      file: req.file.filename,
      fileType,
    });

    await sendEmail({
      to: "contact@monochrome-compliance.com",
      subject: `New PTR Submission from ${email}`,
      html: `
        <p><strong>Email:</strong> ${email}</p>
        <p><strong>Name:</strong> ${contactName}</p>
        <p><strong>Phone:</strong> ${contactPhone || "N/A"}</p>
        <p><strong>Business Name:</strong> ${businessName || "N/A"}</p>
        <p><strong>ABN:</strong> ${abn || "N/A"}</p>
        <p><strong>File:</strong> ${req.file.filename}</p>
        <p><strong>File Type:</strong> ${fileType || "N/A"}</p>
      `,
    });
  } catch (err) {
    logger.logEvent("error", "SubmissionError", { error: err.message });
    throw err;
  }
};

module.exports = {
  upload,
  handleUnprocessedSubmission,
};

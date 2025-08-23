const express = require("express");
const router = express.Router();
const multer = require("multer");
const path = require("path");
const fs = require("fs");

const fileService = require("./file.service");
const { scanFile } = require("../middleware/virus-scan");
const auditService = require("../audit/audit.service");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const { fileCreateSchema, fileQuerySchema } = require("./file.validator");
const { emitSocketEvent } = require("../helpers/socketHelper");

// Setup multer storage
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, "tmpUploads");
  },
  filename: function (req, file, cb) {
    const uniqueSuffix = Date.now() + "-" + Math.round(Math.random() * 1e9);
    cb(null, uniqueSuffix + "-" + file.originalname);
  },
});
const upload = multer({ storage: storage });

router.post("/", authorise(), upload.single("file"), createFile);
router.get("/", authorise(), validateRequest(fileQuerySchema), getFiles);
router.get("/:id", authorise(), getFileById);
router.delete("/:id", authorise(), deleteFile);

async function createFile(req, res, next) {
  try {
    const io = req.app.get("socketio");
    const { customerId, id: userId } = req.auth;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { indicatorId, metricId } = req.body;
    const fileMeta = req.file;

    // 🔄 Emit socket updates using structured { type, stage, payload } under "statusUpdate"
    console.log("Emitting scanStarted:", {
      userId,
      stage: "scanning",
      message: `Scanning ${fileMeta.originalname}...`,
    });

    emitSocketEvent(io, { userId }, "statusUpdate", {
      type: "fileUpload",
      stage: "scanning",
      payload: { message: `Scanning ${fileMeta.originalname}...` },
    });

    // Immediately scan the file in tmpUploads
    await scanFile(
      fileMeta.path,
      path.extname(fileMeta.originalname),
      fileMeta.originalname
    );

    // If scan passed, move to final location
    const category = req.body.category || "general";
    const uploadFolder = path.join("uploads", category);

    if (!fs.existsSync(uploadFolder)) {
      fs.mkdirSync(uploadFolder, { recursive: true });
    }

    const finalPath = path.join(uploadFolder, fileMeta.filename);
    fs.renameSync(fileMeta.path, finalPath);

    const fileData = {
      path: finalPath,
      filename: fileMeta.originalname,
      mimeType: fileMeta.mimetype,
      fileSize: fileMeta.size,
      storagePath: "/" + finalPath.replace(/\\/g, "/"),
      indicatorId,
      metricId,
    };

    const file = await fileService.createFile(fileData, customerId, userId);

    console.log("Emitting scanSuccess:", {
      userId,
      stage: "success",
      message: `${fileMeta.originalname} passed virus scan and saved.`,
    });

    emitSocketEvent(io, { userId }, "statusUpdate", {
      type: "fileUpload",
      stage: "success",
      payload: {
        message: `${fileMeta.originalname} passed virus scan and saved.`,
      },
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "CreateFile",
      entity: "File",
      entityId: file.id,
      details: fileData,
    });

    res.status(201).json(file);
  } catch (err) {
    const io = req.app.get("socketio");
    const { customerId, id: userId } = req.auth || {};
    if (customerId && userId) {
      console.log("Emitting scanFailed:", {
        userId,
        stage: "failed",
        message: `Failed to upload ${req.file?.originalname || "file"}.`,
      });
      emitSocketEvent(io, { userId }, "statusUpdate", {
        type: "fileUpload",
        stage: "failed",
        payload: {
          message: `Failed to upload ${req.file?.originalname || "file"}.`,
        },
      });
    }
    next(err);
  }
}

async function getFileById(req, res, next) {
  try {
    const { customerId, id: userId } = req.auth;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { id } = req.params;

    const file = await fileService.getFileById(id, customerId);

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetFile",
      entity: "File",
      entityId: id,
    });

    res.json(file);
  } catch (err) {
    next(err);
  }
}

async function getFiles(req, res, next) {
  try {
    const { customerId, id: userId } = req.auth;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { indicatorId, metricId } = req.query;

    const files = await fileService.getFilesByIndicatorOrMetric(
      { indicatorId, metricId },
      customerId
    );

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetFiles",
      entity: "File",
      details: { count: files.length, indicatorId, metricId },
    });

    res.json(files);
  } catch (err) {
    next(err);
  }
}

async function deleteFile(req, res, next) {
  try {
    const { customerId, id: userId } = req.auth;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { id } = req.params;

    await fileService.deleteFile(id, customerId);

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DeleteFile",
      entity: "File",
      entityId: id,
    });

    res.status(204).send();
  } catch (err) {
    next(err);
  }
}

module.exports = router;

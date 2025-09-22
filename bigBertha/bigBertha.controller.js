const express = require("express");
const router = express.Router();
const auditService = require("../audit/audit.service");
const { logger } = require("../helpers/logger");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const bigBerthaService = require("./bigBertha.service");
const { processCsvJob } = require("./bigBertha.worker");
const multer = require("multer");
const upload = multer({ dest: process.env.TMP_UPLOAD_DIR || "tmpUploads" });
const fs = require("node:fs");
const path = require("node:path");

const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

function decodeHtmlEntities(str) {
  if (!str || typeof str !== "string") return str;
  return str
    .replace(/&#x2F;/g, "/")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

const {
  ingestStartSchema,
  jobIdParamSchema,
  ptrsIdParamSchema,
  pageQuerySchema,
  uploadLocalSchema,
} = require("./bigBertha.validator");

router.post(
  "/ingest/start",
  requirePtrs,
  validateRequest(ingestStartSchema),
  ingest
);
router.get(
  "/ingest/:jobId",
  requirePtrs,
  validateRequest(jobIdParamSchema, "params"),
  getJob
);
router.get(
  "/ptrs/:ptrsId/rows",
  requirePtrs,
  validateRequest(ptrsIdParamSchema, "params"),
  validateRequest(pageQuerySchema, "query"),
  getPtrsRows
);
router.get(
  "/ptrs/:ptrsId/errors",
  requirePtrs,
  validateRequest(ptrsIdParamSchema, "params"),
  validateRequest(pageQuerySchema, "query"),
  getPtrsErrors
);
router.post(
  "/uploads/local",
  requirePtrs,
  validateRequest(uploadLocalSchema, "query"),
  upload.single("file"),
  uploadsLocalRoute
); // placeholder, actual handler is in uploadsLocalRoute()

async function ingest(req, res) {
  try {
    let {
      filePath,
      customerId,
      ptrsId,
      originalName,
      sizeBytes,
      format,
      selectedHeaders,
      columnMap,
    } = req.body;
    filePath = decodeHtmlEntities(filePath);
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const job = await bigBerthaService.startIngest({
      filePath,
      customerId,
      ptrsId,
      originalName,
      sizeBytes,
      format,
      userId,
      selectedHeaders,
      columnMap,
    });

    const exists = fs.existsSync(filePath);
    logger.logEvent("info", "Ingest debug: checking file existence", {
      filePath,
      exists,
    });
    if (!exists) {
      return res
        .status(400)
        .json({ ok: false, error: `File not found at path: ${filePath}` });
    }

    // Fire-and-forget the worker (errors are logged by the worker path)
    processCsvJob({
      jobId: job.id,
      filePath,
      customerId,
      ptrsId,
      selectedHeaders,
      columnMap,
    }).catch(() => {});

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "StartIngestJob",
      entity: "IngestJob",
      details: { jobId: job.id, ptrsId },
    });
    res.json({ ok: true, jobId: job.id });
  } catch (error) {
    logger.logEvent("error", "Error starting ingest job", {
      action: "StartIngestJob",
      userId: req.auth?.id,
      customerId: req.auth?.customerId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    const status = error.status || 500;
    const message = error.message || "Failed to start ingest job";
    res.status(status).json({ ok: false, error: message });
  }
}

async function getJob(req, res) {
  try {
    const jobId = req.params.jobId;
    const customerId = req.auth?.customerId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const job = await bigBerthaService.getIngestJob({ id: jobId, customerId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetIngestJob",
      entity: "IngestJob",
      entityId: jobId,
      details: { jobId },
    });
    res.json({ ok: true, job });
  } catch (error) {
    logger.logEvent("error", "Error fetching ingest job", {
      action: "GetIngestJob",
      userId: req.auth?.id,
      customerId: req.auth?.customerId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    const status = error.status || 500;
    const message = error.message || "Failed to fetch ingest job";
    res.status(status).json({ ok: false, error: message });
  }
}

async function getPtrsRows(req, res) {
  try {
    const ptrsId = req.params.ptrsId;
    const customerId = req.auth?.customerId;
    const limit = req.query.limit ? parseInt(req.query.limit, 10) : undefined;
    const cursor = req.query.cursor || undefined;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const result = await bigBerthaService.listPtrsRows({
      ptrsId,
      customerId,
      limit,
      cursor,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetPtrsRows",
      entity: "PTRS",
      entityId: ptrsId,
      details: { ptrsId, limit, cursor },
    });
    res.json({ ok: true, ...result });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS rows", {
      action: "GetPtrsRows",
      userId: req.auth?.id,
      customerId: req.auth?.customerId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    const status = error.status || 500;
    const message = error.message || "Failed to fetch PTRS rows";
    res.status(status).json({ ok: false, error: message });
  }
}

async function getPtrsErrors(req, res) {
  try {
    const ptrsId = req.params.ptrsId;
    const customerId = req.auth?.customerId;
    const limit = req.query.limit ? parseInt(req.query.limit, 10) : undefined;
    const cursor = req.query.cursor || undefined;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const result = await bigBerthaService.listPtrsErrors({
      ptrsId,
      customerId,
      limit,
      cursor,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetPtrsErrors",
      entity: "PTRS",
      entityId: ptrsId,
      details: { ptrsId, limit, cursor },
    });
    res.json({ ok: true, ...result });
  } catch (error) {
    logger.logEvent("error", "Error fetching PTRS errors", {
      action: "GetPtrsErrors",
      userId: req.auth?.id,
      customerId: req.auth?.customerId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    const status = error.status || 500;
    const message = error.message || "Failed to fetch PTRS errors";
    res.status(status).json({ ok: false, error: message });
  }
}

async function uploadsLocalRoute(req, res) {
  try {
    const ptrsId = req.query.ptrsId;
    if (!ptrsId) {
      return res.status(400).json({ ok: false, error: "ptrsId is required" });
    }
    if (!req.file) {
      return res.status(400).json({ ok: false, error: "No file uploaded" });
    }
    const customerId = req.auth?.customerId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    // Persist Big Bertha uploads under uploads/ptrs_big_bertha_uploads by default
    const uploadDir =
      process.env.LOCAL_UPLOAD_DIR ||
      path.join("uploads", "ptrs_big_bertha_uploads");
    const fullUploadDir = path.resolve(uploadDir);
    logger.logEvent("info", "Upload debug: resolved upload dir", {
      uploadDir,
      fullUploadDir,
    });
    if (!fs.existsSync(fullUploadDir)) {
      fs.mkdirSync(fullUploadDir, { recursive: true });
    }
    const tempPath = req.file.path;
    const base = path.basename(req.file.originalname || "upload.csv");
    const safe = base.replace(/[^a-zA-Z0-9._-]/g, "_");

    let finalName = safe;
    let targetPath = path.join(fullUploadDir, finalName);
    logger.logEvent("info", "Upload debug: targetPath", { targetPath });

    try {
      if (fs.existsSync(targetPath)) {
        const { nanoid } = await import("nanoid");
        finalName = `${Date.now()}_${nanoid()}_${safe}`;
        targetPath = path.join(fullUploadDir, finalName);
        logger.logEvent("info", "Upload debug: targetPath", { targetPath });
      }
      fs.renameSync(tempPath, targetPath);
      const existsAfterMove = fs.existsSync(targetPath);
      logger.logEvent("info", "Upload debug: file moved", {
        targetPath,
        existsAfterMove,
      });
    } catch (moveErr) {
      try {
        if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath);
      } catch {}
      throw moveErr;
    }

    const fileStat = fs.statSync(targetPath);
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "UploadsLocal",
      entity: "Upload",
      details: { ptrsId, filePath: targetPath, sizeBytes: fileStat.size },
    });
    res.json({
      ok: true,
      filePath: targetPath,
      originalName: finalName,
      sizeBytes: fileStat.size,
      format: "csv",
    });
  } catch (error) {
    logger.logEvent("error", "Error uploading local file", {
      action: "UploadsLocal",
      userId: req.auth?.id,
      customerId: req.auth?.customerId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    const status = error.status || 500;
    const message = error.message || "Failed to upload local file";
    res.status(status).json({ ok: false, error: message });
  }
}

module.exports = router;

const auditService = require("../audit/audit.service");
const express = require("express");
const router = express.Router();
const multer = require("multer");
const upload = multer({ dest: "tmpUploads/" });
const authorise = require("../middleware/authorise");
const tcpService = require("./tcp.service");
const { tcpBulkImportSchema, tcpSchema } = require("./tcp.validator");
const { logger } = require("../helpers/logger");
const fs = require("fs");
const path = require("path");
const { scanFile } = require("../middleware/virus-scan");
const ptrsService = require("../ptrs/ptrs.service");
const csv = require("csv-parser");
const { processTcpMetrics } = require("../utils/calcs/processTcpMetrics");
const { withAudit } = require("../audit/audit.service");
const Joi = require("joi");

// routes
router.get("/", authorise(), getAll);
router.get("/ptrs/:ptrsId", authorise(), getAllByPtrsId);
router.get("/tcp/:id", authorise(), getTcpByPtrsId);
router.get("/:id", authorise(), getById);
router.patch("/bulk-patch", authorise(), bulkPatchUpdate);
router.patch("/:id", authorise(), patchRecord);
router.put("/", authorise(), bulkUpdate);
router.put("/partial", authorise(), partialUpdate);
router.post("/", authorise(), bulkCreate);
router.put("/sbi/:id", authorise(), sbiUpdate);
router.delete("/:id", authorise(), _delete);
router.get("/missing-isSb", authorise(), checkMissingIsSb);
router.put("/submit-final", authorise(), submitFinalPtrs);
router.get("/download-summary", authorise(), downloadSummaryPtrs);
router.post("/upload", authorise(), upload.single("file"), uploadFile);
router.get("/errors/:id", authorise(), getErrorsByPtrsId);
router.put("/recalculate/:id", authorise(), recalculateMetrics);

module.exports = router;

// Fetch all TCPs
async function getAll(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const data = await tcpService.getAll(clientId);
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetAllTcp",
      entity: "Tcp",
      details: { count: Array.isArray(data) ? data.length : undefined },
    });
    res.status(200).json({ status: "success", data: data || [] });
  } catch (error) {
    logger.logEvent("error", "Error fetching all TCPs", {
      action: "GetAllTcp",
      userId,
      clientId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// Fetch all TCPs by ptrsId
async function getAllByPtrsId(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.ptrsId;
  try {
    const data = await tcpService.getByPtrsId({ ptrsId, clientId });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetTcpByPtrsId",
      entity: "Tcp",
      details: { ptrsId, count: Array.isArray(data) ? data.length : undefined },
    });
    res
      .status(200)
      .json({ status: "success", data: Array.isArray(data) ? data : [] });
  } catch (error) {
    logger.logEvent("error", "Error fetching TCPs by ptrsId", {
      action: "GetTcpByPtrsId",
      ptrsId,
      userId,
      clientId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// Fetch a single TCP by id
async function getById(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const id = req.params.id;
  try {
    const data = await tcpService.getById({ id, clientId });
    if (!data) {
      return res.status(404).json({ status: "error", message: "Not found" });
    }
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetTcpById",
      entity: "Tcp",
      entityId: id,
    });
    res.status(200).json({ status: "success", data });
  } catch (error) {
    logger.logEvent("error", "Error fetching TCP by ID", {
      action: "GetTcpById",
      id,
      clientId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// Fetch TCP by ptrsId (legacy)
async function getTcpByPtrsId(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const id = req.params.id; // legacy param name for ptrsId
  try {
    const data = await tcpService.getByPtrsId({ ptrsId: id, clientId });
    if (!data || (Array.isArray(data) && data.length === 0)) {
      return res.status(404).json({ status: "error", message: "Not found" });
    }
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetTcpByPtrsIdLegacy",
      entity: "Tcp",
      details: {
        ptrsId: id,
        count: Array.isArray(data) ? data.length : undefined,
      },
    });
    res
      .status(200)
      .json({ status: "success", data: Array.isArray(data) ? data : [] });
  } catch (error) {
    logger.logEvent("error", "Error fetching TCPs by legacy ptrsId route", {
      action: "GetTcpByPtrsIdLegacy",
      ptrsId: id,
      userId,
      clientId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// PATCH a single TCP record
async function patchRecord(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const id = req.params.id;
  try {
    const params = { ...req.body, id, clientId, userId };
    const data = await withAudit(() => tcpService.patchRecord(params, {}), {
      entity: "Tcp",
      userId,
      clientId,
      entityId: id,
      action: "PatchRecordTCP",
    });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "PatchRecordTCP",
      entity: "Tcp",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.status(200).json({ status: "success", data });
  } catch (error) {
    logger.logEvent("error", "Error patching TCP record", {
      action: "PatchRecordTCP",
      id,
      clientId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// Bulk PATCH update
async function bulkPatchUpdate(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    if (!Array.isArray(req.body)) {
      return res
        .status(400)
        .json({ status: "error", message: "Validation error" });
    }
    const data = await withAudit(
      () =>
        tcpService.bulkPatchUpdate({ records: req.body, clientId, userId }, {}),
      { entity: "Tcp", userId, clientId, action: "BulkPatchUpdateTCP" }
    );
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "BulkPatchUpdateTCP",
      entity: "Tcp",
      details: { count: Array.isArray(data) ? data.length : undefined },
    });
    res.status(200).json({ status: "success", data });
  } catch (error) {
    logger.logEvent("error", "Error in bulk patch update", {
      action: "BulkPatchUpdateTCP",
      clientId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// Bulk CREATE TCPs
async function bulkCreate(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    if (!Array.isArray(req.body)) {
      return res
        .status(400)
        .json({ status: "error", message: "Validation error" });
    }
    for (const record of req.body) {
      const { error: validationError } = tcpBulkImportSchema.validate(record);
      if (validationError) {
        return res
          .status(400)
          .json({ status: "error", message: "Validation error" });
      }
    }
    const ptrsId = req.body[0]?.ptrsId || req.params.ptrsId;
    const params = { records: req.body, clientId, userId, ptrsId };
    const data = await withAudit(() => tcpService.bulkCreate(params, {}), {
      entity: "Tcp",
      userId,
      clientId,
      action: "BulkCreateTCP",
    });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "BulkCreateTCP",
      entity: "Tcp",
      details: { ptrsId, count: Array.isArray(data) ? data.length : undefined },
    });
    res.status(201).json({ status: "success", data });
  } catch (error) {
    logger.logEvent("error", "Error in bulk create TCP", {
      action: "BulkCreateTCP",
      clientId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// Bulk UPDATE TCPs
async function bulkUpdate(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    if (!Array.isArray(req.body)) {
      return res
        .status(400)
        .json({ status: "error", message: "Validation error" });
    }
    for (const record of req.body) {
      const { error: validationError } = tcpSchema.validate(record);
      if (validationError) {
        return res
          .status(400)
          .json({ status: "error", message: "Validation error" });
      }
    }
    const ptrsId = req.body[0]?.ptrsId || req.params.ptrsId;
    const params = { records: req.body, clientId, userId, ptrsId };
    const data = await withAudit(() => tcpService.bulkUpdate(params, {}), {
      entity: "Tcp",
      userId,
      clientId,
      action: "BulkUpdateTCP",
    });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "BulkUpdateTCP",
      entity: "Tcp",
      details: { ptrsId, count: Array.isArray(data) ? data.length : undefined },
    });
    res.status(200).json({ status: "success", data });
  } catch (error) {
    logger.logEvent("error", "Error in bulk update TCP", {
      action: "BulkUpdateTCP",
      clientId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// PATCH partial update (custom)
async function partialUpdate(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const params = { ...req.body, clientId, userId };
    const data = await withAudit(() => tcpService.partialUpdate(params, {}), {
      entity: "Tcp",
      userId,
      clientId,
      action: "PartialUpdateTCP",
    });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "PartialUpdateTCP",
      entity: "Tcp",
      details: { updates: Object.keys(req.body) },
    });
    res.status(200).json({ status: "success", data });
  } catch (error) {
    logger.logEvent("error", "Error in partial update TCP", {
      action: "PartialUpdateTCP",
      clientId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// PUT: SBI update (custom)
async function sbiUpdate(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const records = Array.isArray(req.body)
      ? req.body
      : Object.values(req.body);
    for (const record of records) {
      if (!record.payeeEntityAbn) {
        return res
          .status(400)
          .json({ status: "error", message: "Validation error" });
      }
    }
    const ptrsId = req.params.id;
    const params = { records, clientId, userId, ptrsId };
    const data = await withAudit(() => tcpService.sbiUpdate(params, {}), {
      entity: "Tcp",
      userId,
      clientId,
      entityId: ptrsId,
      action: "SBIUpload",
    });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "SBIUpload",
      entity: "Tcp",
      entityId: ptrsId,
      details: { count: Array.isArray(records) ? records.length : undefined },
    });
    res.status(200).json({ status: "success", data });
  } catch (error) {
    logger.logEvent("error", "Error in SBI upload/update", {
      action: "SBIUpload",
      clientId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// DELETE a TCP
async function _delete(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const id = req.params.id;
  try {
    if (!clientId) {
      return res
        .status(400)
        .json({ status: "error", message: "Validation error" });
    }
    await withAudit(() => tcpService.delete({ id, clientId, userId }, {}), {
      entity: "Tcp",
      userId,
      clientId,
      entityId: id,
      action: "DeleteTCP",
    });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "DeleteTCP",
      entity: "Tcp",
      entityId: id,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error deleting TCP", {
      action: "DeleteTCP",
      id,
      clientId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// Check for missing isSb flag
async function checkMissingIsSb(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const data = await tcpService.hasMissingIsSbFlag({ clientId });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "CheckMissingIsSb",
      entity: "Tcp",
      details: { result: data },
    });
    res.status(200).json({ status: "success", data });
  } catch (error) {
    logger.logEvent("error", "Error checking missing isSb flag", {
      action: "CheckMissingIsSb",
      userId,
      clientId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// Submit final PTRS
async function submitFinalPtrs(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const data = await withAudit(
      () => tcpService.finalisePtrs({ clientId, userId }, {}),
      { entity: "Tcp", userId, clientId, action: "SubmitFinalPtrs" }
    );
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "SubmitFinalPtrs",
      entity: "Tcp",
    });
    res.status(200).json({ status: "success", data });
  } catch (error) {
    logger.logEvent("error", "Error submitting final PTRS", {
      action: "SubmitFinalPtrs",
      clientId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// Download summary PTRS
async function downloadSummaryPtrs(req, res, next) {
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const csvData = await tcpService.generateSummaryCsv({ clientId, userId });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "DownloadSummaryPtrs",
      entity: "Tcp",
      details: { size: csvData?.length },
    });
    res.setHeader("Content-Type", "text/csv");
    res.setHeader(
      "Content-Disposition",
      "attachment; filename=summary_ptrs.csv"
    );
    res.status(200).send(csvData);
  } catch (error) {
    logger.logEvent("error", "Error downloading summary PTRS", {
      action: "DownloadSummaryPtrs",
      clientId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// Ensure tmpUploads directory exists before handling uploads
const tmpUploadPath = path.join(__dirname, "../tmpUploads");
if (!fs.existsSync(tmpUploadPath)) {
  fs.mkdirSync(tmpUploadPath, { recursive: true });
}

// Upload file (CSV import)
async function uploadFile(req, res, next) {
  try {
    if (!req.file || !req.file.path) {
      logger.logEvent("warn", "Missing file or file path in request", {
        action: "PtrsDataUpload",
      });
    }
    const filePath = req.file.path; // temp path from multer
    const ext = path.extname(filePath).toLowerCase();
    await scanFile(filePath, ext, req.file.originalname);

    // Move file to permanent uploads directory after scan
    const uploadsDir = path.join(__dirname, "../uploads/ptrs_data_uploads");
    if (!fs.existsSync(uploadsDir)) {
      fs.mkdirSync(uploadsDir, { recursive: true });
    }
    const destPath = path.join(uploadsDir, req.file.originalname);

    fs.renameSync(filePath, destPath);

    // Parse CSV file and collect basic validation results
    const results = [];
    const invalidRows = [];

    fs.createReadStream(destPath)
      .pipe(csv())
      .on("data", (row) => {
        const allowedFields = [
          "payerEntityName",
          "payerEntityAbn",
          "payerEntityAcnArbn",
          "payeeEntityName",
          "payeeEntityAbn",
          "payeeEntityAcnArbn",
          "paymentAmount",
          "description",
          "transactionType",
          "isReconciled",
          "supplyDate",
          "paymentDate",
          "contractPoReferenceNumber",
          "contractPoPaymentTerms",
          "noticeForPaymentIssueDate",
          "noticeForPaymentTerms",
          "invoiceReferenceNumber",
          "invoiceIssueDate",
          "invoiceReceiptDate",
          "invoiceAmount",
          "invoicePaymentTerms",
          "invoiceDueDate",
        ];
        row = Object.fromEntries(
          Object.entries(row).filter(([key]) => allowedFields.includes(key))
        );

        for (const key in row) {
          if (
            (typeof row[key] === "string" &&
              row[key].trim().toUpperCase() === "'NULL'") ||
            "NULL".includes(row[key].trim().toUpperCase())
          ) {
            row[key] = null;
          } else if (row[key] === "") {
            row[key] = null;
          }
        }
        const forceStringFields = ["payerEntityAcnArbn", "payeeEntityAcnArbn"];
        for (const key of forceStringFields) {
          if (row[key]) {
            row[key] = String(row[key]).trim().replace(/\.0$/, "");
          }
        }
        if (row.hasOwnProperty("isReconciled")) {
          const val = String(row["isReconciled"]).trim().toLowerCase();
          if (["1", "true", "t"].includes(val)) {
            row["isReconciled"] = true;
          } else if (["0", "false", "f"].includes(val)) {
            row["isReconciled"] = false;
          } else {
            row["isReconciled"] = null;
          }
        }

        const now = new Date();
        row.createdBy = req.auth.id;
        row.updatedBy = req.auth.id;
        row.clientId = req.auth.clientId;
        row.ptrsId = req.body.ptrsId;
        const reasons = [];

        const hasPayerName =
          typeof row.payerEntityName === "string" &&
          row.payerEntityName.trim() !== "";
        if (!hasPayerName) reasons.push("Missing or invalid payerEntityName");

        const hasPayeeName =
          typeof row.payeeEntityName === "string" &&
          row.payeeEntityName.trim() !== "";
        if (!hasPayeeName) reasons.push("Missing or invalid payeeEntityName");

        const hasABN =
          row.payeeEntityAbn &&
          /^\d{11}$/.test(String(row.payeeEntityAbn).trim());
        if (!hasABN) reasons.push("Missing or invalid payeeEntityAbn");

        const hasValidAmount = !isNaN(parseFloat(row.paymentAmount));
        if (!hasValidAmount) reasons.push("Missing or invalid paymentAmount");

        const hasPaymentDate =
          row.paymentDate && !isNaN(Date.parse(row.paymentDate));
        if (!hasPaymentDate) reasons.push("Missing or invalid paymentDate");

        const hasAnyError =
          !hasPayerName ||
          !hasPayeeName ||
          !hasABN ||
          !hasValidAmount ||
          !hasPaymentDate;

        if (hasAnyError) {
          invalidRows.push({ ...row, issues: reasons });
        } else {
          results.push(row);
        }
      })
      .on("end", async () => {
        try {
          const source = "csv_upload";
          const insertResults = await tcpService.saveTransformedDataToTcp(
            {
              transformedRecords: results,
              ptrsId: req.body.ptrsId,
              clientId: req.auth.clientId,
              createdBy: req.auth.id,
              source: "csv_upload",
            },
            { transaction: req.dbTransaction }
          );

          if (invalidRows.length > 0) {
            await tcpService.saveErrorsToTcpError(
              {
                errorRecords: invalidRows,
                ptrsId: req.body.ptrsId,
                clientId: req.auth.clientId,
                createdBy: req.auth.id,
                source: "csv_upload",
              },
              { transaction: req.dbTransaction }
            );
          }

          try {
            await ptrsService.saveUploadMetadata(
              {
                clientId: req.auth.clientId,
                userId: req.auth.id,
                ptrsId: req.body.ptrsId,
                filename: req.file.originalname,
                filepath: destPath,
                recordCount: results.length,
                uploadedAt: new Date(),
                updatedBy: req.auth.id,
              },
              { transaction: req.dbTransaction }
            );
          } catch (err) {
            logger.logEvent("error", "Failed to log file upload metadata", {
              action: "uploadFile-metadata",
              error: err.message,
              stack: err.stack,
            });
          }

          logger.logEvent("info", "File uploaded and processed successfully", {
            action: "uploadFile",
            clientId: req.auth.clientId,
            userId: req.auth.id,
            ptrsId: req.body.ptrsId,
            validRows: results.length,
            invalidRows: invalidRows.length,
          });
          await auditService.logEvent({
            clientId: req.auth.clientId,
            userId: req.auth.id,
            ip: req.ip,
            device: req.headers["user-agent"],
            action: "PtrsDataUpload",
            entity: "Tcp",
            details: {
              ptrsId: req.body.ptrsId,
              validRows: results.length,
              invalidRows: invalidRows.length,
              filename: req.file.originalname,
            },
          });
          res.status(200).json({
            status: "success",
            data: {
              totalRows: results.length + invalidRows.length,
              validRows: results.length,
              invalidRows: invalidRows.length,
              inserted: insertResults,
              errors: invalidRows,
              validRecordsPreview: results.slice(0, 20),
            },
          });
        } catch (insertErr) {
          logger.logEvent("error", "Bulk insert failed", {
            action: "uploadFile-insert",
            error: insertErr.message,
            stack: insertErr.stack,
          });
          return next(insertErr);
        }
      })
      .on("error", (err) => {
        logger.logEvent("error", "CSV parsing failed", {
          action: "uploadFile-parse",
          error: err.message,
          stack: err.stack,
        });
        return next(err);
      });
  } catch (err) {
    logger.logEvent("error", "Error uploading file", {
      action: "uploadFile",
      error: err.message,
      stack: err.stack,
    });
    return next(err);
  }
}

// Get errors by PTRS ID
async function getErrorsByPtrsId(req, res, next) {
  const ptrsId = req.params.id;
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const data = await tcpService.getErrorsByPtrsId({ ptrsId, clientId });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetErrorsByPtrsId",
      entity: "Tcp",
      details: { ptrsId, count: Array.isArray(data) ? data.length : undefined },
    });
    res
      .status(200)
      .json({ status: "success", data: Array.isArray(data) ? data : [] });
  } catch (error) {
    logger.logEvent("error", "Error fetching TCP errors by ptrsId", {
      action: "GetErrorsByPtrsId",
      ptrsId,
      clientId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

// Recalculate TCP metrics
async function recalculateMetrics(req, res, next) {
  const ptrsId = req.params.id;
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    await processTcpMetrics(ptrsId, clientId);
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "RecalculateTcpMetrics",
      entity: "Tcp",
      details: { ptrsId },
    });
    res.status(200).json({
      status: "success",
      data: { message: "TCP metrics recalculated." },
    });
  } catch (error) {
    logger.logEvent("error", "Error recalculating TCP metrics", {
      action: "RecalculateTcpMetrics",
      ptrsId,
      clientId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

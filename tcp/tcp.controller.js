const express = require("express");
const router = express.Router();
const multer = require("multer");
const upload = multer({ dest: "tmpUploads/" });
const Joi = require("joi");
const authorise = require("../middleware/authorise");
const tcpService = require("./tcp.service");
const validateRequest = require("../middleware/validate-request");
const { tcpBulkImportSchema, tcpSchema } = require("./tcp.validator");
const { logger } = require("../helpers/logger");
const fs = require("fs");
const path = require("path");
const { scanFile } = require("../middleware/virus-scan");
const reportService = require("../reports/report.service");
const csv = require("csv-parser");
const { processTcpMetrics } = require("../utils/calcs/processTcpMetrics");

// routes
router.get("/", authorise(), getAll);
router.get("/report/:id", authorise(), getAllByReportId);
router.get("/tcp/:id", authorise(), getTcpByReportId);
router.get("/:id", authorise(), getById);
router.patch("/bulk-patch", authorise(), bulkPatchUpdate);
router.patch("/:id", authorise(), patchRecord);
router.put("/", authorise(), validateRequest(tcpSchema), bulkUpdate);
router.put("/partial", authorise(), partialUpdate);
router.post("/", authorise(), validateRequest(tcpBulkImportSchema), bulkCreate);
router.put("/sbi/:id", authorise(), sbiUpdate);
router.delete("/:id", authorise(), _delete);
router.get("/missing-isSb", authorise(), checkMissingIsSb);
router.put("/submit-final", authorise(), submitFinalReport);
router.get("/download-summary", authorise(), downloadSummaryReport);
router.post("/upload", authorise(), upload.single("file"), uploadFile);
router.get("/errors/:id", authorise(), getErrorsByReportId);
router.put("/recalculate/:id", authorise(), recalculateMetrics);

module.exports = router;

function getAll(req, res, next) {
  tcpService
    .getAll({ transaction: req.dbTransaction })
    .then((entities) => res.json(entities))
    .catch(next);
}

function getAllByReportId(req, res, next) {
  tcpService
    .getAllByReportId(req.params.id, req.auth?.clientId)
    .then((tcp) => (tcp ? res.json(tcp) : res.sendStatus(404)))
    .catch(next);
}

// Patch a single TCP record
async function patchRecord(req, res, next) {
  try {
    const { id } = req.params;
    const userId = req.auth.id;
    const clientId = req.auth.clientId;
    const updates = req.body;

    if (!updates || typeof updates !== "object" || Array.isArray(updates)) {
      return res.status(400).json({ message: "Invalid request body" });
    }

    // Fetch the old record before updating
    const oldRecord = await tcpService.getById(id, clientId);
    if (!oldRecord) {
      return res.status(404).json({ message: "Record not found" });
    }

    // Patch the record
    const updated = await tcpService.patchRecord(id, updates, clientId);

    res.json({
      success: true,
      message: "Record updated successfully",
      data: updated,
    });
  } catch (error) {
    console.error("Error patching record:", error);
    next(error);
  }
}

function getTcpByReportId(req, res, next) {
  tcpService
    .getTcpByReportId(req.params.id, req.auth?.clientId)
    .then((tcp) => (tcp ? res.json(tcp) : res.sendStatus(404)))
    .catch(next);
}

function sbiUpdate(req, res, next) {
  try {
    // Ensure req.body is an object and iterate through its keys
    const records = Object.values(req.body);

    const promises = records.flatMap((item) => {
      // Check if item is an array and process each object individually
      const itemsToProcess = Array.isArray(item) ? item : [item];

      return itemsToProcess.map(async (record) => {
        try {
          // Validate each record using sbiSchema
          const reqForValidation = { body: record };
          await validateSbiRecord(reqForValidation);

          // Save each record using tcpService
          return await tcpService.sbiUpdate(req.params.id, record, {
            transaction: req.dbTransaction,
          });
        } catch (error) {
          console.error("Error processing record:", error);
          throw error; // Propagate the error to Promise.all
        }
      });
    });

    // Wait for all records to be saved
    Promise.all(promises)
      .then((results) => {
        res.json({
          success: true,
          message: "All records saved successfully",
          results,
        });
        logger.auditEvent("SBI result uploaded", {
          action: "SBIUpload",
          userId: req.auth.id,
          clientId: req.auth.clientId,
        });
      })
      .catch((error) => {
        console.error("Error saving records:", error);
        next(error); // Pass the error to the global error handler
      });
  } catch (error) {
    console.error("Error processing bulk records:", error);
    next(error); // Pass the error to the global error handler
  }
}

async function validateSbiRecord(req) {
  const schema = Joi.object({
    payeeEntityAbn: Joi.number().required(),
  });
  // Validate the request body
  await schema.validateAsync(req.body);
}

function partialUpdate(req, res, next) {
  try {
    // Ensure req.body is an object and iterate through its keys
    const records = Object.values(req.body);
    console.log("Records to update:", records);

    const promises = records.flatMap((item) => {
      // Check if item is an array and process each object individually
      const itemsToProcess = Array.isArray(item) ? item : [item];

      return itemsToProcess.map(async (record) => {
        try {
          // Exclude id and createdAt fields from the record
          const { id, createdAt, ...recordToUpdate } = record;

          // Validate each record using bulkUpdateSchema
          const reqForValidation = { body: recordToUpdate };
          await partialUpdateSchema(reqForValidation); // Validate the record

          // Save each record using tcpService
          return await tcpService.update(record.id, recordToUpdate, {
            transaction: req.dbTransaction,
          });
        } catch (error) {
          console.error("Error processing record:", error);
          throw error; // Propagate the error to Promise.all
        }
      });
    });

    // Wait for all records to be saved
    Promise.all(promises)
      .then((results) =>
        res.json({
          success: true,
          message: "All records updated successfully",
          results,
        })
      )
      .catch((error) => {
        console.error("Error updating records:", error);
        next(error); // Pass the error to the global error handler
      });
  } catch (error) {
    console.error("Error processing bulk records:", error);
    next(error); // Pass the error to the global error handler
  }
}

async function partialUpdateSchema(req) {
  const schema = Joi.object({
    partialPayment: Joi.boolean().required(),
    updatedBy: Joi.number().required(),
  });

  // Validate the request body
  await schema.validateAsync(req.body);
}

function getById(req, res, next) {
  tcpService
    .getById(req.params.id, { transaction: req.dbTransaction })
    .then((tcp) => (tcp ? res.json(tcp) : res.sendStatus(404)))
    .catch(next);
}

// Bulk create
async function bulkCreate(req, res, next) {
  const transaction = req.dbTransaction;
  try {
    if (!Array.isArray(req.body)) {
      return res
        .status(400)
        .json({ message: "Request body must be an array." });
    }
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const results = [];

    for (const record of req.body) {
      try {
        const created = await tcpService.create(record, clientId);
        results.push(created);
      } catch (error) {
        console.error("Error processing record:", error);
        return res.status(400).json({
          success: false,
          message: "Error saving some records.",
          error: error.message,
        });
      }
    }

    logger.logEvent("info", "Bulk TCP records created", {
      action: "BulkCreateTCP",
      clientId: clientId,
      count: results.length,
    });

    // await transaction.commit();
    res.json({
      success: true,
      message: "All records saved successfully",
      results,
    });
  } catch (error) {
    await transaction.rollback();
    console.error("Error processing bulk records:", error);
    next(error);
  }
}

// PUT: Bulk update
async function bulkUpdate(req, res, next) {
  const transaction = req.dbTransaction;
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const records = Object.values(req.body);
    const results = [];

    for (const item of records) {
      const itemsToProcess = Array.isArray(item) ? item : [item];
      for (const record of itemsToProcess) {
        try {
          const { id, createdAt, ...recordToUpdate } = record;
          const oldRecord = await tcpService.getById(id, clientId);
          const updated = await tcpService.update(id, recordToUpdate, clientId);
          results.push(updated);
        } catch (error) {
          console.error("Error processing record:", error);
          throw error;
        }
      }
    }

    logger.logEvent("info", "Bulk TCP records updated", {
      action: "BulkUpdateTCP",
      clientId: clientId,
      count: results.length,
    });

    await transaction.commit();
    res.json({
      success: true,
      message: "All records updated successfully",
      results,
    });
  } catch (error) {
    await transaction.rollback();
    console.error("Error processing bulk records:", error);
    next(error);
  }
}

// DELETE: Delete a record
async function _delete(req, res, next) {
  const transaction = req.dbTransaction;
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const id = req.params.id;

    const oldRecord = await tcpService.getById(id, { transaction });
    await tcpService.delete(id, { transaction });

    logger.logEvent("warn", "TCP record deleted", {
      action: "DeleteTCP",
      tcpId: id,
      clientId,
    });

    await transaction.commit();
    res.json({
      message: "Tcp deleted successfully",
    });
  } catch (error) {
    await transaction.rollback();
    console.error("Error deleting tcp:", error);
    next(error);
  }
}

function checkMissingIsSb(req, res, next) {
  tcpService
    .hasMissingIsSbFlag({ transaction: req.dbTransaction })
    .then((result) => res.json(result))
    .catch(next);
}

function submitFinalReport(req, res, next) {
  tcpService
    .finaliseReport({ transaction: req.dbTransaction })
    .then((result) => {
      res.json(result);
      logger.auditEvent("Report submitted", {
        action: "SubmitFinalReport",
        userId: req.auth.id,
        clientId: req.auth.clientId,
      });
    })
    .catch(next);
}

function downloadSummaryReport(req, res, next) {
  tcpService
    .generateSummaryCsv({ transaction: req.dbTransaction })
    .then((csv) => {
      res.setHeader("Content-Type", "text/csv");
      res.setHeader(
        "Content-Disposition",
        "attachment; filename=summary_report.csv"
      );
      res.send(csv);
      logger.auditEvent("Summary report downloaded", {
        action: "DownloadSummaryReport",
        userId: req.auth.id,
        clientId: req.auth.clientId,
      });
    })
    .catch(next);
}

// Bulk bulk patch route handler
async function bulkPatchUpdate(req, res, next) {
  logger.logEvent("info", "Incoming bulk patch request", {
    clientId: req.auth.clientId,
  }); // log start of request
  try {
    if (!Array.isArray(req.body)) {
      logger.logEvent("warn", "Invalid request body for bulk patch", {
        clientId: req.auth.clientId,
      });
      return res
        .status(400)
        .json({ message: "Request body must be an array." });
    }

    const clientId = req.auth.clientId;
    console.log("Client ID for bulk patch:", clientId);
    const userId = req.auth.id;

    const updatePromises = req.body.map(async (record) => {
      const { id, ...updates } = record;
      if (!id || typeof updates !== "object" || Array.isArray(updates)) {
        logger.logEvent("warn", "Invalid record structure in bulk patch", {
          clientId,
        });
        throw new Error(
          "Each record must have an id and updates must be an object"
        );
      }
      const { step, ...filteredUpdates } = updates;
      // const oldRecord = await tcpService.getById(id, req.auth?.clientId);
      const updated = await tcpService.patchRecord(
        id,
        filteredUpdates,
        clientId
      );
      return updated;
    });

    const results = await Promise.all(updatePromises);

    logger.logEvent("info", "Bulk patch update successful", {
      action: "BulkPatchUpdateTCP",
      clientId: clientId,
      count: results.length,
      // updatedIds: results.map((r) => r.id),
    });

    res.json({
      success: true,
      message: "All records patch updated successfully",
      results,
    });
  } catch (error) {
    logger.logEvent("error", "Error in bulk patch update", {
      clientId: req.auth.clientId,
      error: error.message,
    });
    console.error("Error bulk patch updating records:", error);
    next(error);
  }
}

// Ensure tmpUploads directory exists before handling uploads
const tmpUploadPath = path.join(__dirname, "../tmpUploads");
if (!fs.existsSync(tmpUploadPath)) {
  fs.mkdirSync(tmpUploadPath, { recursive: true });
}

async function uploadFile(req, res) {
  // console.log("req.file in uploadFile:", req.file);
  try {
    // console.log("[DEBUG] File received by route:", req.file);
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
        // Filter only allowed fields at the start
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

        // Convert 'NULL' strings and empty strings to actual null
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
        // Force string fields to remain strings and strip ".0" float artifacts
        const forceStringFields = ["payerEntityAcnArbn", "payeeEntityAcnArbn"];
        for (const key of forceStringFields) {
          if (row[key]) {
            row[key] = String(row[key]).trim().replace(/\.0$/, "");
          }
        }
        // Robust, case-insensitive conversion for isReconciled
        if (row.hasOwnProperty("isReconciled")) {
          const val = String(row["isReconciled"]).trim().toLowerCase();
          // console.log("Processing isReconciled value:", row["isReconciled"]);
          if (["1", "true", "t"].includes(val)) {
            row["isReconciled"] = true;
          } else if (["0", "false", "f"].includes(val)) {
            row["isReconciled"] = false;
          } else {
            row["isReconciled"] = null;
          }
        }

        // Set default values for each processed row
        // console.log("Processing req.body.reportId:", req.body.reportId);
        const now = new Date();
        row.createdBy = req.auth.id;
        row.updatedBy = req.auth.id;
        row.clientId = req.auth.clientId;
        row.reportId = req.body.reportId;
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
          // Insert valid rows using saveTransformedDataToTcp
          const source = "csv_upload";
          const insertResults = await tcpService.saveTransformedDataToTcp(
            results,
            req.body.reportId,
            req.auth.clientId,
            req.auth.id,
            source,
            {
              transaction: req.dbTransaction,
            }
          );

          // Save invalid rows to tcp_error table
          if (invalidRows.length > 0) {
            await tcpService.saveErrorsToTcpError(
              invalidRows,
              req.body.reportId,
              req.auth.clientId,
              req.auth.id,
              source,
              {
                transaction: req.dbTransaction,
              }
            );
          }

          // Insert metadata into tbl_report_upload (reportUploadService)
          try {
            await reportService.saveUploadMetadata(
              {
                clientId: req.auth.clientId,
                userId: req.auth.id,
                reportId: req.body.reportId,
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
            });
            // Do not block overall process if metadata save fails
          }

          res.status(200).json({
            message:
              "File uploaded, parsed, and records inserted successfully.",
            totalRows: results.length + invalidRows.length,
            validRows: results.length,
            invalidRows: invalidRows.length,
            inserted: insertResults,
            errors: invalidRows, // Include full invalid rows with reasons
            validRecordsPreview: results.slice(0, 20), // Preview for UI
          });
        } catch (insertErr) {
          logger.logEvent("error", "Bulk insert failed", {
            action: "uploadFile-insert",
            error: insertErr.message,
          });
          res.status(500).json({ error: "Failed to insert valid records." });
        }
      })
      .on("error", (err) => {
        logger.logEvent("error", "CSV parsing failed", {
          action: "uploadFile-parse",
          error: err.message,
        });
        res.status(500).json({ error: "Failed to parse CSV file." });
      });
  } catch (err) {
    logger.logEvent("error", "Error uploading file", {
      action: "uploadFile",
      error: err.message,
    });
    return res.status(500).json({ error: "Failed to upload file." });
  }
}

async function getErrorsByReportId(req, res, next) {
  try {
    const reportId = req.params.id;
    const errors = await tcpService.getErrorsByReportId(reportId, {
      transaction: req.dbTransaction,
    });
    if (!errors || errors.length === 0) {
      return res
        .status(404)
        .json({ message: "No errors found for this report." });
    }
    res.json(errors);
  } catch (error) {
    console.error("Error fetching errors by report ID:", error);
    next(error);
  }
}

// Controller for recalculating TCP metrics
async function recalculateMetrics(req, res, next) {
  try {
    const reportId = req.params.id;
    const clientId = req.auth.clientId;
    await processTcpMetrics(reportId, clientId);
    res.json({ success: true, message: "TCP metrics recalculated." });
  } catch (error) {
    next(error);
  }
}

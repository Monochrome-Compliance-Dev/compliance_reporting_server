const express = require("express");
const router = express.Router();
const Joi = require("joi");
const authorise = require("../middleware/authorise");
const tcpService = require("./tcp.service");
const setClientContext = require("../middleware/set-client-context");
const validateRequest = require("../middleware/validate-request");
const {
  tcpImportSchema,
  tcpBulkImportSchema,
  tcpSchema,
} = require("./tcp.validator");
const logger = require("../helpers/logger");

// routes
router.get("/", authorise(), setClientContext, getAll);
router.get("/report/:id", authorise(), setClientContext, getAllByReportId);
router.get("/tcp/:id", authorise(), setClientContext, getTcpByReportId);
router.get("/:id", authorise(), setClientContext, getById);
router.post(
  "/",
  authorise(),
  validateRequest(tcpBulkImportSchema),
  setClientContext,
  bulkCreate
);
router.patch("/:id", authorise(), setClientContext, patchRecord);
router.put(
  "/",
  authorise(),
  validateRequest(tcpSchema),
  setClientContext,
  bulkUpdate
);
router.put("/partial", authorise(), setClientContext, partialUpdate);
router.put("/sbi/:id", authorise(), setClientContext, sbiUpdate);
router.delete("/:id", authorise(), setClientContext, _delete);
router.get("/missing-isSb", authorise(), setClientContext, checkMissingIsSb);
router.put("/submit-final", authorise(), setClientContext, submitFinalReport);
router.get(
  "/download-summary",
  authorise(),
  setClientContext,
  downloadSummaryReport
);

module.exports = router;

function getAll(req, res, next) {
  tcpService
    .getAll(req.auth.clientId)
    .then((entities) => res.json(entities))
    .catch(next);
}

function getAllByReportId(req, res, next) {
  tcpService
    .getAllByReportId(req.params.id, req.auth.clientId)
    .then((tcp) => (tcp ? res.json(tcp) : res.sendStatus(404)))
    .catch(next);
}

async function patchRecord(req, res, next) {
  try {
    const { id } = req.params;
    const clientId = req.auth.clientId;
    const updates = req.body;

    if (!updates || typeof updates !== "object" || Array.isArray(updates)) {
      return res.status(400).json({ message: "Invalid request body" });
    }

    const updated = await tcpService.partialUpdate(id, updates, clientId);
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
    .getTcpByReportId(req.params.id, req.auth.clientId)
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
          return await tcpService.sbiUpdate(
            req.params.id,
            record,
            req.auth.clientId
          );
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
          return await tcpService.update(
            record.id,
            recordToUpdate,
            req.auth.clientId
          );
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
    .getById(req.params.id, req.auth.clientId)
    .then((tcp) => (tcp ? res.json(tcp) : res.sendStatus(404)))
    .catch(next);
}

async function bulkCreate(req, res, next) {
  try {
    // Ensure the incoming request is an array
    if (!Array.isArray(req.body)) {
      return res
        .status(400)
        .json({ message: "Request body must be an array." });
    }

    const results = [];

    for (const record of req.body) {
      try {
        // Normalize numeric fields
        const numericFields = [
          "payerEntityAbn",
          "payerEntityAcnArbn",
          "payeeEntityAbn",
          "payeeEntityAcnArbn",
        ];
        numericFields.forEach((field) => {
          if (record[field] === "" || record[field] === " ") {
            record[field] = null;
          } else if (record[field] !== null && record[field] !== undefined) {
            const parsed = parseInt(record[field], 10);
            record[field] = isNaN(parsed) ? null : parsed;
          }
        });

        // Create the record using the service
        const created = await tcpService.create(record, req.auth.clientId);
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
      clientId: req.auth.clientId,
      count: results.length,
    });

    res.json({
      success: true,
      message: "All records saved successfully",
      results,
    });
  } catch (error) {
    console.error("Error processing bulk records:", error);
    next(error);
  }
}

function create(req, res, next) {
  tcpService
    .create(req.body, req.auth.clientId)
    .then((tcp) => res.json(tcp))
    .catch((error) => {
      console.error("Error creating tcp:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function bulkUpdate(req, res, next) {
  try {
    // Ensure req.body is an object and iterate through its keys
    const records = Object.values(req.body);
    // console.log("Records to update:", records);

    const promises = records.flatMap((item) => {
      // Check if item is an array and process each object individually
      const itemsToProcess = Array.isArray(item) ? item : [item];

      return itemsToProcess.map(async (record) => {
        try {
          // Exclude id and createdAt fields from the record
          const { id, createdAt, ...recordToUpdate } = record;

          // Validate each record using bulkUpdateSchema
          // const reqForValidation = { body: recordToUpdate };
          // await bulkUpdateSchema(reqForValidation); // Validate the record

          // Save each record using tcpService
          return await tcpService.update(id, recordToUpdate, req.auth.clientId);
        } catch (error) {
          console.error("Error processing record:", error);
          throw error; // Propagate the error to Promise.all
        }
      });
    });

    // Wait for all records to be saved
    Promise.all(promises)
      .then((results) => {
        logger.logEvent("info", "Bulk TCP records updated", {
          action: "BulkUpdateTCP",
          clientId: req.auth.clientId,
          count: results.length,
        });
        res.json({
          success: true,
          message: "All records updated successfully",
          results,
        });
      })
      .catch((error) => {
        console.error("Error updating records:", error);
        next(error); // Pass the error to the global error handler
      });
  } catch (error) {
    console.error("Error processing bulk records:", error);
    next(error); // Pass the error to the global error handler
  }
}

function update(req, res, next) {
  tcpService
    .update(req.params.id, req.body, req.auth.clientId)
    .then((tcp) => res.json(tcp))
    .catch(next);
}

function _delete(req, res, next) {
  tcpService
    .delete(req.params.id, req.auth.clientId)
    .then(() => {
      logger.logEvent("warn", "TCP record deleted", {
        action: "DeleteTCP",
        tcpId: req.params.id,
        clientId: req.auth.clientId,
      });
      res.json({ message: "Tcp deleted successfully" });
    })
    .catch((error) => {
      console.error("Error deleting tcp:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function checkMissingIsSb(req, res, next) {
  tcpService
    .hasMissingIsSbFlag(req.auth.clientId)
    .then((result) => res.json(result))
    .catch(next);
}

function submitFinalReport(req, res, next) {
  tcpService
    .finaliseReport(req.auth.clientId)
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
    .generateSummaryCsv(req.auth.clientId)
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

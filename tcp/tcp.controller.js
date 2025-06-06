const express = require("express");
const router = express.Router();
const Joi = require("joi");
const authorise = require("../middleware/authorise");
const tcpService = require("./tcp.service");
const validateRequest = require("../middleware/validate-request");
const {
  tcpImportSchema,
  tcpBulkImportSchema,
  tcpSchema,
} = require("./tcp.validator");
const { logger } = require("../helpers/logger");

// routes
router.get("/", authorise(), getAll);
router.get("/report/:id", authorise(), getAllByReportId);
router.get("/tcp/:id", authorise(), getTcpByReportId);
router.get("/:id", authorise(), getById);
router.post("/", authorise(), validateRequest(tcpBulkImportSchema), bulkCreate);
router.patch("/bulk-patch", authorise(), bulkPatchUpdate);
router.patch("/:id", authorise(), patchRecord);
router.put("/", authorise(), validateRequest(tcpSchema), bulkUpdate);
router.put("/partial", authorise(), partialUpdate);
router.put("/sbi/:id", authorise(), sbiUpdate);
router.delete("/:id", authorise(), _delete);
router.get("/missing-isSb", authorise(), checkMissingIsSb);
router.put("/submit-final", authorise(), submitFinalReport);
router.get("/download-summary", authorise(), downloadSummaryReport);

module.exports = router;

function getAll(req, res, next) {
  tcpService
    .getAll({ transaction: req.dbTransaction })
    .then((entities) => res.json(entities))
    .catch(next);
}

function getAllByReportId(req, res, next) {
  tcpService
    .getAllByReportId(req.params.id, { transaction: req.dbTransaction })
    .then((tcp) => (tcp ? res.json(tcp) : res.sendStatus(404)))
    .catch(next);
}

// Patch a single TCP record and create audit entries for changed fields
async function patchRecord(req, res, next) {
  try {
    const { id } = req.params;
    const userId = req.auth.id;
    const updates = req.body;
    const auditService = require("../audit/audit.service");
    let nanoid;
    // Lazy load nanoid if not already loaded
    if (!nanoid) {
      const { nanoid: importedNanoid } = await import("nanoid");
      nanoid = importedNanoid;
    }

    if (!updates || typeof updates !== "object" || Array.isArray(updates)) {
      return res.status(400).json({ message: "Invalid request body" });
    }

    // Fetch the old record before updating
    const oldRecord = await tcpService.getById(id, {
      transaction: req.dbTransaction,
    });
    if (!oldRecord) {
      return res.status(404).json({ message: "Record not found" });
    }

    // Patch the record
    const updated = await tcpService.partialUpdate(id, updates, {
      transaction: req.dbTransaction,
    });

    // For each changed field, create an audit entry, skipping 'updatedAt'
    const now = new Date();
    const auditEntries = [];
    for (const [field, newValue] of Object.entries(updates)) {
      if (field === "updatedAt") continue; // Skip updatedAt field
      const oldValue = oldRecord[field];
      if (oldValue !== newValue) {
        // Only audit if the value actually changed
        const auditEntry = {
          id: nanoid(10),
          tcpId: id,
          fieldName: field,
          oldValue: oldValue,
          newValue: newValue,
          step: updates.step || null,
          user_id: userId,
          createdAt: now,
          action: "update",
        };
        try {
          await auditService.create(req.auth.clientId, auditEntry, {
            transaction: req.dbTransaction,
          });
          auditEntries.push(auditEntry);
        } catch (err) {
          // Log but do not fail the patch if audit fails
          console.error("Failed to create audit entry:", err);
        }
      }
    }

    res.json({
      success: true,
      message: "Record updated successfully",
      data: updated,
      audits: auditEntries,
    });
  } catch (error) {
    console.error("Error patching record:", error);
    next(error);
  }
}

function getTcpByReportId(req, res, next) {
  tcpService
    .getTcpByReportId(req.params.id, { transaction: req.dbTransaction })
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

// Bulk create with audit entries for each created record
async function bulkCreate(req, res, next) {
  const transaction = req.dbTransaction;
  try {
    if (!Array.isArray(req.body)) {
      return res
        .status(400)
        .json({ message: "Request body must be an array." });
    }
    const auditService = require("../audit/audit.service");
    const { nanoid: importedNanoid } = await import("nanoid");
    const nanoid = importedNanoid;
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const results = [];
    const auditEntriesAll = [];

    for (const record of req.body) {
      try {
        // Normalise numeric fields
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

        const created = await tcpService.create(record, { transaction });
        results.push(created);

        const now = new Date();
        for (const [field, value] of Object.entries(created)) {
          if (field === "updatedAt") continue;
          if (typeof value !== "undefined") {
            const auditEntry = {
              id: nanoid(10),
              tcpId: created.id,
              fieldName: field,
              oldValue: null,
              newValue: value,
              step: created.step || null,
              user_id: userId,
              createdAt: now,
              action: "create",
            };
            try {
              await auditService.create(clientId, auditEntry, { transaction });
              auditEntriesAll.push(auditEntry);
            } catch (err) {
              console.error("Failed to create audit entry:", err);
            }
          }
        }
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

    await transaction.commit();
    res.json({
      success: true,
      message: "All records saved successfully",
      results,
      audits: auditEntriesAll,
    });
  } catch (error) {
    await transaction.rollback();
    console.error("Error processing bulk records:", error);
    next(error);
  }
}

// PUT: Bulk update with audit trail for each field change
async function bulkUpdate(req, res, next) {
  const transaction = req.dbTransaction;
  try {
    const auditService = require("../audit/audit.service");
    const { nanoid: importedNanoid } = await import("nanoid");
    const nanoid = importedNanoid;
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const records = Object.values(req.body);
    const auditEntriesAll = [];
    const results = [];

    for (const item of records) {
      const itemsToProcess = Array.isArray(item) ? item : [item];
      for (const record of itemsToProcess) {
        try {
          const { id, createdAt, ...recordToUpdate } = record;
          const oldRecord = await tcpService.getById(id, { transaction });
          const updated = await tcpService.update(id, recordToUpdate, {
            transaction,
          });
          results.push(updated);

          const now = new Date();
          for (const [field, newValue] of Object.entries(recordToUpdate)) {
            if (field === "updatedAt") continue;
            const oldValue = oldRecord ? oldRecord[field] : undefined;
            if (oldValue !== newValue) {
              const auditEntry = {
                id: nanoid(10),
                tcpId: id,
                fieldName: field,
                oldValue: oldValue,
                newValue: newValue,
                step: recordToUpdate.step || null,
                user_id: userId,
                createdAt: now,
                action: "update",
              };
              try {
                await auditService.create(clientId, auditEntry, {
                  transaction,
                });
                auditEntriesAll.push(auditEntry);
              } catch (err) {
                console.error("Failed to create audit entry:", err);
              }
            }
          }
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
      audits: auditEntriesAll,
    });
  } catch (error) {
    await transaction.rollback();
    console.error("Error processing bulk records:", error);
    next(error);
  }
}

// DELETE: Delete a record and audit deletion
async function _delete(req, res, next) {
  const transaction = req.dbTransaction;
  try {
    const auditService = require("../audit/audit.service");
    const { nanoid: importedNanoid } = await import("nanoid");
    const nanoid = importedNanoid;
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

    const now = new Date();
    const auditEntries = [];
    if (oldRecord) {
      for (const [field, oldValue] of Object.entries(oldRecord)) {
        if (typeof oldValue !== "undefined") {
          const auditEntry = {
            id: nanoid(10),
            tcpId: id,
            fieldName: field,
            oldValue: oldValue,
            newValue: null,
            step: oldRecord.step || null,
            user_id: userId,
            createdAt: now,
            action: "delete",
          };
          try {
            await auditService.create(clientId, auditEntry, { transaction });
            auditEntries.push(auditEntry);
          } catch (err) {
            console.error("Failed to create audit entry:", err);
          }
        }
      }
    }

    await transaction.commit();
    res.json({
      message: "Tcp deleted successfully",
      audits: auditEntries,
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

// Bulk partial update route handler with audit entries for each updated field
async function bulkPatchUpdate(req, res, next) {
  console.log("Received bulk patch request:", req.body);
  logger.logEvent("info", "Incoming bulk patch request", {
    clientId: req.auth.clientId,
  }); // log start of request
  const transaction = req.dbTransaction;
  try {
    if (!Array.isArray(req.body)) {
      logger.logEvent("warn", "Invalid request body for bulk patch", {
        clientId: req.auth.clientId,
      });
      return res
        .status(400)
        .json({ message: "Request body must be an array." });
    }

    const auditService = require("../audit/audit.service");
    const { nanoid: importedNanoid } = await import("nanoid");
    const nanoid = importedNanoid;
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const auditEntriesAll = [];

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
      const oldRecord = await tcpService.getById(id, {
        transaction,
      });
      const updated = await tcpService.patchRecord(id, filteredUpdates, {
        transaction,
      });
      const now = new Date();
      for (const [field, newValue] of Object.entries(filteredUpdates)) {
        if (field === "updatedAt") continue;
        const oldValue = oldRecord ? oldRecord[field] : undefined;
        if (oldValue !== newValue) {
          const auditEntry = {
            id: nanoid(10),
            tcpId: id,
            fieldName: field,
            oldValue: oldValue,
            newValue: newValue,
            step: step || null,
            createdBy: userId,
            createdAt: now,
            action: "update",
            clientId: clientId,
          };
          try {
            console.log(
              "Creating audit entry for clientId:",
              auditEntry,
              clientId
            );
            // Insert the actual call to create the audit entry
            await auditService.create(auditEntry, { transaction });
            auditEntriesAll.push(auditEntry);
          } catch (err) {
            console.error("Failed to create audit entry:", err);
          }
        }
      }
      return updated;
    });

    const results = await Promise.all(updatePromises);

    await transaction.commit(); // Make sure transaction is committed
    logger.logEvent("info", "Bulk patch update successful", {
      action: "BulkPatchUpdateTCP",
      clientId: req.auth.clientId,
      count: results.length,
      updatedIds: results.map((r) => r.id),
    });

    res.json({
      success: true,
      message: "All records patch updated successfully",
      results,
      audits: auditEntriesAll,
    });
  } catch (error) {
    await transaction.rollback(); // Rollback only if there's an error
    logger.logEvent("error", "Error in bulk patch update", {
      clientId: req.auth.clientId,
      error: error.message,
    });
    console.error("Error bulk patch updating records:", error);
    next(error);
  }
}

const db = require("../db/database");
const { logger } = require("../helpers/logger");
const reportService = require("../reports/report.service");
const { tcpBulkImportSchema } = require("./tcp.validator");
const { sequelize } = require("../db/database");

if (process.env.NODE_ENV !== "test") {
  import("nanoid").then((mod) => {
    nanoid = mod.nanoid;
  });
}

module.exports = {
  getAll,
  getAllByReportId,
  getTcpByReportId,
  sbiUpdate,
  getById,
  create,
  update,
  delete: _delete,
  hasMissingIsSbFlag,
  finaliseReport,
  generateSummaryCsv,
  partialUpdate,
  patchRecord,
  getCurrentFieldValue,
  saveTransformedDataToTcp,
  saveErrorsToTcpError,
  getErrorsByReportId,
};

// Accept optional options param, default to empty object, and pass to sequelize queries for RLS/transactions
async function getAll(options = {}) {
  return await db.Tcp.findAll(options);
}

async function getAllByReportId(reportId, options = {}) {
  return await db.Tcp.findAll({ where: { reportId }, ...options });
}

async function getTcpByReportId(reportId, options = {}) {
  return await db.Tcp.findAll({
    where: {
      reportId,
      isTcp: true,
      excludedTcp: false,
    },
    ...options,
  });
}

async function partialUpdate(id, updates, options = {}) {
  await db.Tcp.update(updates, { where: { id }, ...options });
  logger.logEvent("info", "TCP record partially updated", {
    action: "PartialUpdateTCP",
    tcpId: id,
  });
  return db.Tcp.findOne({ where: { id }, ...options });
}

async function sbiUpdate(reportId, params, options = {}) {
  await db.Tcp.update(
    { isSb: false },
    {
      where: {
        reportId: reportId,
        payeeEntityAbn: params.payeeEntityAbn,
      },
      ...options,
    }
  );
}

async function getById(id, options = {}) {
  return await db.Tcp.findByPk(id, options);
}

async function create(params, options = {}) {
  const result = await db.Tcp.create(params, options);
  logger.logEvent("info", "TCP record created", {
    action: "CreateTCP",
  });
  return result;
}

async function update(id, params, options = {}) {
  await db.Tcp.update(params, { where: { id }, ...options });
  logger.logEvent("info", "TCP record updated", {
    action: "UpdateTCP",
    tcpId: id,
  });
  return db.Tcp.findOne({ where: { id }, ...options });
}

async function _delete(id, options = {}) {
  logger.logEvent("warn", "TCP record deleted", {
    action: "DeleteTCP",
    tcpId: id,
  });
  await db.Tcp.destroy({ where: { id }, ...options });
}

// helper functions
async function getTcp(id, options = {}) {
  const record = await db.Tcp.findOne({ where: { id }, ...options });
  if (!record) throw { status: 404, message: "Tcp not found" };
  return record;
}

// Check if there are any TCP records missing isSb flag (SBI completeness check)
async function hasMissingIsSbFlag(options = {}) {
  const count = await db.Tcp.count({
    where: {
      isTcp: true,
      excludedTcp: false,
      isSb: null,
    },
    ...options,
  });
  return count > 0;
}

// Finalise report: delegate to reportService
async function finaliseReport(options = {}) {
  // If reportService.finaliseSubmission needs transaction, pass options
  return await reportService.finaliseSubmission(options);
}

async function generateSummaryCsv(options = {}) {
  const rows = await db.Tcp.findAll({
    attributes: [
      "payeeEntityName",
      "payeeEntityAbn",
      "paymentAmount",
      "paymentDate",
      "invoiceIssueDate",
      "isSb",
      "paymentTime",
    ],
    where: {
      isTcp: true,
      excludedTcp: false,
    },
    raw: true,
    ...options,
  });

  const header =
    "Payee Name,ABN,Amount,Payment Date,Invoice Date,Is Small Business,Payment Time";
  const csv = [
    header,
    ...rows.map(
      (r) =>
        `"${r.payeeEntityName}","${r.payeeEntityAbn}",${r.paymentAmount},"${r.paymentDate}","${r.invoiceIssueDate}",${r.isSb},${r.paymentTime}`
    ),
  ].join("\n");

  return csv;
}

async function patchRecord(id, update, options = {}) {
  try {
    logger.logEvent("info", "TCP PATCH update requested", {
      action: "patchRecordTCP",
    });
    await db.Tcp.update(update, { where: { id }, ...options });
    logger.logEvent("info", "Bulk PATCH update completed", {
      action: "patchRecordTCP",
    });
    return db.Tcp.findOne({ where: { id }, ...options });
  } catch (error) {
    logger.logEvent("error", "Bulk PATCH update failed", {
      action: "patchRecordTCP",
      error: error.message,
    });
    throw error;
  }
}

async function getCurrentFieldValue(tcpId, field_name, options = {}) {
  // Note: field_name is not parameterized for column names, so validate/sanitize if used from user input!
  // Only allow access to certain fields, or ensure field_name is safe!
  const allowedFields = [
    "payeeEntityName",
    "payeeEntityAbn",
    "paymentAmount",
    "paymentDate",
    "invoiceIssueDate",
    "isSb",
    "paymentTime",
    "isTcp",
    "excludedTcp",
    // Add more allowed fields as needed
  ];
  if (!allowedFields.includes(field_name)) {
    throw new Error("Invalid field_name requested");
  }
  const row = await db.Tcp.findOne({
    attributes: [field_name],
    where: { id: tcpId },
    raw: true,
    ...options,
  });
  if (!row) return null;
  return row[field_name] !== undefined ? row[field_name] : null;
}

/**
 * Save transformed TCP data in bulk.
 * @param {Array} transformedRecords - The records to save.
 * @param {Object} options - Additional Sequelize options (e.g., transaction, RLS).
 */
async function saveTransformedDataToTcp(
  transformedRecords,
  reportId,
  clientId,
  createdBy,
  source = "xero",
  options = {}
) {
  if (!Array.isArray(transformedRecords)) {
    throw new Error("Expected an array of transformed TCP records");
  }

  logger.logEvent("info", "Starting bulk save of transformed TCP records", {
    action: "BulkSaveTCP",
    recordCount: transformedRecords.length,
  });

  // Normalise numeric fields
  transformedRecords.forEach((record) => {
    console.log("Processing record:", record);
    if (typeof record.paymentAmount === "string") {
      record.paymentAmount = parseFloat(
        record.paymentAmount.replace(/[^0-9.-]+/g, "")
      );
    }
  });

  // Normalise date fields
  transformedRecords.forEach((record) => {
    if (
      record.invoiceIssueDate &&
      typeof record.invoiceIssueDate === "string"
    ) {
      record.invoiceIssueDate = new Date(record.invoiceIssueDate);
    }
    if (record.invoiceDueDate && typeof record.invoiceDueDate === "string") {
      record.invoiceDueDate = new Date(record.invoiceDueDate);
    }
    // if (!record.description) {
    //   record.description = "No description provided";
    // }
  });

  // Validate each record using tcpBulkImportSchema
  // Add reportId, clientId, createdBy to each record
  for (let i = 0; i < transformedRecords.length; i++) {
    // console.log(`Validating record at index ${i}:`, transformedRecords[i]);
    transformedRecords[i].createdBy = createdBy;
    transformedRecords[i].reportId = reportId;
    transformedRecords[i].clientId = clientId;
    transformedRecords[i].source = source;
    const { error } = tcpBulkImportSchema.validate(transformedRecords[i]);
    if (error) {
      throw new Error(
        `Validation failed for record at index ${i}: ${error.message}`
      );
      // transformedRecords[i].hasError = true; // Mark as error record
      // transformedRecords[i].errorReason = error.message; // Store error reason
      // logger.logEvent("error", "TCP record validation error", {
      //   action: "BulkSaveTCP",
      //   index: i,
      //   error: error.message,
      // });
    }
  }

  await sequelize.transaction(async (transaction) => {
    await sequelize.query(`SET LOCAL app.current_client_id = '${clientId}'`, {
      transaction,
    });

    // Extract records and transaction from options for bulkCreate
    const { validate = true } = options || {};
    await db.Tcp.bulkCreate(transformedRecords, {
      validate,
      transaction,
    });
  });

  logger.logEvent("info", "✅ Successfully created transformed TCP records", {
    action: "BulkSaveTCP",
    savedCount: transformedRecords.length,
  });

  // Notify frontend via WebSocket, if available
  if (global.sendWebSocketUpdate) {
    global.sendWebSocketUpdate({
      message: "Transformed TCP records saved successfully",
      type: "status",
    });
  }

  return true;
}

/**
 * Save TCP error records in bulk.
 * @param {Array} errorRecords - The records with validation errors to save.
 * @param {String} reportId - The report ID for the batch.
 * @param {String} clientId - The client ID for RLS.
 * @param {String} createdBy - The ID of the user who initiated the upload.
 * @param {String} source - The origin of the data (e.g., 'csv_upload').
 * @param {Object} options - Additional Sequelize options (e.g., transaction).
 */
async function saveErrorsToTcpError(
  errorRecords,
  reportId,
  clientId,
  createdBy,
  source,
  options = {}
) {
  if (!Array.isArray(errorRecords)) {
    throw new Error("Expected an array of TCP error records");
  }

  logger.logEvent("info", "Saving TCP error records", {
    action: "BulkSaveTcpError",
    errorCount: errorRecords.length,
  });

  // Set metadata fields on each error record
  for (let i = 0; i < errorRecords.length; i++) {
    errorRecords[i].createdBy = createdBy;
    errorRecords[i].reportId = reportId;
    errorRecords[i].clientId = clientId;
    errorRecords[i].source = source;
  }

  await sequelize.transaction(async (transaction) => {
    await sequelize.query(`SET LOCAL app.current_client_id = '${clientId}'`, {
      transaction,
    });

    console.log("db.TcpError: ", db.TcpError);

    const { validate = true } = options || {};
    await db.TcpError.bulkCreate(errorRecords, {
      validate,
      transaction,
    });
  });

  logger.logEvent("info", "✅ Successfully saved TCP error records", {
    action: "BulkSaveTcpError",
    savedCount: errorRecords.length,
  });

  return true;
}

async function getErrorsByReportId(reportId, options = {}) {
  return await db.TcpError.findAll({
    where: { reportId },
    ...options,
  });
}

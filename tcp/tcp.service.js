const db = require("../db/database");
const reportService = require("../reports/report.service");
const { tcpBulkImportSchema } = require("./tcp.validator");
const { sequelize } = require("../db/database");
const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");

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

async function getAll(options = {}) {
  return await db.Tcp.findAll(options);
}

async function getAllByReportId(reportId, clientId) {
  const t = await beginTransactionWithClientContext(clientId);
  return await db.Tcp.findAll({ where: { reportId }, transaction: t });
}

async function getTcpByReportId(reportId, clientId) {
  const t = await beginTransactionWithClientContext(clientId);
  return await db.Tcp.findAll({
    where: { reportId },
    transaction: t,
  });
}

async function partialUpdate(id, updates, options = {}) {
  await db.Tcp.update(updates, { where: { id }, ...options });
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

async function getById(id, clientId) {
  const t = await beginTransactionWithClientContext(clientId);
  return await db.Tcp.findByPk(id, { transaction: t });
}

async function create(params, clientId) {
  const t = await beginTransactionWithClientContext(clientId);
  return await db.Tcp.create(params, { transaction: t });
}

async function update(id, params, options = {}) {
  await db.Tcp.update(params, { where: { id }, ...options });
  return db.Tcp.findOne({ where: { id }, ...options });
}

async function _delete(id, options = {}) {
  await db.Tcp.destroy({ where: { id }, ...options });
}

async function getTcp(id, options = {}) {
  const record = await db.Tcp.findOne({ where: { id }, ...options });
  if (!record) throw { status: 404, message: "Tcp not found" };
  return record;
}

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

async function finaliseReport(options = {}) {
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

async function patchRecord(id, update, clientId) {
  const t = await beginTransactionWithClientContext(clientId);
  await db.Tcp.update(update, { where: { id }, transaction: t });
  return db.Tcp.findOne({ where: { id }, transaction: t });
}

async function getCurrentFieldValue(tcpId, field_name, options = {}) {
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
  return row ? row[field_name] : null;
}

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

  transformedRecords.forEach((record) => {
    if (typeof record.paymentAmount === "string") {
      record.paymentAmount = parseFloat(
        record.paymentAmount.replace(/[^0-9.-]+/g, "")
      );
    }
    if (
      record.invoiceIssueDate &&
      typeof record.invoiceIssueDate === "string"
    ) {
      record.invoiceIssueDate = new Date(record.invoiceIssueDate);
    }
    if (record.invoiceDueDate && typeof record.invoiceDueDate === "string") {
      record.invoiceDueDate = new Date(record.invoiceDueDate);
    }
  });

  for (let i = 0; i < transformedRecords.length; i++) {
    transformedRecords[i].createdBy = createdBy;
    transformedRecords[i].reportId = reportId;
    transformedRecords[i].clientId = clientId;
    transformedRecords[i].source = source;
    const { error } = tcpBulkImportSchema.validate(transformedRecords[i]);
    if (error) {
      throw new Error(
        `Validation failed for record at index ${i}: ${error.message}`
      );
    }
  }

  const t = await beginTransactionWithClientContext(clientId);
  try {
    const { validate = true } = options || {};
    await db.Tcp.bulkCreate(transformedRecords, {
      validate,
      transaction: t,
    });
    const insertedRecords = await db.Tcp.findAll({
      where: { reportId },
      transaction: t,
    });
    await t.commit();
    return insertedRecords;
  } catch (err) {
    await t.rollback();
    throw err;
  }
}

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

    const { validate = true } = options || {};
    await db.TcpError.bulkCreate(errorRecords, {
      validate,
      transaction,
    });
  });

  return true;
}

async function getErrorsByReportId(reportId, options = {}) {
  return await db.TcpError.findAll({
    where: { reportId },
    ...options,
  });
}

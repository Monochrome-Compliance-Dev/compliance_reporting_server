const db = require("../helpers/db");
const { logger } = require("../helpers/logger");
const reportService = require("../reports/report.service");
let nanoid = () => "xxxxxxxxxx"; // fallback for test

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
};

async function getAll() {
  return await db.Tcp.findAll();
}

async function getAllByReportId(reportId) {
  return await db.Tcp.findAll({ where: { reportId } });
}

async function getTcpByReportId(reportId) {
  return await db.Tcp.findAll({
    where: {
      reportId,
      isTcp: true,
      excludedTcp: false,
    },
  });
}

async function partialUpdate(id, updates) {
  await db.Tcp.update(updates, { where: { id } });
  logger.logEvent("info", "TCP record partially updated", {
    action: "PartialUpdateTCP",
    tcpId: id,
  });
  return db.Tcp.findOne({ where: { id } });
}

async function sbiUpdate(reportId, params) {
  await db.Tcp.update(
    { isSb: false },
    {
      where: {
        reportId: reportId,
        payeeEntityAbn: params.payeeEntityAbn,
      },
    }
  );
}

async function getById(id) {
  return await getTcp(id);
}

async function create(params) {
  const result = await db.Tcp.create(params);
  logger.logEvent("info", "TCP record created", {
    action: "CreateTCP",
  });
  return result;
}

async function update(id, params) {
  await db.Tcp.update(params, { where: { id } });
  logger.logEvent("info", "TCP record updated", {
    action: "UpdateTCP",
    tcpId: id,
  });
  return db.Tcp.findOne({ where: { id } });
}

async function _delete(id) {
  logger.logEvent("warn", "TCP record deleted", {
    action: "DeleteTCP",
    tcpId: id,
  });
  await db.Tcp.destroy({ where: { id } });
}

// helper functions
async function getTcp(id) {
  const record = await db.Tcp.findOne({ where: { id } });
  if (!record) throw { status: 404, message: "Tcp not found" };
  return record;
}

// Check if there are any TCP records missing isSb flag (SBI completeness check)
async function hasMissingIsSbFlag() {
  const count = await db.Tcp.count({
    where: {
      isTcp: true,
      excludedTcp: false,
      isSb: null,
    },
  });
  return count > 0;
}

// Finalise report: delegate to reportService
async function finaliseReport() {
  return await reportService.finaliseSubmission();
}

async function generateSummaryCsv() {
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

async function patchRecord(id, update) {
  try {
    logger.logEvent("info", "TCP PATCH update requested", {
      action: "patchRecordTCP",
    });
    await db.Tcp.update(update, { where: { id } });
    logger.logEvent("info", "Bulk PATCH update completed", {
      action: "patchRecordTCP",
    });
    return db.Tcp.findOne({ where: { id } });
  } catch (error) {
    logger.logEvent("error", "Bulk PATCH update failed", {
      action: "patchRecordTCP",
      error: error.message,
    });
    throw error;
  }
}

async function getCurrentFieldValue(tcpId, field_name) {
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
  });
  if (!row) return null;
  return row[field_name] !== undefined ? row[field_name] : null;
}

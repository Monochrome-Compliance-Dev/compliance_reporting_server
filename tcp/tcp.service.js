const { cli } = require("winston/lib/winston/config");
const db = require("../helpers/db");
const dbService = require("../helpers/dbService");
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

async function getAll(clientId) {
  const viewName = `client_${clientId}_tbl_tcp`;
  const [rows] = await db.sequelize.query(`SELECT * FROM \`${viewName}\``);
  return rows;
}

async function getAllByReportId(reportId, clientId) {
  const viewName = `client_${clientId}_tbl_tcp`;
  const [rows] = await db.sequelize.query(
    `SELECT * FROM \`${viewName}\` WHERE reportId = ?`,
    { replacements: [reportId] }
  );
  return rows;
}

async function getTcpByReportId(reportId, clientId) {
  const viewName = `client_${clientId}_tbl_tcp`;
  const [rows] = await db.sequelize.query(
    `SELECT * FROM \`${viewName}\` WHERE reportId = ? AND isTcp = true AND excludedTcp = false`,
    { replacements: [reportId] }
  );
  return rows;
}

async function partialUpdate(id, updates, clientId) {
  const result = await dbService.updateRecord(clientId, "tcp", id, updates, db);
  logger.logEvent("info", "TCP record partially updated", {
    action: "PartialUpdateTCP",
    tcpId: id,
    clientId,
  });
  return result;
}

async function sbiUpdate(reportId, params, clientId) {
  const viewName = `client_${clientId}_tbl_tcp`;
  const sql = `
    UPDATE \`${viewName}\` SET isSb = false 
    WHERE reportId = ? AND payeeEntityAbn = ?
  `;
  await db.sequelize.query(sql, {
    replacements: [reportId, params.payeeEntityAbn],
  });
}

async function getById(id, clientId) {
  return await getTcp(id, clientId);
}

async function create(params, clientId) {
  const result = await dbService.createRecord(clientId, "tcp", params, db);
  logger.logEvent("info", "TCP record created", {
    action: "CreateTCP",
    clientId,
  });
  return result;
}

async function update(id, params, clientId) {
  const result = await dbService.updateRecord(clientId, "tcp", id, params, db);
  logger.logEvent("info", "TCP record updated", {
    action: "UpdateTCP",
    tcpId: id,
    clientId,
  });
  return result;
}

async function _delete(id, clientId) {
  logger.logEvent("warn", "TCP record deleted", {
    action: "DeleteTCP",
    tcpId: id,
    clientId,
  });
  await dbService.deleteRecord(clientId, "tcp", id, db);
}

// helper functions
async function getTcp(id, clientId) {
  const viewName = `client_${clientId}_tbl_tcp`;
  const [rows] = await db.sequelize.query(
    `SELECT * FROM \`${viewName}\` WHERE id = ?`,
    {
      replacements: [id],
    }
  );
  if (!rows.length) throw { status: 404, message: "Tcp not found" };
  return rows[0];
}

// Check if there are any TCP records missing isSb flag (SBI completeness check)
async function hasMissingIsSbFlag(clientId) {
  const viewName = `client_${clientId}_tbl_tcp`;
  const [rows] = await db.sequelize.query(
    `SELECT COUNT(*) AS missing FROM \`${viewName}\` WHERE isTcp = true AND excludedTcp = false AND isSb IS NULL`
  );
  return rows[0].missing > 0;
}

// Finalise report: delegate to reportService
async function finaliseReport(clientId) {
  return await reportService.finaliseSubmission(clientId);
}

async function generateSummaryCsv(clientId) {
  const viewName = `client_${clientId}_tbl_tcp`;
  const [rows] = await db.sequelize.query(`
    SELECT payeeEntityName, payeeEntityAbn, paymentAmount, paymentDate, invoiceIssueDate, isSb, paymentTime
    FROM \`${viewName}\`
    WHERE isTcp = true AND excludedTcp = false
  `);

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
  try {
    logger.logEvent("info", "TCP PATCH update requested", {
      action: "patchRecordTCP",
      clientId,
    });

    const result = dbService.patchRecord(clientId, "tcp", id, update, db);

    logger.logEvent("info", "Bulk PATCH update completed", {
      action: "patchRecordTCP",
      clientId,
    });

    return result;
  } catch (error) {
    logger.logEvent("error", "Bulk PATCH update failed", {
      action: "patchRecordTCP",
      clientId,
      error: error.message,
    });
    throw error;
  }
}

async function getCurrentFieldValue(clientId, tcpId, field_name) {
  const viewName = `client_${clientId}_tbl_tcp`;
  const [rows] = await db.sequelize.query(
    `SELECT \`${field_name}\` FROM \`${viewName}\` WHERE id = ? LIMIT 1`,
    { replacements: [tcpId] }
  );
  if (!rows.length) return null;
  return rows[0][field_name] !== undefined ? rows[0][field_name] : null;
}

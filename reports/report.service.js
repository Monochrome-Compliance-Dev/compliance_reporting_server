const db = require("../helpers/db");
const dbService = require("../helpers/dbService");
const logger = require("../helpers/logger");

module.exports = {
  getAll,
  getById,
  create,
  update,
  delete: _delete,
  getAllByReportId,
  finaliseSubmission,
};

async function getAll(clientId) {
  const viewName = `client_${clientId}_tbl_report`;
  const [rows] = await db.sequelize.query(`SELECT * FROM \`${viewName}\``);
  return rows;
}

async function getAllByReportId(reportId, clientId) {
  const viewName = `client_${clientId}_tbl_report`;
  const [rows] = await db.sequelize.query(
    `SELECT * FROM \`${viewName}\` WHERE reportId = ?`,
    { replacements: [reportId] }
  );
  return rows;
}

async function create(clientId, params) {
  return await dbService.createRecord(clientId, "report", params, db);
}

async function update(clientId, id, params) {
  return await dbService.updateRecord(clientId, "report", id, params, db);
}

async function _delete(clientId, id) {
  await dbService.deleteRecord(clientId, "report", id, db);
}

async function getById(id, clientId) {
  const viewName = `client_${clientId}_tbl_report`;
  const [rows] = await db.sequelize.query(
    `SELECT * FROM \`${viewName}\` WHERE id = ?`,
    {
      replacements: [id],
    }
  );
  if (!rows.length) throw { status: 404, message: "Report not found" };
  return rows[0];
}

async function finaliseSubmission(clientId) {
  const viewName = `client_${clientId}_tbl_tcp`;
  const [rows] = await db.sequelize.query(
    `SELECT COUNT(*) AS count FROM \`${viewName}\` WHERE isTcp = true AND excludedTcp = false AND isSb IS NULL`
  );
  if (rows[0].count > 0) {
    throw { status: 400, message: "Some records are missing isSb flags" };
  }

  const [reportIds] = await db.sequelize.query(
    `SELECT DISTINCT reportId FROM \`${viewName}\` WHERE isTcp = true AND excludedTcp = false`
  );

  const now = new Date();
  const updatePayload = {
    reportStatus: "Submitted",
    submittedDate: now,
  };

  for (const { reportId } of reportIds) {
    await dbService.updateRecord(
      clientId,
      "report",
      reportId,
      updatePayload,
      db
    );
  }

  return { success: true, message: "Report(s) marked as Submitted" };
}

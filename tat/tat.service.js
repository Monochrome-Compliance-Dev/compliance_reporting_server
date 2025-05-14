const db = require("../helpers/db");

module.exports = {
  getAll,
  getAllByReportId,
  getTatByReportId,
  sbiUpdate,
  getById,
  create,
  update,
  delete: _delete,
};

async function getAll(clientId) {
  const viewName = `client_${clientId}_tbl_tbl_tat`;
  const [rows] = await db.sequelize.query(`SELECT * FROM \`${viewName}\``);
  return rows;
}

async function getAllByReportId(reportId, clientId) {
  const viewName = `client_${clientId}_tbl_tat`;
  const [rows] = await db.sequelize.query(
    `SELECT * FROM \`${viewName}\` WHERE reportId = ?`,
    { replacements: [reportId] }
  );
  return rows;
}

async function getTatByReportId(reportId, clientId) {
  const viewName = `client_${clientId}_tbl_tat`;
  const [rows] = await db.sequelize.query(
    `SELECT * FROM \`${viewName}\` WHERE reportId = ? AND isTat = true AND excludedTat = false`,
    { replacements: [reportId] }
  );
  return rows;
}

async function sbiUpdate(reportId, params) {
  const viewName = `client_${clientId}_tbl_tbl_tat`;
  const sql = `
    UPDATE \`${viewName}\` SET isSb = false 
    WHERE reportId = ? AND payeeEntityAbn = ?
  `;
  await db.sequelize.query(sql, {
    replacements: [reportId, params.payeeEntityAbn],
  });
}

async function getById(id) {
  return await getTat(id);
}

async function create(params, clientId) {
  return await dbService.createRecord(clientId, "tat", params, db);
}

async function update(id, params, clientId) {
  return await dbService.updateRecord(clientId, "tat", id, params, db);
}

async function _delete(id, clientId) {
  await dbService.deleteRecord(clientId, "tat", id, db);
}

// helper functions
async function getTat(id) {
  const viewName = `client_${clientId}_tbl_tbl_tat`;
  const [rows] = await db.sequelize.query(
    "SELECT * FROM \`${viewName}\` WHERE id = ?",
    {
      replacements: [id],
    }
  );
  if (!rows.length) throw { status: 404, message: "Tat not found" };
  return rows[0];
}

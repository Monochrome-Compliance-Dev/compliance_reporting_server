const db = require("../helpers/db");
const dbService = require("../helpers/dbService");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = {
  getAll,
  getAllByReportId,
  getTcpByReportId,
  sbiUpdate,
  getById,
  create,
  update,
  delete: _delete,
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
  return await dbService.createRecord(clientId, "tcp", params, db);
}

async function update(id, params, clientId) {
  return await dbService.updateRecord(clientId, "tcp", id, params, db);
}

async function _delete(id, clientId) {
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

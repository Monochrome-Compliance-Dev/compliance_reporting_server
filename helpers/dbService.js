const db = require("../helpers/db");
const { logger } = require("../helpers/logger");

let nanoid;

try {
  nanoid = require("nanoid").nanoid;
} catch (err) {
  // fallback during tests or unsupported environments
  nanoid = () => "testid_" + Math.random().toString(36).substring(2, 10);
}

module.exports = {
  createRecord,
  updateRecord,
  deleteRecord,
};

async function createRecord(clientId, tableName, params, db) {
  const viewName = `client_${clientId}_tbl_${tableName}`;

  // Ensure ID is generated and applied directly
  const id = params.id || nanoid(10);
  params.id = id;

  // Add createdAt and updatedAt fields
  params.createdAt = new Date();
  params.updatedAt = new Date();

  const fields = Object.keys(params).join(", ");
  const placeholders = Object.keys(params)
    .map(() => "?")
    .join(", ");
  const values = Object.values(params);

  const sql = `INSERT INTO \`${viewName}\` (${fields}) VALUES (${placeholders})`;
  await db.sequelize.query(sql, { replacements: values });

  const [newRow] = await db.sequelize.query(
    `SELECT * FROM \`${viewName}\` WHERE id = ?`,
    { replacements: [id] }
  );

  logger.logEvent("info", "Record created", {
    action: "CreateRecord",
    table: tableName,
    clientId,
    recordId: id,
  });
  return newRow[0];
}

async function updateRecord(clientId, tableName, id, params, db) {
  const viewName = `client_${clientId}_tbl_${tableName}`;

  // Update the timestamp
  params.updatedAt = new Date();

  const fields = Object.keys(params)
    .map((key) => `${key} = ?`)
    .join(", ");
  const values = [...Object.values(params), id];

  const sql = `UPDATE \`${viewName}\` SET ${fields} WHERE id = ?`;
  logger.logEvent("info", "Executing record update", {
    action: "UpdateRecord",
    table: tableName,
    clientId,
    recordId: id,
    sql,
    values,
  });
  await db.sequelize.query(sql, { replacements: values });

  logger.logEvent("info", "Record updated", {
    action: "UpdateRecord",
    table: tableName,
    clientId,
    recordId: id,
  });
  return { id, ...params };
}

async function deleteRecord(clientId, tableName, id, db) {
  const viewName = `client_${clientId}_tbl_${tableName}`;
  const sql = `DELETE FROM \`${viewName}\` WHERE id = ?`;
  await db.sequelize.query(sql, { replacements: [id] });
  logger.logEvent("warn", "Record deleted", {
    action: "DeleteRecord",
    table: tableName,
    clientId,
    recordId: id,
  });
}

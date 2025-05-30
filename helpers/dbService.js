const db = require("../helpers/db");
const { logger } = require("../helpers/logger");

let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = {
  createRecord,
  updateRecord,
  deleteRecord,
  patchRecord,
};

async function createRecord(clientId, tableName, params, db) {
  const viewName = `tbl_${tableName}`;

  const id = params.id || nanoid(10);
  params.id = id;

  // Ensure clientId is set forcibly
  params.clientId = clientId;
  console.log("-----------------------Client ID:", params.clientId);

  params.createdAt = new Date();
  if (tableName !== "tcp_audit") params.updatedAt = new Date();

  const fields = Object.keys(params).join(", ");
  const placeholders = Object.keys(params)
    .map(() => "?")
    .join(", ");
  const values = Object.values(params);

  const sql = `INSERT INTO \`${viewName}\` (${fields}) VALUES (${placeholders})`;
  console.log("-------------------------Executing SQL:", sql);
  await db.sequelize.query(sql, { replacements: values });

  const [newRow] = await db.sequelize.query(
    `SELECT * FROM \`${viewName}\` WHERE id = ?`,
    { replacements: [id] }
  );

  if (!newRow.length) {
    logger.logEvent("warn", "No record found after insert", {
      action: "CreateRecord",
      table: tableName,
      clientId,
      recordId: id,
    });
    return null;
  }

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

  // Update the timestamp if table is not tcp_audit
  if (tableName !== "tcp_audit") {
    params.updatedAt = new Date();
  }

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

async function patchRecord(clientId, tableName, id, params, db) {
  const viewName = `client_${clientId}_tbl_${tableName}`;

  // Update the timestamp if table is not tcp_audit
  if (tableName !== "tcp_audit") {
    params.updatedAt = new Date();
  }

  const fields = Object.keys(params)
    .map((key) => `${key} = ?`)
    .join(", ");
  const values = [...Object.values(params), id];

  const sql = `UPDATE \`${viewName}\` SET ${fields} WHERE id = ?`;
  logger.logEvent("info", "Executing record patch", {
    action: "PatchRecord",
    table: tableName,
    clientId,
    recordId: id,
    sql,
    values,
  });
  await db.sequelize.query(sql, { replacements: values });

  logger.logEvent("info", "Record patched", {
    action: "PatchRecord",
    table: tableName,
    clientId,
    recordId: id,
  });
  return { id, ...params };
}

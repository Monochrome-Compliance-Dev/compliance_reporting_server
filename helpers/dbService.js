const db = require("../helpers/db");
const winston = require("../helpers/logger");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

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

  // Remove clientId if present in params (view enforces it)
  const { clientId: _, ...sanitizedParams } = params;

  const fields = Object.keys(sanitizedParams).join(", ");
  const placeholders = Object.keys(sanitizedParams)
    .map(() => "?")
    .join(", ");
  const values = Object.values(sanitizedParams);

  const sql = `INSERT INTO \`${viewName}\` (${fields}) VALUES (${placeholders})`;
  console.log("SQL Insert:", sql);
  console.log("Insert Values:", values);
  await db.sequelize.query(sql, { replacements: values });

  const [newRow] = await db.sequelize.query(
    `SELECT * FROM \`${viewName}\` WHERE id = ?`,
    { replacements: [id] }
  );

  winston.info(`Record created in ${viewName}`, { id });
  return newRow[0];
}

async function updateRecord(clientId, tableName, id, params, db) {
  const viewName = `client_${clientId}_tbl_${tableName}`;

  // Remove clientId if present
  const { clientId: _, ...sanitizedParams } = params;

  // Update the timestamp
  sanitizedParams.updatedAt = new Date();

  const fields = Object.keys(sanitizedParams)
    .map((key) => `${key} = ?`)
    .join(", ");
  const values = [...Object.values(sanitizedParams), id];

  const sql = `UPDATE \`${viewName}\` SET ${fields} WHERE id = ?`;
  console.log("SQL Update:", sql);
  console.log("Update Values:", values);
  await db.sequelize.query(sql, { replacements: values });

  winston.info(`Record updated in ${viewName}`, { id });
  return { id, ...sanitizedParams };
}

async function deleteRecord(clientId, tableName, id, db) {
  const viewName = `client_${clientId}_tbl_${tableName}`;
  const sql = `DELETE FROM \`${viewName}\` WHERE id = ?`;
  await db.sequelize.query(sql, { replacements: [id] });
  winston.info(`Record deleted from ${viewName}`, { id });
}

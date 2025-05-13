const config = require("config.json");
const jwt = require("jsonwebtoken");
const bcrypt = require("bcryptjs");
const crypto = require("crypto");
const { Op } = require("sequelize");
const sendEmail = require("../helpers/send-email");
const db = require("../helpers/db");
const Role = require("../helpers/role");

module.exports = {
  getAll,
  getById,
  create,
  update,
  delete: _delete,
};

async function getAll() {
  return await db.Client.findAll();
}

async function getById(id) {
  return await getClient(id);
}

async function create(params) {
  // validate
  if (await db.Client.findOne({ where: { abn: params.abn } })) {
    throw { status: 500, message: "Client with this ABN already exists" };
  }

  // save client
  const client = await db.Client.create(params);

  // Create views for tables locked down to the client
  await createClientViews(client.id);
  return client;
}

async function update(id, params) {
  const client = await getClient(id);

  // validate
  // if (
  //   params.businessName !== client.businessName &&
  //   (await db.Client.findOne({ where: { businessName: params.businessName } }))
  // ) {
  //   throw "Client with this ABN already exists";
  // }

  // copy params to client and save
  Object.assign(client, params);
  await client.save();
}

async function _delete(id) {
  const client = await getClient(id);
  await client.destroy();
}

// helper functions
async function getClient(id) {
  const client = await db.Client.findByPk(id);
  if (!client) throw { status: 404, message: "Client not found" };
  return client;
}

const tablesNames = ["tbl_report", "tbl_tat", "tbl_tcp"];

async function createClientViews(clientId) {
  const results = [];

  for (const tableName of tablesNames) {
    const viewName = `client_${clientId}_${tableName}`;

    const sql = `
      CREATE OR REPLACE VIEW \`${viewName}\` AS
      SELECT * FROM ${tableName} WHERE clientId = ?
    `;

    // Properly quote the clientId value
    const safeSql = sql.replace("?", `'${clientId}'`);
    try {
      await db.sequelize.query(safeSql); // Ensure db.sequelize is properly initialized
      results.push({ tableName, success: true });
    } catch (error) {
      results.push({ tableName, success: false, error: error.message });
    }
  }

  console.log("Client views created:", results);
  if (results.some((result) => !result.success)) {
    throw new Error("Failed to create some client views");
  }

  return results;
}

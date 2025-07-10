const db = require("../db/database");

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

async function getById(clientId, id) {
  const client = await db.Client.findOne({ where: { id, clientId } });
  if (!client) throw { status: 404, message: "Client not found" };
  return client;
}

async function create(clientId, params) {
  if (await db.Client.findOne({ where: { abn: params.abn, clientId } })) {
    throw { status: 500, message: "Client with this ABN already exists" };
  }
  return await db.Client.create({ ...params, clientId });
}

async function update(clientId, id, params) {
  const client = await db.Client.findOne({ where: { id, clientId } });
  if (!client) throw { status: 404, message: "Client not found" };
  Object.assign(client, params);
  await client.save();
}

async function _delete(clientId, id) {
  const client = await db.Client.findOne({ where: { id, clientId } });
  if (!client) throw { status: 404, message: "Client not found" };
  await client.destroy();
}

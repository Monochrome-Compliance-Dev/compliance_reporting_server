const db = require("../helpers/db");
const { logger } = require("../helpers/logger");

module.exports = {
  getAll,
  getById,
  create,
  update,
  delete: _delete,
};

async function getAll() {
  logger.logEvent("info", "Fetching all clients", { action: "GetAllClients" });
  return await db.Client.findAll();
}

async function getById(id) {
  logger.logEvent("info", "Fetching client by ID", {
    action: "GetClient",
  });
  const client = await db.Client.findOne({ where: { id } });
  if (!client) throw { status: 404, message: "Client not found" };
  return client;
}

async function create(params) {
  logger.logEvent("info", "Creating new client", {
    action: "CreateClient",
    abn: params.abn,
  });
  // validate
  if (await db.Client.findOne({ where: { abn: params.abn } })) {
    throw { status: 500, message: "Client with this ABN already exists" };
  }

  // save client
  const client = await db.Client.create(params);

  logger.logEvent("info", "Client created", {
    action: "CreateClient",
  });
  return client;
}

async function update(id, params) {
  logger.logEvent("info", "Updating client", {
    action: "UpdateClient",
    paymentConfirmed: params?.paymentConfirmed,
  });
  const client = await db.Client.findOne({ where: { id } });
  if (!client) throw { status: 404, message: "Client not found" };

  // copy params to client and save
  Object.assign(client, params);
  await client.save();
  logger.logEvent("info", "Client updated", {
    action: "UpdateClient",
  });
}

async function _delete(id) {
  logger.logEvent("info", "Deleting client", {
    action: "DeleteClient",
  });
  const client = await db.Client.findOne({ where: { id } });
  if (!client) throw { status: 404, message: "Client not found" };

  // Delete the client record
  await client.destroy();
  logger.logEvent("warn", "Client deleted", {
    action: "DeleteClient",
  });
}

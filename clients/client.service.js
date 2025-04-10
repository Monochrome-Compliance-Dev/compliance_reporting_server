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
    throw "Client with this ABN already exists";
  }

  // save client
  await db.Client.create(params);
}

async function update(id, params) {
  const client = await getClient(id);

  // validate
  if (
    params.BusinessName !== client.BusinessName &&
    (await db.Client.findOne({ where: { BusinessName: params.BusinessName } }))
  ) {
    throw "Client with this ABN already exists";
  }

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
  if (!client) throw "Client not found";
  return client;
}

async function getEntitiesByABN(abn) {
  const entities = await db.Client.findAll({
    where: {
      ABN: {
        [Op.like]: `%${abn}%`,
      },
    },
  });
  return entities;
}

async function getEntitiesByACN(acn) {
  const entities = await db.Client.findAll({
    where: {
      ACN: {
        [Op.like]: `%${acn}%`,
      },
    },
  });
  return entities;
}

async function getEntitiesByBusinessName(businessName) {
  const entities = await db.Client.findAll({
    where: {
      BusinessName: {
        [Op.like]: `%${businessName}%`,
      },
    },
  });
  return entities;
}

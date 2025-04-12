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
  getAllById,
  getById,
  create,
  update,
  delete: _delete,
};

async function getAll() {
  return await db.Record.findAll();
}

async function getAllById(clientId) {
  // get all records for the client
  const records = await db.Record.findAll({
    where: {
      clientId: clientId,
    },
  });

  if (!records) throw "Records not found";
  return records;
}

async function getById(id) {
  return await getRecord(id);
}

async function create(params) {
  // validate
  // if (await db.Record.findOne({ where: { abn: params.abn } })) {
  //   throw "Record with this ABN already exists";
  // }

  // save record
  await db.Record.create(params);
}

async function update(id, params) {
  const record = await getRecord(id);

  // validate
  if (
    params.BusinessName !== record.BusinessName &&
    (await db.Record.findOne({ where: { BusinessName: params.BusinessName } }))
  ) {
    throw "Record with this ABN already exists";
  }

  // copy params to record and save
  Object.assign(record, params);
  await record.save();
}

async function _delete(id) {
  const record = await getRecord(id);
  await record.destroy();
}

// helper functions
async function getRecord(id) {
  const record = await db.Record.findByPk(id);
  if (!record) throw "Record not found";
  return record;
}

async function getEntitiesByABN(abn) {
  const entities = await db.Record.findAll({
    where: {
      ABN: {
        [Op.like]: `%${abn}%`,
      },
    },
  });
  return entities;
}

async function getEntitiesByACN(acn) {
  const entities = await db.Record.findAll({
    where: {
      ACN: {
        [Op.like]: `%${acn}%`,
      },
    },
  });
  return entities;
}

async function getEntitiesByBusinessName(businessName) {
  const entities = await db.Record.findAll({
    where: {
      BusinessName: {
        [Op.like]: `%${businessName}%`,
      },
    },
  });
  return entities;
}

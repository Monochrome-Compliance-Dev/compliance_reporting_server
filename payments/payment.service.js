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
  return await db.Finance.findAll();
}

async function getById(id) {
  return await getFinance(id);
}

async function create(params) {
  // validate
  if (await db.Finance.findOne({ where: { abn: params.abn } })) {
    throw "Finance with this ABN already exists";
  }

  // save finance
  await db.Finance.create(params);
}

async function update(id, params) {
  const finance = await getFinance(id);

  // validate
  // if (
  //   params.businessName !== finance.businessName &&
  //   (await db.Finance.findOne({ where: { businessName: params.businessName } }))
  // ) {
  //   throw "Finance with this ABN already exists";
  // }

  // copy params to finance and save
  Object.assign(finance, params);
  await finance.save();
}

async function _delete(id) {
  const finance = await getFinance(id);
  await finance.destroy();
}

// helper functions
async function getFinance(id) {
  const finance = await db.Finance.findByPk(id);
  if (!finance) throw "Finance not found";
  return finance;
}

async function getEntitiesByABN(abn) {
  const entities = await db.Finance.findAll({
    where: {
      ABN: {
        [Op.like]: `%${abn}%`,
      },
    },
  });
  return entities;
}

async function getEntitiesByACN(acn) {
  const entities = await db.Finance.findAll({
    where: {
      ACN: {
        [Op.like]: `%${acn}%`,
      },
    },
  });
  return entities;
}

async function getEntitiesByBusinessName(businessName) {
  const entities = await db.Finance.findAll({
    where: {
      BusinessName: {
        [Op.like]: `%${businessName}%`,
      },
    },
  });
  return entities;
}

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
  return await db.Payment.findAll();
}

async function getById(id) {
  return await getPayment(id);
}

async function create(params) {
  // validate
  if (await db.Payment.findOne({ where: { abn: params.abn } })) {
    throw "Payment with this ABN already exists";
  }

  // save payment
  await db.Payment.create(params);
}

async function update(id, params) {
  const payment = await getPayment(id);

  // validate
  // if (
  //   params.businessName !== payment.businessName &&
  //   (await db.Payment.findOne({ where: { businessName: params.businessName } }))
  // ) {
  //   throw "Payment with this ABN already exists";
  // }

  // copy params to payment and save
  Object.assign(payment, params);
  await payment.save();
}

async function _delete(id) {
  const payment = await getPayment(id);
  await payment.destroy();
}

// helper functions
async function getPayment(id) {
  const payment = await db.Payment.findByPk(id);
  if (!payment) throw "Payment not found";
  return payment;
}

async function getEntitiesByABN(abn) {
  const entities = await db.Payment.findAll({
    where: {
      ABN: {
        [Op.like]: `%${abn}%`,
      },
    },
  });
  return entities;
}

async function getEntitiesByACN(acn) {
  const entities = await db.Payment.findAll({
    where: {
      ACN: {
        [Op.like]: `%${acn}%`,
      },
    },
  });
  return entities;
}

async function getEntitiesByBusinessName(businessName) {
  const entities = await db.Payment.findAll({
    where: {
      BusinessName: {
        [Op.like]: `%${businessName}%`,
      },
    },
  });
  return entities;
}

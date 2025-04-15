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
  getByReportId,
  create,
  update,
  delete: _delete,
};

async function getAll() {
  return await db.Finance.findAll();
}

async function getByReportId(id) {
  return await getFinanceByReportId(id);
}

async function create(params) {
  // save finance
  const finance = await db.Finance.create(params);
  if (!finance) {
    throw "Finance creation failed";
  }
  return finance;
}

async function update(id, params) {
  const finance = await getFinance(id);

  // copy params to finance and save
  Object.assign(finance, params);
  await finance.save();
  return finance;
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

async function getFinanceByReportId(id) {
  const finance = await db.Finance.findOne({
    where: {
      reportId: id,
    },
  });
  if (!finance) throw "Finance not found";
  return finance;
}

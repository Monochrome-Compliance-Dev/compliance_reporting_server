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
  return await db.Report.findAll();
}

async function getById(id) {
  return await getReport(id);
}

async function create(params) {
  console.log("Creating Report");
  console.log("Request Body:", params); // Log the request body for debugging
  // validate
  // if (await db.Report.findOne({ where: { abn: params.abn } })) {
  //   throw "Report with this ABN already exists";
  // }

  // save report
  await db.Report.create(params);
}

async function update(id, params) {
  const report = await getReport(id);

  // validate
  if (
    params.BusinessName !== report.BusinessName &&
    (await db.Report.findOne({ where: { BusinessName: params.BusinessName } }))
  ) {
    throw "Report with this ABN already exists";
  }

  // copy params to report and save
  Object.assign(report, params);
  await report.save();
}

async function _delete(id) {
  const report = await getReport(id);
  await report.destroy();
}

// helper functions
async function getReport(id) {
  const report = await db.Report.findByPk(id);
  if (!report) throw "Report not found";
  return report;
}

async function getEntitiesByABN(abn) {
  const entities = await db.Report.findAll({
    where: {
      ABN: {
        [Op.like]: `%${abn}%`,
      },
    },
  });
  return entities;
}

async function getEntitiesByACN(acn) {
  const entities = await db.Report.findAll({
    where: {
      ACN: {
        [Op.like]: `%${acn}%`,
      },
    },
  });
  return entities;
}

async function getEntitiesByBusinessName(businessName) {
  const entities = await db.Report.findAll({
    where: {
      BusinessName: {
        [Op.like]: `%${businessName}%`,
      },
    },
  });
  return entities;
}

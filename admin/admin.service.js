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
  return await db.Admin.findAll();
}

async function getAllById(clientId) {
  // Get all admins for the client
  const admins = await db.Admin.findAll({
    where: {
      clientId: clientId,
    },
  });

  console.log("admins", admins); // Log the result for debugging

  // Return the admins array (even if empty)
  return admins;
}

async function getById(id) {
  return await getAdmin(id);
}

async function create(params) {
  // validate
  // if (await db.Admin.findOne({ where: { abn: params.abn } })) {
  //   throw "Admin with this ABN already exists";
  // }

  // save admin
  await db.Admin.create(params);
}

async function update(id, params) {
  const admin = await getAdmin(id);

  // validate
  if (
    params.BusinessName !== admin.BusinessName &&
    (await db.Admin.findOne({ where: { BusinessName: params.BusinessName } }))
  ) {
    throw "Admin with this ABN already exists";
  }

  // copy params to admin and save
  Object.assign(admin, params);
  await admin.save();
}

async function _delete(id) {
  const admin = await getAdmin(id);
  await admin.destroy();
}

// helper functions
async function getAdmin(id) {
  const admin = await db.Admin.findByPk(id);
  if (!admin) throw "Admin not found";
  return admin;
}

async function getEntitiesByABN(abn) {
  const entities = await db.Admin.findAll({
    where: {
      ABN: {
        [Op.like]: `%${abn}%`,
      },
    },
  });
  return entities;
}

async function getEntitiesByACN(acn) {
  const entities = await db.Admin.findAll({
    where: {
      ACN: {
        [Op.like]: `%${acn}%`,
      },
    },
  });
  return entities;
}

async function getEntitiesByBusinessName(businessName) {
  const entities = await db.Admin.findAll({
    where: {
      BusinessName: {
        [Op.like]: `%${businessName}%`,
      },
    },
  });
  return entities;
}

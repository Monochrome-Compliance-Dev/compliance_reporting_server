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
  return await db.Entity.findAll();
}

async function getById(id) {
  return await getEntity(id);
}

async function create(params) {
  // validate
  if (await db.Entity.findOne({ where: { ABN: params.ABN } })) {
    throw "Entity with this ABN already exists";
  }

  // save entity
  await db.Entity.create(params);
}

async function update(id, params) {
  const entity = await getEntity(id);

  // validate
  if (
    params.BusinessName !== entity.BusinessName &&
    (await db.Entity.findOne({ where: { BusinessName: params.BusinessName } }))
  ) {
    throw "Entity with this ABN already exists";
  }

  // copy params to entity and save
  Object.assign(entity, params);
  await entity.save();
}

async function _delete(id) {
  const entity = await getEntity(id);
  await entity.destroy();
}

// helper functions
async function getEntity(id) {
  const entity = await db.Entity.findByPk(id);
  if (!entity) throw "Entity not found";
  return entity;
}

async function getEntitiesByABN(abn) {
  const entities = await db.Entity.findAll({
    where: {
      ABN: {
        [Op.like]: `%${abn}%`,
      },
    },
  });
  return entities;
}

async function getEntitiesByACN(acn) {
  const entities = await db.Entity.findAll({
    where: {
      ACN: {
        [Op.like]: `%${acn}%`,
      },
    },
  });
  return entities;
}

async function getEntitiesByBusinessName(businessName) {
  const entities = await db.Entity.findAll({
    where: {
      BusinessName: {
        [Op.like]: `%${businessName}%`,
      },
    },
  });
  return entities;
}

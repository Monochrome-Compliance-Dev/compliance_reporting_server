const db = require("../db/database");
const { logger } = require("../helpers/logger");

module.exports = {
  getAll,
  getAllByReportId,
  getById,
  create,
  update,
  delete: _delete,
};

async function getAll() {
  return await db.Entity.findAll();
}

async function getAllByReportId(reportId) {
  const entity = await db.Entity.findAll({
    where: { reportId },
  });
  return entity;
}

async function getById(id) {
  return await getEntity(id);
}

async function create(params) {
  // save entity
  const entity = await db.Entity.create(params);
  logger.logEvent("info", "Entity created", {
    action: "CreateEntity",
    entityId: entity.id,
  });
  return entity;
}

async function update(id, params) {
  // console.log("entityService update", id, params);
  const entity = await getEntity(id);

  // validate
  // if (
  //   params.businessName !== entity.businessName &&
  //   (await db.Entity.findOne({ where: { businessName: params.businessName } }))
  // ) {
  //   throw "Entity with this ABN already exists";
  // }

  // copy params to entity and save
  Object.assign(entity, params);
  const response = await entity.save();
  logger.logEvent("info", "Entity updated", {
    action: "UpdateEntity",
    entityId: entity.id,
    updatedFields: params,
  });
  return response;
}

async function _delete(id) {
  const entity = await getEntity(id);
  await entity.destroy();
  logger.logEvent("warn", "Entity deleted", {
    action: "DeleteEntity",
    entityId: entity.id,
  });
}

// helper functions
async function getEntity(id) {
  const entity = await db.Entity.findByPk(id);
  if (!entity) throw { sentityus: 404, message: "Entity not found" };
  return entity;
}

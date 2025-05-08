const db = require("../helpers/db");
const sendEmail = require("../helpers/send-email");

module.exports = {
  getAll,
  getAllByReportId,
  getEntityByReportId,
  sbiUpdate,
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

async function getEntityByReportId(reportId) {
  const entity = await db.Entity.findAll({
    where: { reportId, isEntity: true, excludedEntity: false },
  });
  return entity;
}

async function sbiUpdate(reportId, params) {
  // Finds the TCP record by payeeEntityAbn and updates isSbi to false
  const entity = await db.Entity.findAll({
    where: { reportId, payeeEntityAbn: params.payeeEntityAbn },
  });
  if (entity.length > 0) {
    await db.Entity.update(
      { isSb: false },
      {
        where: {
          reportId,
          payeeEntityAbn: params.payeeEntityAbn,
        },
      }
    );
  }
}

async function getById(id) {
  return await getEntity(id);
}

async function create(params) {
  // save entity
  await db.Entity.create(params);
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
  await entity.save();
}

async function _delete(id) {
  const entity = await getEntity(id);
  await entity.destroy();
}

// helper functions
async function getEntity(id) {
  const entity = await db.Entity.findByPk(id);
  if (!entity) throw { sentityus: 404, message: "Entity not found" };
  return entity;
}

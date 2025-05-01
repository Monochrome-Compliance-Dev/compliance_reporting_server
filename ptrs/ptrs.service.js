const db = require("../helpers/db");

module.exports = {
  getAll,
  getById,
  create,
  update,
  delete: _delete,
};

async function getAll() {
  return await db.Ptrs.findAll();
}

async function getById(id) {
  return await getPtrs(id);
}

async function create(params) {
  // validate
  if (await db.Ptrs.findOne({ where: { reportId: params.reportId } })) {
    throw { status: 500, message: "Ptrs with this reportId already exists" };
  }

  // save ptrs
  await db.Ptrs.create(params);
}

async function update(id, params) {
  const ptrs = await getPtrs(id);

  // validate
  // if (
  //   params.businessName !== ptrs.businessName &&
  //   (await db.Ptrs.findOne({ where: { businessName: params.businessName } }))
  // ) {
  //   throw "Ptrs with this ABN already exists";
  // }

  // copy params to ptrs and save
  Object.assign(ptrs, params);
  await ptrs.save();
}

async function _delete(id) {
  const ptrs = await getPtrs(id);
  await ptrs.destroy();
}

// helper functions
async function getPtrs(id) {
  const ptrs = await db.Ptrs.findByPk(id);
  if (!ptrs) throw { status: 404, message: "Ptrs not found" };
  return ptrs;
}

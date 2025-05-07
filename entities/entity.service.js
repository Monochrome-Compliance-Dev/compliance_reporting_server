const db = require("../helpers/db");

module.exports = {
  getAll,
  getAllByReportId,
  getTatByReportId,
  sbiUpdate,
  getById,
  create,
  update,
  delete: _delete,
};

async function getAll() {
  return await db.Tat.findAll();
}

async function getAllByReportId(reportId) {
  const tat = await db.Tat.findAll({
    where: { reportId },
  });
  return tat;
}

async function getTatByReportId(reportId) {
  const tat = await db.Tat.findAll({
    where: { reportId, isTat: true, excludedTat: false },
  });
  return tat;
}

async function sbiUpdate(reportId, params) {
  // Finds the TCP record by payeeEntityAbn and updates isSbi to false
  const tat = await db.Tat.findAll({
    where: { reportId, payeeEntityAbn: params.payeeEntityAbn },
  });
  if (tat.length > 0) {
    await db.Tat.update(
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
  return await getTat(id);
}

async function create(params) {
  // save tat
  await db.Tat.create(params);
}

async function update(id, params) {
  // console.log("tatService update", id, params);
  const tat = await getTat(id);

  // validate
  // if (
  //   params.businessName !== tat.businessName &&
  //   (await db.Tat.findOne({ where: { businessName: params.businessName } }))
  // ) {
  //   throw "Tat with this ABN already exists";
  // }

  // copy params to tat and save
  Object.assign(tat, params);
  await tat.save();
}

async function _delete(id) {
  const tat = await getTat(id);
  await tat.destroy();
}

// helper functions
async function getTat(id) {
  const tat = await db.Tat.findByPk(id);
  if (!tat) throw { status: 404, message: "Tat not found" };
  return tat;
}

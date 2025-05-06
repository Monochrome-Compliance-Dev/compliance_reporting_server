const db = require("../helpers/db");

module.exports = {
  getAll,
  getAllByReportId,
  getTcpByReportId,
  sbiUpdate,
  getById,
  create,
  update,
  delete: _delete,
};

async function getAll() {
  return await db.Tcp.findAll();
}

async function getAllByReportId(reportId) {
  const tcp = await db.Tcp.findAll({
    where: { reportId },
  });
  return tcp;
}

async function getTcpByReportId(reportId) {
  const tcp = await db.Tcp.findAll({
    where: { reportId, isTcp: true, excludedTcp: false },
  });
  return tcp;
}

async function sbiUpdate(reportId, params) {
  // Finds the TCP record by payeeEntityAbn and updates isSbi to false
  const tcp = await db.Tcp.findAll({
    where: { reportId, payeeEntityAbn: params.payeeEntityAbn },
  });
  if (tcp.length > 0) {
    await db.Tcp.update(
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
  return await getTcp(id);
}

async function create(params) {
  // save tcp
  await db.Tcp.create(params);
}

async function update(id, params) {
  // console.log("tcpService update", id, params);
  const tcp = await getTcp(id);

  // validate
  // if (
  //   params.businessName !== tcp.businessName &&
  //   (await db.Tcp.findOne({ where: { businessName: params.businessName } }))
  // ) {
  //   throw "Tcp with this ABN already exists";
  // }

  // copy params to tcp and save
  Object.assign(tcp, params);
  await tcp.save();
}

async function _delete(id) {
  const tcp = await getTcp(id);
  await tcp.destroy();
}

// helper functions
async function getTcp(id) {
  const tcp = await db.Tcp.findByPk(id);
  if (!tcp) throw { status: 404, message: "Tcp not found" };
  return tcp;
}

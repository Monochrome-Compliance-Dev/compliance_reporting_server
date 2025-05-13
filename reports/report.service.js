const { Op } = require("sequelize");
const db = require("../helpers/db");

module.exports = {
  getAll,
  getById,
  create,
  update,
  delete: _delete,
};

async function getAll(clientId) {
  const viewName = `client_${clientId}_reports`;
  try {
    const [results] = await db.sequelize.query(`SELECT * FROM \`${viewName}\``);
    if (results.length === 0) {
      return { message: "No reports found for the specified client." };
    }
    return results;
  } catch (error) {
    return {
      message: "An error occurred while fetching reports.",
      error: error.message,
    };
  }
}

async function create(params) {
  const report = await db.Report.create(params);
  if (!report) {
    throw { status: 500, message: "Report creation failed" };
  }
  // return saved report
  return report;
}

async function update(id, params) {
  const report = await getReport(id);

  // copy params to report and save
  Object.assign(report, params);
  await report.save();
  // return updated report
  return report;
}

async function _delete(id) {
  const report = await getReport(id);
  await report.destroy();
}

// helper functions
async function getById(id) {
  const report = await db.Report.findByPk(id);
  if (!report) throw { status: 404, message: "Report not found" };
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

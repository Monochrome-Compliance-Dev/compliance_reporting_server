const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");
const db = require("../db/database");
const { Op } = require("sequelize");

module.exports = {
  createSupplierRisk,
  getSupplierRisks,
  updateSupplierRisk,
  deleteSupplierRisk,
  createTrainingRecord,
  getTrainingRecords,
  updateTrainingRecord,
  deleteTrainingRecord,
  createGrievance,
  updateGrievanceRecord,
  getGrievances,
  deleteGrievance,
  createReportingPeriod,
  getReportingPeriods,
  getReportingPeriodById,
  getInterviewResponses,
  submitInterviewResponses,
  generateStatement,
};

async function createSupplierRisk(clientId, userId, params) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const newSupplierRisk = await db.MSSupplierRisk.create(
      { ...params, clientId, createdBy: userId, updatedBy: userId },
      {
        transaction: t,
      }
    );
    await t.commit();
    return newSupplierRisk;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getSupplierRisks(clientId, startDate, endDate) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const where = {};
    if (startDate && endDate) {
      where.date = {
        [Op.between]: [startDate, endDate],
      };
    }
    const supplierRisks = await db.MSSupplierRisk.findAll({
      where,
      transaction: t,
    });
    await t.commit();
    return supplierRisks;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updateSupplierRisk(clientId, id, params) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const [count, [updatedRisk]] = await db.MSSupplierRisk.update(params, {
      where: { id },
      returning: true,
      transaction: t,
    });
    await t.commit();
    return updatedRisk;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function deleteSupplierRisk(clientId, id) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    await db.MSSupplierRisk.destroy({
      where: { id },
      transaction: t,
    });
    await t.commit();
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function createTrainingRecord(clientId, userId, params) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const newTrainingRecord = await db.MSTraining.create(
      { ...params, clientId, createdBy: userId, updatedBy: userId },
      {
        transaction: t,
      }
    );
    await t.commit();
    return newTrainingRecord;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getTrainingRecords(clientId, startDate, endDate) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const where = {};
    if (startDate && endDate) {
      where.date = {
        [Op.between]: [startDate, endDate],
      };
    }
    const trainingRecords = await db.MSTraining.findAll({
      where,
      transaction: t,
    });
    await t.commit();
    return trainingRecords;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updateTrainingRecord(clientId, recordId, params) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const [count, [updatedRecord]] = await db.MSTraining.update(params, {
      where: { id: recordId },
      returning: true,
      transaction: t,
    });
    await t.commit();
    return updatedRecord;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function deleteTrainingRecord(clientId, id) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    await db.MSTraining.destroy({
      where: { id },
      transaction: t,
    });
    await t.commit();
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function createGrievance(clientId, userId, params) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const newGrievance = await db.MSGrievance.create(
      { ...params, clientId, createdBy: userId, updatedBy: userId },
      {
        transaction: t,
      }
    );
    await t.commit();
    return newGrievance;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getGrievances(clientId, startDate, endDate) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const where = {};
    if (startDate && endDate) {
      where.date = {
        [Op.between]: [startDate, endDate],
      };
    }
    const grievances = await db.MSGrievance.findAll({
      where,
      transaction: t,
    });
    await t.commit();
    return grievances;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updateGrievanceRecord(clientId, id, params) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const [count, [updatedGrievance]] = await db.MSGrievance.update(params, {
      where: { id },
      returning: true,
      transaction: t,
    });
    await t.commit();
    return updatedGrievance;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function deleteGrievance(clientId, id) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    await db.MSGrievance.destroy({
      where: { id },
      transaction: t,
    });
    await t.commit();
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function createReportingPeriod(clientId, userId, params) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const newPeriod = await db.MSReportingPeriod.create(
      { ...params, clientId, createdBy: userId, updatedBy: userId },
      {
        transaction: t,
      }
    );
    await t.commit();
    return newPeriod;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getReportingPeriods(clientId) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const periods = await db.MSReportingPeriod.findAll({ transaction: t });
    await t.commit();
    return periods;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getReportingPeriodById(clientId, id) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const period = await db.MSReportingPeriod.findByPk(id, { transaction: t });
    await t.commit();
    return period;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getInterviewResponses(clientId, reportingPeriodId) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const responses = await db.MSInterviewResponse.findAll({
      where: { reportingPeriodId },
      transaction: t,
    });
    await t.commit();
    return responses;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function submitInterviewResponses(
  clientId,
  userId,
  reportingPeriodId,
  params
) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const responses = await db.MSInterviewResponse.bulkCreate(
      params.map((r) => ({
        ...r,
        reportingPeriodId,
        clientId,
        createdBy: userId,
        updatedBy: userId,
      })),
      { transaction: t }
    );
    await t.commit();
    return responses;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function generateStatement(clientId, reportingPeriodId) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    // Stub implementation for now
    await t.commit();
    return {
      message: "Statement generation is not implemented yet.",
      reportingPeriodId,
    };
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

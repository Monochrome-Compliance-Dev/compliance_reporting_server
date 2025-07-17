const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");
const db = require("../db/database");

module.exports = {
  createSupplierRisk,
  getSupplierRisksByReportingPeriodId,
  deleteSupplierRisk,
  createTrainingRecord,
  getTrainingRecordsByReportingPeriodId,
  deleteTrainingRecord,
  createGrievance,
  getGrievancesByReportingPeriodId,
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

async function getSupplierRisksByReportingPeriodId(
  clientId,
  reportingPeriodId
) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const supplierRisks = await db.MSSupplierRisk.findAll({
      where: { reportingPeriodId },
      transaction: t,
    });
    await t.commit();
    return supplierRisks;
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

async function getTrainingRecordsByReportingPeriodId(
  clientId,
  reportingPeriodId
) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const trainingRecords = await db.MSTraining.findAll({
      where: { reportingPeriodId },
      transaction: t,
    });
    await t.commit();
    return trainingRecords;
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

async function getGrievancesByReportingPeriodId(clientId, reportingPeriodId) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const grievances = await db.MSGrievance.findAll({
      where: { reportingPeriodId },
      transaction: t,
    });
    await t.commit();
    return grievances;
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

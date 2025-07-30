const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");
const db = require("../db/database");
const { Op } = require("sequelize");

module.exports = {
  createSupplierRisk,
  getSupplierRisks,
  getSupplierRiskById,
  updateSupplierRisk,
  deleteSupplierRisk,
  createTraining,
  getTraining,
  getTrainingById,
  updateTraining,
  deleteTraining,
  createGrievance,
  getGrievances,
  getGrievanceById,
  updateGrievance,
  deleteGrievance,
  createReportingPeriod,
  getReportingPeriods,
  getReportingPeriodById,
  getInterviewResponses,
  submitInterviewResponses,
  generateStatement,
  updateTrainingById,
  deleteTrainingById,
  updateSupplierRiskById,
  deleteSupplierRiskById,
  updateGrievanceById,
  deleteGrievanceById,
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
  console.log("clientId, startDate, endDate: ", clientId, startDate, endDate);
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

async function getSupplierRiskById(clientId, id) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const risk = await db.MSSupplierRisk.findOne({
      where: { id },
      transaction: t,
    });
    await t.commit();
    return risk;
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

async function createTraining(clientId, userId, params) {
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

async function getTraining(clientId, startDate, endDate) {
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

async function getTrainingById(clientId, id) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const training = await db.MSTraining.findOne({
      where: { id },
      transaction: t,
    });
    await t.commit();
    return training;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updateTraining(clientId, recordId, params) {
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

async function deleteTraining(clientId, id) {
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

async function getGrievanceById(clientId, id) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const grievance = await db.MSGrievance.findOne({
      where: { id },
      transaction: t,
    });
    await t.commit();
    return grievance;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updateGrievance(clientId, id, params) {
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

async function updateTrainingById(clientId, id, params) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    if (params.completed === false) {
      params.completedAt = null;
    }

    console.log("Updating training record", {
      id,
      params,
    });

    const [count, [updatedRecord]] = await db.MSTraining.update(params, {
      where: { id },
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

async function deleteTrainingById(clientId, id) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const record = await db.MSTraining.findOne({
      where: { id },
      transaction: t,
    });
    if (record) {
      await record.destroy({ transaction: t });
    }
    await t.commit();
    return record;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updateSupplierRiskById(clientId, id, params) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const [count, [updatedRecord]] = await db.MSSupplierRisk.update(params, {
      where: { id },
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

async function deleteSupplierRiskById(clientId, id) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const record = await db.MSSupplierRisk.findOne({
      where: { id },
      transaction: t,
    });
    if (record) {
      await record.destroy({ transaction: t });
    }
    await t.commit();
    return record;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updateGrievanceById(clientId, id, params) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const [count, [updatedRecord]] = await db.MSGrievance.update(params, {
      where: { id },
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

async function deleteGrievanceById(clientId, id) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const record = await db.MSGrievance.findOne({
      where: { id },
      transaction: t,
    });
    if (record) {
      await record.destroy({ transaction: t });
    }
    await t.commit();
    return record;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

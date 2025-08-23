const {
  beginTransactionWithCustomerContext,
} = require("../helpers/setCustomerIdRLS");
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

async function createSupplierRisk(params, options = {}) {
  const { customerId, createdBy, updatedBy, ...rest } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const newSupplierRisk = await db.MSSupplierRisk.create(
      { ...rest, customerId, createdBy, updatedBy },
      {
        transaction: t,
        ...options,
      }
    );
    await t.commit();
    return newSupplierRisk.get({ plain: true });
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getSupplierRisks(customerId, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const where = {};
    if (options.startDate && options.endDate) {
      where.date = {
        [Op.between]: [options.startDate, options.endDate],
      };
    }
    const supplierRisks = await db.MSSupplierRisk.findAll({
      where,
      order: [["createdAt", "DESC"]],
      transaction: t,
    });
    await t.commit();
    return supplierRisks.map((r) => r.get({ plain: true }));
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getSupplierRiskById(customerId, id, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const risk = await db.MSSupplierRisk.findOne({
      where: { id },
      transaction: t,
      ...options,
    });
    await t.commit();
    return risk ? risk.get({ plain: true }) : null;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updateSupplierRisk(params, options = {}) {
  const { customerId, id, updatedBy, ...rest } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const [count, [updatedRisk]] = await db.MSSupplierRisk.update(
      { ...rest, updatedBy },
      {
        where: { id },
        returning: true,
        transaction: t,
        ...options,
      }
    );
    await t.commit();
    return updatedRisk ? updatedRisk.get({ plain: true }) : null;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function deleteSupplierRisk(params, options = {}) {
  const { customerId, id } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    await db.MSSupplierRisk.destroy({
      where: { id },
      transaction: t,
      ...options,
    });
    await t.commit();
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function createTraining(params, options = {}) {
  const { customerId, createdBy, updatedBy, ...rest } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const newTrainingRecord = await db.MSTraining.create(
      { ...rest, customerId, createdBy, updatedBy },
      {
        transaction: t,
        ...options,
      }
    );
    await t.commit();
    return newTrainingRecord.get({ plain: true });
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getTraining(customerId, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const where = {};
    if (options.startDate && options.endDate) {
      where.date = {
        [Op.between]: [options.startDate, options.endDate],
      };
    }
    const trainingRecords = await db.MSTraining.findAll({
      where,
      order: [["createdAt", "DESC"]],
      transaction: t,
    });
    await t.commit();
    return trainingRecords.map((r) => r.get({ plain: true }));
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getTrainingById(customerId, id, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const training = await db.MSTraining.findOne({
      where: { id },
      transaction: t,
      ...options,
    });
    await t.commit();
    return training ? training.get({ plain: true }) : null;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updateTraining(params, options = {}) {
  const { customerId, id, updatedBy, ...rest } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const [count, [updatedRecord]] = await db.MSTraining.update(
      { ...rest, updatedBy },
      {
        where: { id },
        returning: true,
        transaction: t,
        ...options,
      }
    );
    await t.commit();
    return updatedRecord ? updatedRecord.get({ plain: true }) : null;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function deleteTraining(params, options = {}) {
  const { customerId, id } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    await db.MSTraining.destroy({
      where: { id },
      transaction: t,
      ...options,
    });
    await t.commit();
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function createGrievance(params, options = {}) {
  const { customerId, createdBy, updatedBy, ...rest } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const newGrievance = await db.MSGrievance.create(
      { ...rest, customerId, createdBy, updatedBy },
      {
        transaction: t,
        ...options,
      }
    );
    await t.commit();
    return newGrievance.get({ plain: true });
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getGrievances(customerId, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const where = {};
    if (options.startDate && options.endDate) {
      where.date = {
        [Op.between]: [options.startDate, options.endDate],
      };
    }
    const grievances = await db.MSGrievance.findAll({
      where,
      order: [["createdAt", "DESC"]],
      transaction: t,
    });
    await t.commit();
    return grievances.map((r) => r.get({ plain: true }));
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getGrievanceById(customerId, id, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const grievance = await db.MSGrievance.findOne({
      where: { id },
      transaction: t,
      ...options,
    });
    await t.commit();
    return grievance ? grievance.get({ plain: true }) : null;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updateGrievance(params, options = {}) {
  const { customerId, id, updatedBy, ...rest } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const [count, [updatedGrievance]] = await db.MSGrievance.update(
      { ...rest, updatedBy },
      {
        where: { id },
        returning: true,
        transaction: t,
        ...options,
      }
    );
    await t.commit();
    return updatedGrievance ? updatedGrievance.get({ plain: true }) : null;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function deleteGrievance(params, options = {}) {
  const { customerId, id } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    await db.MSGrievance.destroy({
      where: { id },
      transaction: t,
      ...options,
    });
    await t.commit();
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function createReportingPeriod(params, options = {}) {
  const { customerId, createdBy, updatedBy, ...rest } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const newPeriod = await db.MSReportingPeriod.create(
      { ...rest, customerId, createdBy, updatedBy },
      {
        transaction: t,
        ...options,
      }
    );
    await t.commit();
    return newPeriod.get({ plain: true });
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getReportingPeriods(customerId, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const periods = await db.MSReportingPeriod.findAll({
      order: [["createdAt", "DESC"]],
      transaction: t,
      ...options,
    });
    await t.commit();
    return periods.map((r) => r.get({ plain: true }));
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getReportingPeriodById(customerId, id, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const period = await db.MSReportingPeriod.findByPk(id, {
      transaction: t,
      ...options,
    });
    await t.commit();
    return period ? period.get({ plain: true }) : null;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getInterviewResponses(
  customerId,
  reportingPeriodId,
  options = {}
) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const responses = await db.MSInterviewResponse.findAll({
      where: { reportingPeriodId },
      order: [["createdAt", "DESC"]],
      transaction: t,
      ...options,
    });
    await t.commit();
    return responses.map((r) => r.get({ plain: true }));
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function submitInterviewResponses(params, options = {}) {
  const { customerId, createdBy, reportingPeriodId, responses } = params;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const dbResponses = await db.MSInterviewResponse.bulkCreate(
      responses.map((r) => ({
        ...r,
        reportingPeriodId,
        customerId,
        createdBy,
        updatedBy: createdBy,
      })),
      { transaction: t, ...options }
    );
    await t.commit();
    return dbResponses.map((r) => r.get({ plain: true }));
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function generateStatement(customerId, reportingPeriodId, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
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

async function updateTrainingById(customerId, id, params, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const updateParams = { ...params };
    if (updateParams.completed === false) {
      updateParams.completedAt = null;
    }
    const [count, [updatedRecord]] = await db.MSTraining.update(updateParams, {
      where: { id },
      returning: true,
      transaction: t,
      ...options,
    });
    await t.commit();
    return updatedRecord ? updatedRecord.get({ plain: true }) : null;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function deleteTrainingById(customerId, id, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const record = await db.MSTraining.findOne({
      where: { id },
      transaction: t,
      ...options,
    });
    if (record) {
      await record.destroy({ transaction: t });
    }
    await t.commit();
    // Do not return anything for deletes
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updateSupplierRiskById(customerId, id, params, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const [count, [updatedRecord]] = await db.MSSupplierRisk.update(params, {
      where: { id },
      returning: true,
      transaction: t,
      ...options,
    });
    await t.commit();
    return updatedRecord ? updatedRecord.get({ plain: true }) : null;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function deleteSupplierRiskById(customerId, id, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const record = await db.MSSupplierRisk.findOne({
      where: { id },
      transaction: t,
      ...options,
    });
    if (record) {
      await record.destroy({ transaction: t });
    }
    await t.commit();
    // Do not return anything for deletes
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updateGrievanceById(customerId, id, params, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const [count, [updatedRecord]] = await db.MSGrievance.update(params, {
      where: { id },
      returning: true,
      transaction: t,
      ...options,
    });
    await t.commit();
    return updatedRecord ? updatedRecord.get({ plain: true }) : null;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function deleteGrievanceById(customerId, id, options = {}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const record = await db.MSGrievance.findOne({
      where: { id },
      transaction: t,
      ...options,
    });
    if (record) {
      await record.destroy({ transaction: t });
    }
    await t.commit();
    // Do not return anything for deletes
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

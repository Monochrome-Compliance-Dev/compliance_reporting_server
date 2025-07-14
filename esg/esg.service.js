const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");
const db = require("../db/database");

module.exports = {
  createIndicator,
  createMetric,
  getMetricsByClient,
  createReportingPeriod,
  getReportingPeriodsByClient,
  getIndicatorsByReportingPeriodId,
  getMetricsByReportingPeriodId,
  getMetricById,
  deleteIndicator,
  deleteMetric,
  getReportingPeriodById,
  updateReportingPeriod,
  // Units CRUD
  createUnit,
  getUnitsByClient,
  getUnitById,
  updateUnit,
  deleteUnit,
  cloneTemplatesForReportingPeriod,
};

async function createIndicator(params, options = {}) {
  // params may include isTemplate: boolean
  const t = await beginTransactionWithClientContext(params.clientId);
  try {
    const result = await db.ESGIndicator.create(params, {
      ...options,
      transaction: t,
    });

    await t.commit();

    return result.get({ plain: true });
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

async function createMetric(params, options = {}) {
  // params may include isTemplate: boolean
  const t = await beginTransactionWithClientContext(params.clientId);
  try {
    const indicator = await db.ESGIndicator.findByPk(params.indicatorId, {
      transaction: t,
    });
    if (!indicator) {
      throw { status: 400, message: "Invalid indicatorId" };
    }

    const period = await db.ReportingPeriod.findByPk(params.reportingPeriodId, {
      transaction: t,
    });
    if (!period) {
      throw { status: 400, message: "Invalid reportingPeriodId" };
    }

    const result = await db.ESGMetric.create(params, {
      ...options,
      transaction: t,
    });

    await t.commit();

    return result.get({ plain: true });
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getMetricsByClient(clientId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const metrics = await db.ESGMetric.findAll({
      ...options,
      transaction: t,
    });

    return metrics;
  } catch (error) {
    throw error;
  }
}

async function createReportingPeriod(params, options = {}) {
  const t = await beginTransactionWithClientContext(params.clientId);
  try {
    const result = await db.ReportingPeriod.create(params, {
      ...options,
      transaction: t,
    });

    await t.commit();

    return result.get({ plain: true });
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

async function getReportingPeriodsByClient(clientId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const periods = await db.ReportingPeriod.findAll({
      ...options,
      transaction: t,
    });

    return periods;
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getIndicatorsByReportingPeriodId(
  clientId,
  reportingPeriodId,
  options = {}
) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const indicators = await db.ESGIndicator.findAll({
      where: { reportingPeriodId },
      ...options,
      transaction: t,
    });

    return indicators;
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getMetricsByReportingPeriodId(
  clientId,
  reportingPeriodId,
  options = {}
) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const metrics = await db.ESGMetric.findAll({
      where: { reportingPeriodId },
      ...options,
      transaction: t,
    });

    return metrics;
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getMetricById(clientId, metricId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const metric = await db.ESGMetric.findOne({
      where: { id: metricId },
      ...options,
      transaction: t,
    });

    return metric;
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

// Soft delete ESG Indicator by id
async function deleteIndicator(clientId, indicatorId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    await db.ESGIndicator.destroy({
      where: { id: indicatorId },
      ...options,
      transaction: t,
    });

    await t.commit();
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

// Soft delete ESG Metric by id
async function deleteMetric(clientId, metricId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    await db.ESGMetric.destroy({
      where: { id: metricId },
      ...options,
      transaction: t,
    });

    await t.commit();
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

async function getReportingPeriodById(clientId, id, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const period = await db.ReportingPeriod.findByPk(id, {
      ...options,
      transaction: t,
    });

    return period;
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function updateReportingPeriod(clientId, id, updates, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const result = await db.ReportingPeriod.update(updates, {
      where: { id },
      ...options,
      transaction: t,
    });

    await t.commit();
    return result;
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

// ---- Unit CRUD ----
async function createUnit(params, options = {}) {
  const t = await beginTransactionWithClientContext(params.clientId);
  try {
    const result = await db.Unit.create(params, {
      ...options,
      transaction: t,
    });
    await t.commit();
    return result.get({ plain: true });
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

async function getUnitsByClient(clientId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const units = await db.Unit.findAll({
      ...options,
      transaction: t,
    });
    return units;
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getUnitById(clientId, id, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const unit = await db.Unit.findByPk(id, {
      ...options,
      transaction: t,
    });
    return unit;
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function updateUnit(clientId, id, updates, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const result = await db.Unit.update(updates, {
      where: { id },
      ...options,
      transaction: t,
    });
    await t.commit();
    return result;
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

async function deleteUnit(clientId, id, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    await db.Unit.destroy({
      where: { id },
      ...options,
      transaction: t,
    });
    await t.commit();
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

async function cloneTemplatesForReportingPeriod(
  clientId,
  reportingPeriodId,
  options = {}
) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    // Clone ESG Indicators
    const templateIndicators = await db.ESGIndicator.findAll({
      where: { clientId, isTemplate: true },
      transaction: t,
    });

    const clonedIndicators = await Promise.all(
      templateIndicators.map(async (indicator) => {
        const clone = indicator.toJSON();
        delete clone.id;
        clone.reportingPeriodId = reportingPeriodId;
        clone.isTemplate = false;
        return await db.ESGIndicator.create(clone, { transaction: t });
      })
    );

    // Clone ESG Metrics linked to those indicators
    const templateMetrics = await db.ESGMetric.findAll({
      where: { clientId, isTemplate: true },
      transaction: t,
    });

    await Promise.all(
      templateMetrics.map(async (metric) => {
        const clone = metric.toJSON();
        delete clone.id;
        clone.reportingPeriodId = reportingPeriodId;
        clone.isTemplate = false;

        // attempt to match the new cloned indicator by original template's indicatorId
        const matchingIndicator = clonedIndicators.find(
          (ci) => ci.code === metric.indicatorId
        );
        if (matchingIndicator) {
          clone.indicatorId = matchingIndicator.id;
        }

        return await db.ESGMetric.create(clone, { transaction: t });
      })
    );

    await t.commit();
    return { message: "Templates cloned for reporting period." };
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

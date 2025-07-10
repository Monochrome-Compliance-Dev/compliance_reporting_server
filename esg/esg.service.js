const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");
const db = require("../db/database");
const { logger } = require("../helpers/logger");

module.exports = {
  createIndicator,
  createMetric,
  getMetricsByClient,
  createReportingPeriod,
  getReportingPeriodsByClient,
  getIndicatorsByReportingPeriodId,
  getMetricsByReportingPeriodId,
  deleteIndicator,
  deleteMetric,
  getReportingPeriodById,
  updateReportingPeriod,
};

async function createIndicator(params, options = {}) {
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

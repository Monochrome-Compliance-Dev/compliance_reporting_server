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
};

async function createIndicator(params, options = {}) {
  const t = await beginTransactionWithClientContext(params.clientId);
  try {
    const result = await db.ESGIndicator.create(params, {
      ...options,
      transaction: t,
    });

    await t.commit();
    logger.info("ESG Indicator created & committed", {
      action: "CreateESGIndicator",
      ...params,
    });

    return result.get({ plain: true });
  } catch (error) {
    await t.rollback();
    logger.error("Error creating ESG Indicator, rolled back", {
      action: "CreateESGIndicator",
      error: error.message,
    });
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
    logger.info("ESG Metric created & committed", {
      action: "CreateESGMetric",
      ...params,
    });

    return result.get({ plain: true });
  } catch (error) {
    if (!t.finished) await t.rollback();
    logger.error("Error creating ESG Metric, rolled back", {
      action: "CreateESGMetric",
      error: error.message,
    });
    throw error;
  }
}

async function getMetricsByClient(clientId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const metrics = await db.ESGMetric.findAll({
      ...options,
      transaction: t,
    });

    logger.info("Fetched ESG Metrics by client", {
      action: "GetESGMetrics",
      clientId,
      count: Array.isArray(metrics) ? metrics.length : 0,
    });

    return metrics;
  } catch (error) {
    logger.error("Error fetching ESG Metrics by client", {
      action: "GetESGMetrics",
      clientId,
      error: error.message,
    });
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
    logger.info("ESG Reporting Period created & committed", {
      action: "CreateReportingPeriod",
      ...params,
    });

    return result.get({ plain: true });
  } catch (error) {
    await t.rollback();
    logger.error("Error creating ESG Reporting Period, rolled back", {
      action: "CreateReportingPeriod",
      error: error.message,
    });
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

    logger.info("Fetched ESG ReportingPeriods by client", {
      action: "GetReportingPeriods",
      clientId,
      count: Array.isArray(periods) ? periods.length : 0,
    });

    return periods;
  } catch (error) {
    logger.error("Error fetching ESG ReportingPeriods by client", {
      action: "GetReportingPeriods",
      clientId,
      error: error.message,
    });
    throw error;
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

    logger.info("Fetched ESG Indicators by reporting period", {
      action: "GetIndicatorsByReportingPeriod",
      clientId,
      reportingPeriodId,
      count: Array.isArray(indicators) ? indicators.length : 0,
    });

    return indicators;
  } catch (error) {
    logger.error("Error fetching ESG Indicators by reporting period", {
      action: "GetIndicatorsByReportingPeriod",
      clientId,
      reportingPeriodId,
      error: error.message,
    });
    throw error;
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

    logger.info("Fetched ESG Metrics by reporting period", {
      action: "GetMetricsByReportingPeriod",
      clientId,
      reportingPeriodId,
      count: Array.isArray(metrics) ? metrics.length : 0,
    });

    return metrics;
  } catch (error) {
    logger.error("Error fetching ESG Metrics by reporting period", {
      action: "GetMetricsByReportingPeriod",
      clientId,
      reportingPeriodId,
      error: error.message,
    });
    throw error;
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
    logger.info("Soft deleted ESG Indicator", {
      action: "DeleteESGIndicator",
      clientId,
      indicatorId,
    });
  } catch (error) {
    await t.rollback();
    logger.error("Error soft deleting ESG Indicator", {
      action: "DeleteESGIndicator",
      clientId,
      indicatorId,
      error: error.message,
    });
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
    logger.info("Soft deleted ESG Metric", {
      action: "DeleteESGMetric",
      clientId,
      metricId,
    });
  } catch (error) {
    await t.rollback();
    logger.error("Error soft deleting ESG Metric", {
      action: "DeleteESGMetric",
      clientId,
      metricId,
      error: error.message,
    });
    throw error;
  }
}

const db = require("../db/database");
const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");

/**
 * Get total values by ESG category for a client and reporting period.
 * Transaction-aware, rollback on error, and passes client context.
 */
async function getCategoryTotals(clientId, reportingPeriodId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const totals = await db.ESGMetric.findAll({
      attributes: [
        [db.sequelize.col("indicator.category"), "category"],
        [db.sequelize.fn("SUM", db.sequelize.col("value")), "totalValue"],
      ],
      include: [
        {
          model: db.ESGIndicator,
          attributes: [],
          required: true,
        },
      ],
      where: {
        clientId,
        reportingPeriodId,
      },
      group: ["indicator.category"],
      transaction: t,
      ...options,
    });
    await t.commit();
    return totals;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

/**
 * Get all indicators with their latest metrics for a client and reporting period.
 * Transaction-aware and client-context-aware.
 */
async function getAllIndicatorsWithLatestMetrics(
  clientId,
  reportingPeriodId,
  options = {}
) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const data = await db.ESGIndicator.findAll({
      where: { clientId, reportingPeriodId },
      include: [
        {
          model: db.ESGMetric,
          required: false,
        },
      ],
      transaction: t,
      ...options,
    });
    await t.commit();
    return data;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getTotalsByIndicator(clientId, reportingPeriodId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const totals = await db.ESGMetric.findAll({
      attributes: [
        "indicatorId",
        [db.sequelize.fn("SUM", db.sequelize.col("value")), "totalValue"],
      ],
      where: { clientId, reportingPeriodId },
      group: ["indicatorId"],
      transaction: t,
      ...options,
    });
    await t.commit();
    return totals;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

module.exports = {
  getCategoryTotals,
  getAllIndicatorsWithLatestMetrics,
  getTotalsByIndicator,
};

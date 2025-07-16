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
        [db.sequelize.col("ESGIndicator.category"), "category"],
        [db.sequelize.fn("SUM", db.sequelize.col("value")), "totalValue"],
      ],
      include: [
        {
          model: db.ESGIndicator,
          as: "ESGIndicator",
          attributes: [], // no extra fields needed
          required: true,
        },
        {
          model: db.Unit,
          as: "Unit",
          attributes: ["id", "name", "description"],
          required: false,
        },
      ],
      where: { clientId, reportingPeriodId },
      group: [
        "ESGIndicator.category",
        "Unit.id",
        "Unit.name",
        "Unit.description",
      ],
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
      attributes: ["id", "name", "code", "description", "category"],
      where: { clientId, reportingPeriodId },
      include: [
        {
          model: db.ESGMetric,
          attributes: ["id", "value", "unitId", "createdAt"],
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
      include: [
        {
          model: db.ESGIndicator,
          as: "ESGIndicator",
          attributes: ["id", "name", "code", "category"],
          required: true,
        },
        {
          model: db.Unit,
          as: "Unit",
          attributes: ["id", "name", "description"],
          required: false,
        },
      ],
      where: { clientId, reportingPeriodId },
      group: [
        "indicatorId",
        "ESGIndicator.id",
        "ESGIndicator.name",
        "ESGIndicator.code",
        "ESGIndicator.category",
        "Unit.id",
        "Unit.name",
        "Unit.description",
      ],
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

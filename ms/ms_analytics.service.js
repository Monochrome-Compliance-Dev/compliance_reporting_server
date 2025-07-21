const db = require("../db/database");
const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");

module.exports = {
  getSupplierRiskSummary,
  getTrainingCompletionStats,
  getGrievanceSummary,
};

/**
 * Get supplier risk summary: count suppliers grouped by risk across all reporting periods.
 */
async function getSupplierRiskSummary(clientId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const raw = await db.MSSupplierRisk.findAll({
      attributes: [
        [
          db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
          "period",
        ],
        "risk",
        [db.sequelize.fn("COUNT", db.sequelize.col("id")), "count"],
      ],
      where: { clientId },
      group: [
        db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
        "risk",
      ],
      transaction: t,
      ...options,
    });

    await t.commit();

    const grouped = {};
    for (const row of raw) {
      const period = row.dataValues.period;
      const risk = row.risk;
      const count = parseInt(row.dataValues.count, 10);
      if (!grouped[period]) grouped[period] = {};
      grouped[period][risk] = count;
    }

    return Object.entries(grouped).map(([period, summary]) => ({
      period,
      summary,
    }));
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

/**
 * Get training completion stats: counts total vs completed training across all reporting periods.
 */
async function getTrainingCompletionStats(clientId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const total = await db.MSTraining.findAll({
      attributes: [
        [
          db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
          "period",
        ],
        [db.sequelize.fn("COUNT", db.sequelize.col("id")), "total"],
      ],
      where: { clientId },
      group: [
        db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
      ],
      transaction: t,
      ...options,
    });

    const completed = await db.MSTraining.findAll({
      attributes: [
        [
          db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
          "period",
        ],
        [db.sequelize.fn("COUNT", db.sequelize.col("id")), "completed"],
      ],
      where: { clientId, completed: true },
      group: [
        db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
      ],
      transaction: t,
      ...options,
    });

    await t.commit();

    // Combine totals + completed by period
    const summary = total.map((tRow) => {
      const period = tRow.dataValues.period;
      const completedRow = completed.find(
        (c) => c.dataValues.period === period
      );
      return {
        period,
        total: parseInt(tRow.dataValues.total, 10),
        completed: parseInt(completedRow?.dataValues.completed || 0, 10),
      };
    });

    return summary;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

/**
 * Get grievance summary: count grievances grouped by status across all reporting periods.
 */
async function getGrievanceSummary(clientId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const raw = await db.MSGrievance.findAll({
      attributes: [
        [
          db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
          "period",
        ],
        "status",
        [db.sequelize.fn("COUNT", db.sequelize.col("id")), "count"],
      ],
      where: { clientId },
      group: [
        db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
        "status",
      ],
      transaction: t,
      ...options,
    });

    await t.commit();

    const grouped = {};
    for (const row of raw) {
      const period = row.dataValues.period;
      const status = row.status;
      const count = parseInt(row.dataValues.count, 10);
      if (!grouped[period]) grouped[period] = {};
      grouped[period][status] = count;
    }

    return Object.entries(grouped).map(([period, summary]) => ({
      period,
      summary,
    }));
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

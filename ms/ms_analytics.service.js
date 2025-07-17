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
        "reportingPeriodId",
        "risk",
        [db.sequelize.fn("COUNT", db.sequelize.col("id")), "count"],
      ],
      where: { clientId },
      group: ["reportingPeriodId", "risk"],
      transaction: t,
      ...options,
    });

    await t.commit();

    const grouped = {};
    for (const row of raw) {
      const period = row.reportingPeriodId;
      const risk = row.risk;
      const count = parseInt(row.dataValues.count, 10);
      if (!grouped[period]) grouped[period] = {};
      grouped[period][risk] = count;
    }

    return Object.entries(grouped).map(([reportingPeriodId, summary]) => ({
      reportingPeriodId,
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
        "reportingPeriodId",
        [db.sequelize.fn("COUNT", db.sequelize.col("id")), "total"],
      ],
      where: { clientId },
      group: ["reportingPeriodId"],
      transaction: t,
      ...options,
    });

    const completed = await db.MSTraining.findAll({
      attributes: [
        "reportingPeriodId",
        [db.sequelize.fn("COUNT", db.sequelize.col("id")), "completed"],
      ],
      where: { clientId, completed: true },
      group: ["reportingPeriodId"],
      transaction: t,
      ...options,
    });

    await t.commit();

    // Combine totals + completed by reportingPeriodId
    const summary = total.map((tRow) => {
      const completedRow = completed.find(
        (c) => c.reportingPeriodId === tRow.reportingPeriodId
      );
      return {
        reportingPeriodId: tRow.reportingPeriodId,
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
        "reportingPeriodId",
        "status",
        [db.sequelize.fn("COUNT", db.sequelize.col("id")), "count"],
      ],
      where: { clientId },
      group: ["reportingPeriodId", "status"],
      transaction: t,
      ...options,
    });

    await t.commit();

    const grouped = {};
    for (const row of raw) {
      const period = row.reportingPeriodId;
      const status = row.status;
      const count = parseInt(row.dataValues.count, 10);
      if (!grouped[period]) grouped[period] = {};
      grouped[period][status] = count;
    }

    return Object.entries(grouped).map(([reportingPeriodId, summary]) => ({
      reportingPeriodId,
      summary,
    }));
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

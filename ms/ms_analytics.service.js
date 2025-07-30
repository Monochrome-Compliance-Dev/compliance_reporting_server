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
 * Helper to get the last date of the month for a given date.
 */
function getMonthEndDate(date) {
  return new Date(new Date(date.getFullYear(), date.getMonth() + 1, 0));
}

/**
 * Get supplier risk summary: count suppliers grouped by risk, monthly only.
 */
async function getSupplierRiskSummary(clientId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const { startDate, endDate } = options;
    console.log("startDate, endDate :", startDate, endDate);
    const where = { clientId };
    if (startDate && endDate) {
      where.createdAt = { [db.Sequelize.Op.between]: [startDate, endDate] };
    }
    // ---- Monthly summary ----
    const raw = await db.MSSupplierRisk.findAll({
      attributes: [
        [
          db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
          "period",
        ],
        "risk",
        [db.sequelize.fn("COUNT", db.sequelize.col("id")), "count"],
      ],
      where,
      group: [
        db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
        "risk",
      ],
      transaction: t,
      ...options,
    });
    console.log("raw: ", raw);

    await t.commit();

    // --- Monthly grouping ---
    const grouped = {};
    for (const row of raw) {
      const period = row.dataValues.period;
      const risk = row.risk;
      const count = parseInt(row.dataValues.count, 10);
      if (!grouped[period]) grouped[period] = {};
      grouped[period][risk] = count;
    }
    const monthlyArr = Object.entries(grouped).map(([period, summary]) => {
      const startDate = new Date(period).toISOString().slice(0, 10);
      const endDate = getMonthEndDate(new Date(period))
        .toISOString()
        .slice(0, 10);
      return {
        reportingPeriodId: `${startDate}::${endDate}`,
        startDate,
        endDate,
        summary,
      };
    });

    console.log("monthlyArr: ", monthlyArr);
    return monthlyArr;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

/**
 * Get training completion stats: counts total vs completed training, monthly only.
 */
async function getTrainingCompletionStats(clientId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const { startDate, endDate } = options;
    const where = { clientId };
    if (startDate && endDate) {
      where.createdAt = { [db.Sequelize.Op.between]: [startDate, endDate] };
    }
    // --- Monthly ---
    const total = await db.MSTraining.findAll({
      attributes: [
        [
          db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
          "period",
        ],
        [db.sequelize.fn("COUNT", db.sequelize.col("id")), "total"],
      ],
      where,
      group: [
        db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
      ],
      transaction: t,
      ...options,
    });
    const whereCompleted = { ...where, completed: true };
    const completed = await db.MSTraining.findAll({
      attributes: [
        [
          db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
          "period",
        ],
        [db.sequelize.fn("COUNT", db.sequelize.col("id")), "completed"],
      ],
      where: whereCompleted,
      group: [
        db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
      ],
      transaction: t,
      ...options,
    });
    await t.commit();
    // --- Monthly combine ---
    const monthlyArr = total.map((tRow) => {
      const period = tRow.dataValues.period;
      const completedRow = completed.find(
        (c) => c.dataValues.period === period
      );
      const startDate = new Date(period).toISOString().slice(0, 10);
      const endDate = getMonthEndDate(new Date(period))
        .toISOString()
        .slice(0, 10);
      return {
        reportingPeriodId: `${startDate}::${endDate}`,
        startDate,
        endDate,
        summary: {
          total: parseInt(tRow.dataValues.total, 10),
          completed: parseInt(completedRow?.dataValues.completed || 0, 10),
        },
      };
    });
    return monthlyArr;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

/**
 * Get grievance summary: count grievances grouped by status, monthly only.
 */
async function getGrievanceSummary(clientId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const { startDate, endDate } = options;
    const where = { clientId };
    if (startDate && endDate) {
      where.createdAt = { [db.Sequelize.Op.between]: [startDate, endDate] };
    }
    // --- Monthly ---
    const raw = await db.MSGrievance.findAll({
      attributes: [
        [
          db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
          "period",
        ],
        "status",
        [db.sequelize.fn("COUNT", db.sequelize.col("id")), "count"],
      ],
      where,
      group: [
        db.sequelize.fn("DATE_TRUNC", "month", db.sequelize.col("createdAt")),
        "status",
      ],
      transaction: t,
      ...options,
    });
    await t.commit();
    // --- Monthly grouping ---
    const grouped = {};
    for (const row of raw) {
      const period = row.dataValues.period;
      const status = row.status;
      const count = parseInt(row.dataValues.count, 10);
      if (!grouped[period]) grouped[period] = {};
      grouped[period][status] = count;
    }
    const monthlyArr = Object.entries(grouped).map(([period, summary]) => {
      const startDate = new Date(period).toISOString().slice(0, 10);
      const endDate = getMonthEndDate(new Date(period))
        .toISOString()
        .slice(0, 10);
      return {
        reportingPeriodId: `${startDate}::${endDate}`,
        startDate,
        endDate,
        summary,
      };
    });
    return monthlyArr;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

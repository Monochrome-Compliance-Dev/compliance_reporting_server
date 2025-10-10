const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const db = require("@/db/database");
const { logger } = require("@/helpers/logger");

module.exports = {
  getDashboard,
  getTotals,
  getTrackableStatus,
  getWeeklyBurn,
  getOverruns,
  getResourceUtilisation,
  getBillableSplit,
  getRevenueBars,
  getAssignmentTimeliness,
  getTurnaround,
};

/**
 * Returns the full dashboard payload in a single RLS transaction.
 * Mirrors the example service style (audit logs, RLS, try/catch, commit/rollback).
 */
async function getDashboard(orgId, customerId) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    logger.logEvent("info", "Pulse: fetching dashboard payload", {
      action: "PulseGetDashboardPayload",
      orgId,
      customerId,
    });

    const [totals] = await db.sequelize.query("SELECT * FROM v_totals", {
      type: db.sequelize.QueryTypes.SELECT,
      transaction: t,
    });
    const status = await db.sequelize.query(
      "SELECT * FROM v_trackable_status",
      { type: db.sequelize.QueryTypes.SELECT, transaction: t }
    );
    const weeklyBurn = await db.sequelize.query(
      "SELECT * FROM v_weekly_burn ORDER BY week",
      { type: db.sequelize.QueryTypes.SELECT, transaction: t }
    );
    const overruns = await db.sequelize.query("SELECT * FROM v_overruns", {
      type: db.sequelize.QueryTypes.SELECT,
      transaction: t,
    });
    const utilisation = await db.sequelize.query(
      "SELECT * FROM v_resource_utilisation",
      { type: db.sequelize.QueryTypes.SELECT, transaction: t }
    );
    const billableSplit = await db.sequelize.query(
      "SELECT * FROM v_billable_split",
      { type: db.sequelize.QueryTypes.SELECT, transaction: t }
    );
    const [revenueBars] = await db.sequelize.query(
      "SELECT * FROM v_revenue_bars",
      { type: db.sequelize.QueryTypes.SELECT, transaction: t }
    );
    const [timeliness] = await db.sequelize.query(
      "SELECT * FROM v_assignment_timeliness",
      { type: db.sequelize.QueryTypes.SELECT, transaction: t }
    );
    const turnaround = await db.sequelize.query("SELECT * FROM v_turnaround", {
      type: db.sequelize.QueryTypes.SELECT,
      transaction: t,
    });

    await t.commit();

    const payload = {
      totals: totals || { total_budget: 0, total_spend: 0 },
      status,
      weeklyBurn,
      overruns,
      utilisation,
      billableSplit,
      revenueBars: revenueBars || { potential: 0, realised: 0 },
      timeliness: {
        on_time: Number(timeliness?.on_time || 0),
        delayed: Number(timeliness?.delayed || 0),
      },
      turnaround,
    };

    logger.logEvent("info", "Pulse: dashboard payload ready", {
      action: "PulseGetDashboardPayload",
      orgId,
      customerId,
      sizes: {
        status: status?.length || 0,
        weeklyBurn: weeklyBurn?.length || 0,
        overruns: overruns?.length || 0,
        utilisation: utilisation?.length || 0,
        billableSplit: billableSplit?.length || 0,
        turnaround: turnaround?.length || 0,
      },
    });

    return payload;
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Pulse: dashboard payload failed", {
      action: "PulseGetDashboardPayload",
      orgId,
      customerId,
      error: error.message,
    });
    throw error;
  }
}

async function getTotals(orgId, customerId) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    logger.logEvent("info", "Pulse: fetching totals", {
      action: "PulseGetTotals",
      orgId,
      customerId,
    });
    const [row] = await db.sequelize.query("SELECT * FROM v_totals", {
      type: db.sequelize.QueryTypes.SELECT,
      transaction: t,
    });
    await t.commit();
    return row || { total_budget: 0, total_spend: 0 };
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Pulse: totals failed", {
      action: "PulseGetTotals",
      orgId,
      customerId,
      error: error.message,
    });
    throw error;
  }
}

async function getTrackableStatus(orgId, customerId) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    logger.logEvent("info", "Pulse: fetching trackable status", {
      action: "PulseGetStatus",
      orgId,
      customerId,
    });
    const rows = await db.sequelize.query("SELECT * FROM v_trackable_status", {
      type: db.sequelize.QueryTypes.SELECT,
      transaction: t,
    });
    await t.commit();
    return rows;
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Pulse: trackable status failed", {
      action: "PulseGetStatus",
      orgId,
      customerId,
      error: error.message,
    });
    throw error;
  }
}

async function getWeeklyBurn(orgId, customerId) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    logger.logEvent("info", "Pulse: fetching weekly burn", {
      action: "PulseGetWeeklyBurn",
      orgId,
      customerId,
    });
    const rows = await db.sequelize.query(
      "SELECT * FROM v_weekly_burn ORDER BY week",
      {
        type: db.sequelize.QueryTypes.SELECT,
        transaction: t,
      }
    );
    await t.commit();
    return rows;
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Pulse: weekly burn failed", {
      action: "PulseGetWeeklyBurn",
      orgId,
      customerId,
      error: error.message,
    });
    throw error;
  }
}

async function getOverruns(orgId, customerId) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    logger.logEvent("info", "Pulse: fetching overruns", {
      action: "PulseGetOverruns",
      orgId,
      customerId,
    });
    const rows = await db.sequelize.query("SELECT * FROM v_overruns", {
      type: db.sequelize.QueryTypes.SELECT,
      transaction: t,
    });
    await t.commit();
    return rows;
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Pulse: overruns failed", {
      action: "PulseGetOverruns",
      orgId,
      customerId,
      error: error.message,
    });
    throw error;
  }
}

async function getResourceUtilisation(orgId, customerId) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    logger.logEvent("info", "Pulse: fetching resource utilisation", {
      action: "PulseGetUtilisation",
      orgId,
      customerId,
    });
    const rows = await db.sequelize.query(
      "SELECT * FROM v_resource_utilisation",
      {
        type: db.sequelize.QueryTypes.SELECT,
        transaction: t,
      }
    );
    await t.commit();
    return rows;
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Pulse: utilisation failed", {
      action: "PulseGetUtilisation",
      orgId,
      customerId,
      error: error.message,
    });
    throw error;
  }
}

async function getBillableSplit(orgId, customerId) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    logger.logEvent("info", "Pulse: fetching billable split", {
      action: "PulseGetBillableSplit",
      orgId,
      customerId,
    });
    const rows = await db.sequelize.query("SELECT * FROM v_billable_split", {
      type: db.sequelize.QueryTypes.SELECT,
      transaction: t,
    });
    await t.commit();
    return rows;
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Pulse: billable split failed", {
      action: "PulseGetBillableSplit",
      orgId,
      customerId,
      error: error.message,
    });
    throw error;
  }
}

async function getRevenueBars(orgId, customerId) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    logger.logEvent("info", "Pulse: fetching revenue bars", {
      action: "PulseGetRevenue",
      orgId,
      customerId,
    });
    const [row] = await db.sequelize.query("SELECT * FROM v_revenue_bars", {
      type: db.sequelize.QueryTypes.SELECT,
      transaction: t,
    });
    await t.commit();
    return row || { potential: 0, realised: 0 };
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Pulse: revenue bars failed", {
      action: "PulseGetRevenue",
      orgId,
      customerId,
      error: error.message,
    });
    throw error;
  }
}

async function getAssignmentTimeliness(orgId, customerId) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    logger.logEvent("info", "Pulse: fetching assignment timeliness", {
      action: "PulseGetTimeliness",
      orgId,
      customerId,
    });
    const [row] = await db.sequelize.query(
      "SELECT * FROM v_assignment_timeliness",
      {
        type: db.sequelize.QueryTypes.SELECT,
        transaction: t,
      }
    );
    await t.commit();
    return {
      on_time: Number(row?.on_time || 0),
      delayed: Number(row?.delayed || 0),
    };
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Pulse: timeliness failed", {
      action: "PulseGetTimeliness",
      orgId,
      customerId,
      error: error.message,
    });
    throw error;
  }
}

async function getTurnaround(orgId, customerId) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    logger.logEvent("info", "Pulse: fetching turnaround", {
      action: "PulseGetTurnaround",
      orgId,
      customerId,
    });
    const rows = await db.sequelize.query("SELECT * FROM v_turnaround", {
      type: db.sequelize.QueryTypes.SELECT,
      transaction: t,
    });
    await t.commit();
    return rows;
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Pulse: turnaround failed", {
      action: "PulseGetTurnaround",
      orgId,
      customerId,
      error: error.message,
    });
    throw error;
  }
}

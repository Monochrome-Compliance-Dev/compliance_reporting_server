const db = require("../db/database");
const { logger } = require("../helpers/logger");

module.exports = {
  getAll,
  getById,
  create,
  update,
  delete: _delete,
  getAllByReportId,
  finaliseSubmission,
};

async function getAll(options = {}) {
  try {
    const rows = await db.Report.findAll({
      ...options,
      transaction: options.transaction,
    });
    logger.logEvent("info", "Fetched all reports", {
      action: "GetAllReports",
      count: Array.isArray(rows) ? rows.length : undefined,
    });
    return rows;
  } catch (error) {
    logger.logEvent("error", "Error fetching all reports", {
      action: "GetAllReports",
      error: error.message,
    });
    throw error;
  }
}

async function getAllByReportId(reportId, options = {}) {
  try {
    const rows = await db.Report.findAll({
      where: { reportId },
      ...options,
      transaction: options.transaction,
    });
    logger.logEvent("info", "Fetched reports by reportId", {
      action: "GetAllByReportId",
      reportId,
      count: Array.isArray(rows) ? rows.length : undefined,
    });
    return rows;
  } catch (error) {
    logger.logEvent("error", "Error fetching reports by reportId", {
      action: "GetAllByReportId",
      reportId,
      error: error.message,
    });
    throw error;
  }
}

async function create(params, options = {}) {
  console.log("Creating report with data:", params);
  const result = await db.Report.create(params, {
    ...options,
    transaction: options.transaction,
  });
  logger.logEvent("info", "Report created", {
    action: "CreateReport",
    ...params,
  });
  return result;
}

async function update(id, params, options = {}) {
  await db.Report.update(params, {
    where: { id },
    ...options,
    transaction: options.transaction,
  });
  const result = await db.Report.findOne({
    where: { id },
    ...options,
    transaction: options.transaction,
  });
  logger.logEvent("info", "Report updated", {
    action: "UpdateReport",
    reportId: id,
    ...params,
  });
  return result;
}

async function _delete(id, options = {}) {
  await db.Report.destroy({
    where: { id },
    ...options,
    transaction: options.transaction,
  });
  logger.logEvent("warn", "Report deleted", {
    action: "DeleteReport",
    reportId: id,
  });
}

async function getById(id, options = {}) {
  try {
    const report = await db.Report.findOne({
      where: { id },
      ...options,
      transaction: options.transaction,
    });
    if (!report) {
      logger.logEvent("warn", "Report not found", {
        action: "GetReportById",
        reportId: id,
      });
      throw { status: 404, message: "Report not found" };
    }
    logger.logEvent("info", "Fetched report by ID", {
      action: "GetReportById",
      reportId: id,
    });
    return report;
  } catch (error) {
    logger.logEvent("error", "Error fetching report by ID", {
      action: "GetReportById",
      reportId: id,
      error: error.message,
    });
    throw error;
  }
}

async function finaliseSubmission() {
  const viewName = `client_${db.sequelize.config.database}_tbl_tcp`;
  const [rows] = await db.sequelize.query(
    `SELECT COUNT(*) AS count FROM "${viewName}" WHERE isTcp = true AND excludedTcp = false AND isSb IS NULL`
  );
  if (rows[0].count > 0) {
    logger.logEvent(
      "warn",
      "Report submission blocked due to missing isSb flags",
      {
        action: "FinaliseSubmissionBlocked",
      }
    );
    throw { status: 400, message: "Some records are missing isSb flags" };
  }

  const [reportIds] = await db.sequelize.query(
    `SELECT DISTINCT reportId FROM "${viewName}" WHERE isTcp = true AND excludedTcp = false`
  );

  const now = new Date();
  const updatePayload = {
    reportStatus: "Submitted",
    submittedDate: now,
  };

  for (const { reportId } of reportIds) {
    await db.Report.update(updatePayload, {
      where: { id: reportId },
    });
  }

  logger.logEvent("info", "Reports finalised", {
    action: "FinaliseSubmission",
    reportIds: reportIds.map((r) => r.reportId),
  });

  return { success: true, message: "Report(s) marked as Submitted" };
}

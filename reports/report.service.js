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

async function getAll() {
  try {
    const rows = await db.Report.findAll();
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

async function getAllByReportId(reportId) {
  try {
    const rows = await db.Report.findAll({ where: { reportId } });
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

async function create(params) {
  const result = await db.Report.create(params);
  logger.logEvent("info", "Report created", {
    action: "CreateReport",
    ...params,
  });
  return result;
}

async function update(id, params) {
  await db.Report.update(params, { where: { id } });
  const result = await db.Report.findOne({ where: { id } });
  logger.logEvent("info", "Report updated", {
    action: "UpdateReport",
    reportId: id,
    ...params,
  });
  return result;
}

async function _delete(id) {
  await db.Report.destroy({ where: { id } });
  logger.logEvent("warn", "Report deleted", {
    action: "DeleteReport",
    reportId: id,
  });
}

async function getById(id) {
  try {
    const report = await db.Report.findOne({ where: { id } });
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
    await db.Report.update(updatePayload, { where: { id: reportId } });
  }

  logger.logEvent("info", "Reports finalised", {
    action: "FinaliseSubmission",
    reportIds: reportIds.map((r) => r.reportId),
  });

  return { success: true, message: "Report(s) marked as Submitted" };
}

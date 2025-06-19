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
    const whereClause = options.includeDeleted
      ? {}
      : { reportStatus: { [db.Sequelize.Op.ne]: "Deleted" } };

    const rows = await db.Report.findAll({
      where: whereClause,
      ...options,
      transaction: options.transaction,
    });

    logger.logEvent("info", "Fetched all reports", {
      action: "GetAllReports",
      count: Array.isArray(rows) ? rows.length : undefined,
      includeDeleted: options.includeDeleted || false,
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
  const t = await db.sequelize.transaction();
  try {
    await db.sequelize.query(
      `SET LOCAL app.current_client_id = '${params.clientId}'`,
      { transaction: t }
    );

    const result = await db.Report.create(params, {
      ...options,
      transaction: t,
    });

    await t.commit();
    logger.logEvent("info", "Report created & committed", {
      action: "CreateReport",
      ...params,
    });

    return result;
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Error creating report, rolled back", {
      action: "CreateReport",
      error: error.message,
    });
    throw error;
  }
}

async function update(id, params, options = {}) {
  await db.Report.update(params, {
    where: { id },
    ...options,
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

async function _delete(id, clientId, options = {}) {
  const t = await db.sequelize.transaction();
  try {
    await db.sequelize.query(
      `SET LOCAL app.current_client_id = '${clientId}'`,
      { transaction: t }
    );

    const [count] = await db.Report.update(
      { reportStatus: "Deleted" },
      {
        ...options,
        where: { id },
        transaction: t,
      }
    );

    if (count === 0) {
      throw new Error("Report not found or update blocked by RLS.");
    }

    await t.commit();
    logger.logEvent("warn", "Report status set to Deleted", {
      action: "SoftDeleteReport",
      reportId: id,
    });
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Failed to soft delete report", {
      action: "SoftDeleteReport",
      reportId: id,
      error: error.message,
    });
    throw error;
  }
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

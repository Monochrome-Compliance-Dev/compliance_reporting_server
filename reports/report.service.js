const db = require("../helpers/db");
const dbService = require("../helpers/dbService");
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

async function getAll(clientId) {
  const viewName = `client_${clientId}_tbl_report`;
  try {
    const [rows] = await db.sequelize.query(`SELECT * FROM \`${viewName}\``);
    logger.logEvent("info", "Fetched all reports", {
      action: "GetAllReports",
      clientId,
      count: Array.isArray(rows) ? rows.length : undefined,
    });
    return rows;
  } catch (error) {
    logger.logEvent("error", "Error fetching all reports", {
      action: "GetAllReports",
      clientId,
      error: error.message,
    });
    throw error;
  }
}

async function getAllByReportId(reportId, clientId) {
  const viewName = `client_${clientId}_tbl_report`;
  try {
    const [rows] = await db.sequelize.query(
      `SELECT * FROM \`${viewName}\` WHERE reportId = ?`,
      { replacements: [reportId] }
    );
    logger.logEvent("info", "Fetched reports by reportId", {
      action: "GetAllByReportId",
      clientId,
      reportId,
      count: Array.isArray(rows) ? rows.length : undefined,
    });
    return rows;
  } catch (error) {
    logger.logEvent("error", "Error fetching reports by reportId", {
      action: "GetAllByReportId",
      clientId,
      reportId,
      error: error.message,
    });
    throw error;
  }
}

async function create(clientId, params) {
  const result = await dbService.createRecord(clientId, "report", params, db);
  logger.logEvent("info", "Report created", {
    action: "CreateReport",
    clientId,
    ...params,
  });
  console.log("Report created:", result);
  return result;
}

async function update(clientId, id, params) {
  const result = await dbService.updateRecord(
    clientId,
    "report",
    id,
    params,
    db
  );
  logger.logEvent("info", "Report updated", {
    action: "UpdateReport",
    clientId,
    reportId: id,
    ...params,
  });
  return result;
}

async function _delete(clientId, id) {
  await dbService.deleteRecord(clientId, "report", id, db);
  logger.logEvent("warn", "Report deleted", {
    action: "DeleteReport",
    clientId,
    reportId: id,
  });
}

async function getById(id, clientId) {
  const viewName = `client_${clientId}_tbl_report`;
  try {
    const [rows] = await db.sequelize.query(
      `SELECT * FROM \`${viewName}\` WHERE id = ?`,
      {
        replacements: [id],
      }
    );
    if (!rows.length) {
      logger.logEvent("warn", "Report not found", {
        action: "GetReportById",
        clientId,
        reportId: id,
      });
      throw { status: 404, message: "Report not found" };
    }
    logger.logEvent("info", "Fetched report by ID", {
      action: "GetReportById",
      clientId,
      reportId: id,
    });
    return rows[0];
  } catch (error) {
    logger.logEvent("error", "Error fetching report by ID", {
      action: "GetReportById",
      clientId,
      reportId: id,
      error: error.message,
    });
    throw error;
  }
}

async function finaliseSubmission(clientId) {
  const viewName = `client_${clientId}_tbl_tcp`;
  const [rows] = await db.sequelize.query(
    `SELECT COUNT(*) AS count FROM \`${viewName}\` WHERE isTcp = true AND excludedTcp = false AND isSb IS NULL`
  );
  if (rows[0].count > 0) {
    logger.logEvent(
      "warn",
      "Report submission blocked due to missing isSb flags",
      {
        action: "FinaliseSubmissionBlocked",
        clientId,
      }
    );
    throw { status: 400, message: "Some records are missing isSb flags" };
  }

  const [reportIds] = await db.sequelize.query(
    `SELECT DISTINCT reportId FROM \`${viewName}\` WHERE isTcp = true AND excludedTcp = false`
  );

  const now = new Date();
  const updatePayload = {
    reportStatus: "Submitted",
    submittedDate: now,
  };

  for (const { reportId } of reportIds) {
    await dbService.updateRecord(
      clientId,
      "report",
      reportId,
      updatePayload,
      db
    );
  }

  logger.logEvent("info", "Reports finalised", {
    action: "FinaliseSubmission",
    clientId,
    reportIds: reportIds.map((r) => r.reportId),
  });

  return { success: true, message: "Report(s) marked as Submitted" };
}

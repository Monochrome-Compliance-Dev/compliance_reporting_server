const db = require("../helpers/db");
const dbService = require("../helpers/dbService");
const { logger } = require("../helpers/logger");

module.exports = {
  getAll,
  getById,
  create,
};

async function getAll(clientId) {
  try {
    const result = await dbService.getAll(clientId, "tcp_audit", db);
    logger.logEvent("info", "Fetched all audits", {
      action: "GetAllAudits",
      clientId,
      count: Array.isArray(result) ? result.length : undefined,
    });
    return result;
  } catch (error) {
    logger.logEvent("error", "Error fetching all audits", {
      action: "GetAllAudits",
      clientId,
      error: error.message,
    });
    throw error;
  }
}

async function getById(id, clientId) {
  try {
    const result = await dbService.getById(id, clientId, "tcp_audit", db);
    logger.logEvent("info", "Fetched audit by ID", {
      action: "GetAuditById",
      clientId,
      auditId: id,
    });
    return result;
  } catch (error) {
    logger.logEvent("error", "Error fetching audit by ID", {
      action: "GetAuditById",
      clientId,
      auditId: id,
      error: error.message,
    });
    throw error;
  }
}

async function create(clientId, params) {
  try {
    const result = await dbService.createRecord(
      clientId,
      "tcp_audit",
      params,
      db
    );
    logger.logEvent("info", "Audit entry created", {
      action: "CreateAudit",
      clientId,
      auditId: result.id,
    });
    return result;
  } catch (error) {
    logger.logEvent("error", "Error creating audit entry", {
      action: "CreateAudit",
      clientId,
      error: error.message,
    });
    throw error;
  }
}

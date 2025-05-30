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
    // No need to manually add clientId here since dbService.createRecord ensures it
    const result = await dbService.createRecord(
      clientId,
      "tcp_audit",
      params,
      db
    );
    if (!result) {
      logger.logEvent("warn", "No audit entry returned by DB insert", {
        action: "CreateAudit",
        clientId,
        params,
      });
      throw new Error("Audit entry creation failed: no record returned");
    }
    console.log("Audit entry created:", result);
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

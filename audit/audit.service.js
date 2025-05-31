const db = require("../helpers/db");
const { logger } = require("../helpers/logger");

module.exports = {
  getAll,
  getById,
  create,
};

async function getAll() {
  try {
    const result = await db.TcpAudit.findAll();
    logger.logEvent("info", "Fetched all audits", {
      action: "GetAllAudits",
      count: Array.isArray(result) ? result.length : undefined,
    });
    return result;
  } catch (error) {
    logger.logEvent("error", "Error fetching all audits", {
      action: "GetAllAudits",
      error: error.message,
    });
    throw error;
  }
}

async function getById(id) {
  try {
    const result = await db.TcpAudit.findOne({ where: { id } });
    logger.logEvent("info", "Fetched audit by ID", {
      action: "GetAuditById",
      auditId: id,
    });
    return result;
  } catch (error) {
    logger.logEvent("error", "Error fetching audit by ID", {
      action: "GetAuditById",
      auditId: id,
      error: error.message,
    });
    throw error;
  }
}

async function create(params) {
  try {
    const result = await db.TcpAudit.create(params);
    if (!result) {
      logger.logEvent("warn", "No audit entry returned by DB insert", {
        action: "CreateAudit",
        params,
      });
      throw new Error("Audit entry creation failed: no record returned");
    }
    console.log("Audit entry created:", result);
    logger.logEvent("info", "Audit entry created", {
      action: "CreateAudit",
      auditId: result.id,
    });
    return result;
  } catch (error) {
    logger.logEvent("error", "Error creating audit entry", {
      action: "CreateAudit",
      error: error.message,
    });
    throw error;
  }
}

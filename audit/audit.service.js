const db = require("../db/database");
const { logger } = require("../helpers/logger");

module.exports = {
  getAll,
  getById,
  create,
};

async function getAll(options = {}) {
  try {
    const result = await db.Audit.findAll({
      transaction: options.transaction,
      ...options,
    });
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

async function getById(id, options = {}) {
  try {
    const result = await db.Audit.findOne({
      where: { id },
      transaction: options.transaction,
      ...options,
    });
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

async function create(params, options = {}) {
  try {
    const result = await db.Audit.create(params, {
      transaction: options.transaction,
    });
    if (!result) {
      logger.logEvent("warn", "No audit entry returned by DB insert", {
        action: "CreateAudit",
        params,
      });
      throw new Error("Audit entry creation failed: no record returned");
    }
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

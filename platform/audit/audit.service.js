const logger = require("@/helpers/logger");
const auditRepository = require("@/platform/audit/audit.repository");

function writeAuditEvent(auditEvent) {
  if (typeof logger.auditEvent === "function") {
    logger.auditEvent(auditEvent);
    return;
  }

  if (logger.auditLogger && typeof logger.auditLogger.info === "function") {
    logger.auditLogger.info(auditEvent);
    return;
  }

  if (logger.audit && typeof logger.audit.info === "function") {
    logger.audit.info(auditEvent);
    return;
  }

  if (logger.logger && typeof logger.logger.info === "function") {
    logger.logger.info("Audit event", { auditEvent });
    return;
  }

  if (typeof logger.info === "function") {
    logger.info("Audit event", { auditEvent });
    return;
  }

  const error = new Error("Audit logger is not configured.");
  error.status = 500;
  throw error;
}

async function recordFoundationAudit({
  foundationId,
  capability,
  outcome,
  actor,
  request,
}) {
  if (!foundationId) {
    const error = new Error("foundationId is required for audit evidence.");
    error.status = 500;
    throw error;
  }

  const auditEvent = {
    eventType: "platform.foundation.executed",
    foundationId,
    capability,
    outcome,
    actor: {
      id: actor?.id || null,
      role: actor?.role || null,
      customerId: actor?.customerId || null,
    },
    occurredAt: new Date().toISOString(),
  };

  writeAuditEvent(auditEvent);

  await auditRepository.createFoundationAuditEvent({
    foundationId: auditEvent.foundationId,
    capability: auditEvent.capability,
    outcome: auditEvent.outcome,
    actor: auditEvent.actor,
    occurredAt: auditEvent.occurredAt,
    request,
  });

  return auditEvent;
}

module.exports = {
  recordFoundationAudit,
};

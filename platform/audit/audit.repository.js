const crypto = require("crypto");

const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

function createAuditId() {
  return crypto.randomBytes(8).toString("base64url").slice(0, 10);
}

function requireValue(value, message) {
  if (!value) {
    const error = new Error(message);
    error.status = 500;
    throw error;
  }
}

async function createFoundationAuditEvent({
  foundationId,
  capability,
  outcome,
  actor,
  occurredAt,
  request,
  securityObservation,
}) {
  const customerId = actor?.customerId || null;
  const userId = actor?.id || null;

  requireValue(foundationId, "foundationId is required for audit persistence.");
  requireValue(customerId, "customerId is required for audit persistence.");
  requireValue(userId, "userId is required for audit persistence.");

  const transaction = await beginTransactionWithCustomerContext(customerId);

  try {
    const auditRow = {
      id: createAuditId(),
      customerId,
      userId,
      action: "Execute",
      entity: "platform.foundation",
      entityId: foundationId,
      details: {
        eventType: "platform.foundation.executed",
        foundationId,
        capability,
        outcome,
        actor: {
          id: actor?.id || null,
          role: actor?.role || null,
          customerId: actor?.customerId || null,
        },
        occurredAt,
        security: securityObservation || null,
      },
      ip: request?.ip || null,
      device: request?.headers?.["user-agent"] || null,
    };

    const createdAuditEvent = await db.AuditEvent.create(auditRow, {
      transaction,
    });

    await transaction.commit();

    return createdAuditEvent;
  } catch (error) {
    await transaction.rollback();
    throw error;
  }
}

module.exports = {
  createFoundationAuditEvent,
};

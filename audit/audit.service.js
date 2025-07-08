const db = require("../db/database");
const { logger } = require("../helpers/logger");

let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

async function logEvent({
  clientId,
  userId,
  action,
  entity,
  entityId,
  details = {},
  transaction = null,
}) {
  try {
    const auditRecord = await db.AuditEvent.create(
      {
        id: nanoid(10),
        clientId,
        userId,
        action,
        entity,
        entityId,
        details,
      },
      { transaction }
    );

    logger.info("Audit event created", {
      action,
      entity,
      entityId,
      clientId,
      userId,
    });

    return auditRecord;
  } catch (err) {
    logger.error("Failed to create audit event", {
      error: err.message,
      action,
      entity,
      entityId,
      clientId,
      userId,
    });
    throw err;
  }
}

module.exports = {
  logEvent,
};

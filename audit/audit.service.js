const db = require("../db/database");
const { logger } = require("../helpers/logger");
const { auditFieldConfig } = require("./audit_config");

let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

function generateDiff(before = {}, after = {}, fields = []) {
  // console.log("generateDiff before: ", before);
  // console.log("generateDiff after: ", after);
  // console.log("generateDiff fields: ", fields);
  const diff = { before: {}, after: {} };

  fields.forEach((field) => {
    if (before[field] !== after[field]) {
      diff.before[field] = before[field];
      diff.after[field] = after[field];
    }
  });

  return diff;
}

async function logEvent({
  clientId,
  userId,
  action,
  entity,
  entityId,
  details = {},
  ip,
  device,
  transaction = null,
}) {
  try {
    if (
      action === "Update" &&
      details &&
      typeof details === "object" &&
      auditFieldConfig[entity]
    ) {
      details = generateDiff(
        details.before,
        details.after,
        auditFieldConfig[entity]
      );
    }
    const auditRecord = await db.AuditEvent.create(
      {
        id: nanoid(10),
        clientId,
        userId,
        action,
        entity,
        entityId,
        details,
        ip,
        device,
      },
      { transaction }
    );

    logger.info("Audit event created", {
      action,
      entity,
      entityId,
      clientId,
      userId,
      ip,
      device,
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
      ip,
      device,
    });
    throw err;
  }
}

function withAudit({
  clientId,
  userId,
  action,
  entity,
  entityId,
  before,
  after,
  ip,
  device,
  transaction = null,
}) {
  const fields = auditFieldConfig[entity];
  const details =
    action === "Update"
      ? generateDiff(before, after, fields || [])
      : action === "Create"
        ? { after }
        : action === "Delete"
          ? { before }
          : {};

  return logEvent({
    clientId,
    userId,
    action,
    entity,
    entityId,
    details,
    ip,
    device,
    transaction,
  });
}

function auditGet(action, entity) {
  return function (req, res, next) {
    const originalSend = res.send;
    res.send = function (body) {
      res.send = originalSend;
      res.send(body);

      const count = Array.isArray(body)
        ? body.length
        : body && typeof body === "object" && body.count
          ? body.count
          : undefined;

      logEvent({
        clientId: req.auth?.clientId,
        userId: req.auth?.id,
        action,
        entity,
        entityId: null,
        details: { count },
        ip: req.ip,
        device: req.headers["user-agent"],
      }).catch((err) =>
        logger.error("Audit log failed from auditGet middleware", {
          error: err.message,
        })
      );
    };
    next();
  };
}

module.exports = {
  logEvent,
  generateDiff,
  withAudit,
  auditGet,
};

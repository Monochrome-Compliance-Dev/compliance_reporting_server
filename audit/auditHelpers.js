const { withAudit } = require("./audit.service");

async function logCreateAudit({
  entity,
  customerId,
  userId,
  result,
  req,
  action,
}) {
  const ip = req.ip;
  const device = req.headers["user-agent"];

  await withAudit({
    customerId,
    userId,
    action,
    entity: entity.toLowerCase(),
    entityId: result?.id,
    after:
      typeof result.get === "function" ? result.get({ plain: true }) : result,
    ip,
    device,
  });

  return result;
}

async function logReadAudit({
  entity,
  customerId,
  userId,
  req,
  result,
  action,
}) {
  const ip = req.ip;
  const device = req.headers["user-agent"];

  await withAudit({
    customerId,
    userId,
    action,
    entity: entity.toLowerCase(),
    entityId: null,
    details: { count: Array.isArray(result) ? result.length : undefined },
    ip,
    device,
  });
}

async function logUpdateAudit({
  entity,
  customerId,
  userId,
  entityId,
  before,
  after,
  req,
  action,
}) {
  const ip = req.ip;
  const device = req.headers["user-agent"];

  await withAudit({
    customerId,
    userId,
    action,
    entity: entity.toLowerCase(),
    entityId,
    before,
    after,
    ip,
    device,
  });
}

async function logDeleteAudit({
  entity,
  customerId,
  userId,
  entityId,
  before,
  req,
  action,
}) {
  const ip = req.ip;
  const device = req.headers["user-agent"];

  await withAudit({
    customerId,
    userId,
    action,
    entity: entity.toLowerCase(),
    entityId,
    before,
    ip,
    device,
  });
}

module.exports = {
  logCreateAudit,
  logReadAudit,
  logUpdateAudit,
  logDeleteAudit,
};

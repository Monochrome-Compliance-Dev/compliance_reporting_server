const crypto = require("crypto");

const auditService = require("@/platform/audit/audit.service");

function getActor(req) {
  return req.user || req.auth || req.currentUser || null;
}

async function executeFoundation(req) {
  const actor = getActor(req);

  if (!actor) {
    const error = new Error("Authenticated user context is required.");
    error.status = 401;
    throw error;
  }

  const result = {
    success: true,
    foundationId: crypto.randomUUID(),
    capability: "foundation",
    message: "Platform foundation executed successfully.",
    actor: {
      id: actor.id || actor.userId || null,
      role: actor.role || null,
      customerId: actor.customerId || null,
    },
  };

  await auditService.recordFoundationAudit({
    foundationId: result.foundationId,
    capability: result.capability,
    outcome: "success",
    actor: result.actor,
    request: req,
  });

  return result;
}

module.exports = {
  executeFoundation,
};

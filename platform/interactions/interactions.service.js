const crypto = require("crypto");

const auditService = require("@/platform/audit/audit.service");

function getActor(req) {
  return req.user || req.auth || req.currentUser || null;
}

function executeInteraction(req) {
  const actor = getActor(req);

  if (!actor) {
    const error = new Error("Authenticated user context is required.");
    error.status = 401;
    throw error;
  }

  const result = {
    success: true,
    interactionId: crypto.randomUUID(),
    capability: "interactions",
    message: "Platform interaction executed successfully.",
    actor: {
      id: actor.id || actor.userId || null,
      role: actor.role || null,
      customerId: actor.customerId || null,
    },
  };

  auditService.recordInteractionAudit({
    interactionId: result.interactionId,
    capability: result.capability,
    outcome: "success",
    actor: result.actor,
  });

  return result;
}

module.exports = {
  executeInteraction,
};

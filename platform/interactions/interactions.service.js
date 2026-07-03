const crypto = require("crypto");

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

  return {
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
}

module.exports = {
  executeInteraction,
};

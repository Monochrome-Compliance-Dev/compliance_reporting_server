function requireValue(value, message) {
  if (!value) {
    const error = new Error(message);
    error.status = 500;
    throw error;
  }
}

function normaliseActor(actor) {
  return {
    id: actor?.id || actor?.userId || null,
    role: actor?.role || null,
    customerId: actor?.customerId || null,
  };
}

function normaliseRequest(request) {
  return {
    method: request?.method || null,
    path: request?.originalUrl || request?.path || null,
    ip: request?.ip || null,
    userAgent: request?.headers?.["user-agent"] || null,
  };
}

function observeFoundationCommand({ foundationId, actor, request }) {
  requireValue(
    foundationId,
    "foundationId is required for security observation.",
  );

  const normalisedActor = normaliseActor(actor);

  requireValue(
    normalisedActor.id,
    "actor id is required for security observation.",
  );
  requireValue(
    normalisedActor.customerId,
    "customerId is required for security observation.",
  );

  return {
    eventType: "platform.security.foundation_observed",
    outcome: "allowed",
    capability: "foundation",
    foundationId,
    actor: normalisedActor,
    request: normaliseRequest(request),
    occurredAt: new Date().toISOString(),
  };
}

module.exports = {
  observeFoundationCommand,
};

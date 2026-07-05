const ALLOWED_DENIAL_REASONS = [
  "missing_customer_context",
  "role_not_allowed",
  "customer_mismatch",
];

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

function buildFoundationSecurityObservation({
  foundationId,
  actor,
  request,
  outcome,
  reason,
}) {
  requireValue(
    foundationId,
    "foundationId is required for security observation.",
  );

  const normalisedActor = normaliseActor(actor);

  requireValue(
    normalisedActor.id,
    "actor id is required for security observation.",
  );

  return {
    eventType: "platform.security.foundation_observed",
    outcome,
    capability: "foundation",
    foundationId,
    actor: normalisedActor,
    request: normaliseRequest(request),
    reason: reason || null,
    occurredAt: new Date().toISOString(),
  };
}

function observeFoundationCommand({ foundationId, actor, request }) {
  const normalisedActor = normaliseActor(actor);

  requireValue(
    normalisedActor.customerId,
    "customerId is required for security observation.",
  );

  return buildFoundationSecurityObservation({
    foundationId,
    actor: normalisedActor,
    request,
    outcome: "allowed",
  });
}

function observeDeniedFoundationCommand({
  foundationId,
  actor,
  request,
  reason,
}) {
  requireValue(reason, "reason is required for denied security observation.");

  if (!ALLOWED_DENIAL_REASONS.includes(reason)) {
    const error = new Error("Unsupported denied security observation reason.");
    error.status = 500;
    throw error;
  }

  return buildFoundationSecurityObservation({
    foundationId,
    actor,
    request,
    outcome: "denied",
    reason,
  });
}

module.exports = {
  observeDeniedFoundationCommand,
  observeFoundationCommand,
};

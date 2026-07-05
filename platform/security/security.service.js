const ALLOWED_DENIAL_REASONS = [
  "missing_customer_context",
  "role_not_allowed",
  "customer_mismatch",
];

const DEFAULT_FOUNDATION_REQUIRED_ROLES = ["Admin", "Boss", "User"];
const DEFAULT_DATA_DATASET_REQUIRED_ROLES = ["Admin", "Boss", "User"];

function requireValue(value, message) {
  if (!value) {
    const error = new Error(message);
    error.status = 500;
    throw error;
  }
}

function createSecurityDenialError(message, securityObservation) {
  const error = new Error(message);
  error.status = 403;
  error.securityObservation = securityObservation;
  return error;
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

function assertSupportedDenialReason(reason) {
  requireValue(reason, "reason is required for denied security observation.");

  if (!ALLOWED_DENIAL_REASONS.includes(reason)) {
    const error = new Error("Unsupported denied security observation reason.");
    error.status = 500;
    throw error;
  }
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
  assertSupportedDenialReason(reason);

  return buildFoundationSecurityObservation({
    foundationId,
    actor,
    request,
    outcome: "denied",
    reason,
  });
}

function enforceFoundationCommand({
  foundationId,
  actor,
  request,
  requiredRoles = DEFAULT_FOUNDATION_REQUIRED_ROLES,
}) {
  requireValue(
    requiredRoles,
    "requiredRoles is required for security enforcement.",
  );

  const normalisedActor = normaliseActor(actor);

  requireValue(
    normalisedActor.customerId,
    "customerId is required for security enforcement.",
  );

  if (!requiredRoles.includes(normalisedActor.role)) {
    const securityObservation = observeDeniedFoundationCommand({
      foundationId,
      actor: normalisedActor,
      request,
      reason: "role_not_allowed",
    });

    throw createSecurityDenialError(
      "Role is not allowed for governed execution.",
      securityObservation,
    );
  }

  return observeFoundationCommand({
    foundationId,
    actor: normalisedActor,
    request,
  });
}

function buildDataDatasetSecurityObservation({
  datasetId,
  actor,
  request,
  outcome,
  reason,
}) {
  requireValue(datasetId, "datasetId is required for security observation.");

  const normalisedActor = normaliseActor(actor);

  requireValue(
    normalisedActor.id,
    "actor id is required for security observation.",
  );

  return {
    eventType: "platform.security.data_dataset_observed",
    outcome,
    capability: "data",
    action: "dataset.create",
    datasetId,
    actor: normalisedActor,
    request: normaliseRequest(request),
    reason: reason || null,
    occurredAt: new Date().toISOString(),
  };
}

function observeDataDatasetCreation({ datasetId, actor, request }) {
  const normalisedActor = normaliseActor(actor);

  requireValue(
    normalisedActor.customerId,
    "customerId is required for security observation.",
  );

  return buildDataDatasetSecurityObservation({
    datasetId,
    actor: normalisedActor,
    request,
    outcome: "allowed",
  });
}

function observeDeniedDataDatasetCreation({
  datasetId,
  actor,
  request,
  reason,
}) {
  assertSupportedDenialReason(reason);

  return buildDataDatasetSecurityObservation({
    datasetId,
    actor,
    request,
    outcome: "denied",
    reason,
  });
}

function enforceDataDatasetCreation({
  datasetId,
  actor,
  customerId,
  request,
  requiredRoles = DEFAULT_DATA_DATASET_REQUIRED_ROLES,
}) {
  requireValue(
    requiredRoles,
    "requiredRoles is required for security enforcement.",
  );

  const normalisedActor = normaliseActor(actor);

  if (!normalisedActor.customerId || !customerId) {
    const securityObservation = observeDeniedDataDatasetCreation({
      datasetId,
      actor: normalisedActor,
      request,
      reason: "missing_customer_context",
    });

    throw createSecurityDenialError(
      "Customer context is required for governed execution.",
      securityObservation,
    );
  }

  if (normalisedActor.customerId !== customerId) {
    const securityObservation = observeDeniedDataDatasetCreation({
      datasetId,
      actor: normalisedActor,
      request,
      reason: "customer_mismatch",
    });

    throw createSecurityDenialError(
      "Customer context does not match governed execution.",
      securityObservation,
    );
  }

  if (!requiredRoles.includes(normalisedActor.role)) {
    const securityObservation = observeDeniedDataDatasetCreation({
      datasetId,
      actor: normalisedActor,
      request,
      reason: "role_not_allowed",
    });

    throw createSecurityDenialError(
      "Role is not allowed for governed execution.",
      securityObservation,
    );
  }

  return observeDataDatasetCreation({
    datasetId,
    actor: normalisedActor,
    request,
  });
}

module.exports = {
  enforceDataDatasetCreation,
  enforceFoundationCommand,
  observeDataDatasetCreation,
  observeDeniedDataDatasetCreation,
  observeDeniedFoundationCommand,
  observeFoundationCommand,
};

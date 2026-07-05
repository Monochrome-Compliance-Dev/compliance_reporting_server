function createError(message, status) {
  const error = new Error(message);
  error.status = status;
  return error;
}

function getRequestActor(req) {
  return req?.auth || req?.user || req?.currentUser || null;
}

function normaliseExecutionContext(req) {
  const actor = getRequestActor(req);

  if (!actor) {
    throw createError("Authenticated execution context is required.", 401);
  }

  const actorId = actor.id || actor.userId || null;
  const customerId = actor.customerId || null;

  if (!actorId) {
    throw createError("actorId is required for governed execution.", 500);
  }

  if (!customerId) {
    throw createError("customerId is required for governed execution.", 500);
  }

  return {
    actorId,
    role: actor.role || null,
    customerId,
    source:
      actor === req?.auth
        ? "auth"
        : actor === req?.user
          ? "user"
          : "currentUser",
  };
}

module.exports = {
  normaliseExecutionContext,
};

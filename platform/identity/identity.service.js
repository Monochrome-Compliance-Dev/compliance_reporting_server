function createError(message, status) {
  const error = new Error(message);
  error.status = status;
  return error;
}

function getRequestActor(req) {
  return req?.auth || req?.user || req?.currentUser || null;
}

function getRequestCustomerId(req, actor) {
  return (
    req?.effectiveCustomerId ||
    req?.tenantCustomerId ||
    actor?.customerId ||
    req?.user?.customerId ||
    null
  );
}

function getRequestRole(req, actor) {
  return req?.actingRole || actor?.role || req?.user?.role || null;
}

function getExecutionContextSource(req, actor) {
  if (req?.effectiveCustomerId || req?.tenantCustomerId) {
    return "tenantContext";
  }

  if (actor === req?.auth) {
    return "auth";
  }

  if (actor === req?.user) {
    return "user";
  }

  return "currentUser";
}

function normaliseExecutionContext(req) {
  const actor = getRequestActor(req);

  if (!actor) {
    throw createError("Authenticated execution context is required.", 401);
  }

  const actorId = actor.id || actor.userId || null;
  const customerId = getRequestCustomerId(req, actor);

  if (!actorId) {
    throw createError("actorId is required for governed execution.", 500);
  }

  if (!customerId) {
    throw createError("customerId is required for governed execution.", 500);
  }

  return {
    actorId,
    role: getRequestRole(req, actor),
    customerId,
    source: getExecutionContextSource(req, actor),
  };
}

function attachExecutionContext(req, res, next) {
  try {
    req.executionContext = normaliseExecutionContext(req);
    return next();
  } catch (error) {
    return next(error);
  }
}

module.exports = {
  attachExecutionContext,
  normaliseExecutionContext,
};

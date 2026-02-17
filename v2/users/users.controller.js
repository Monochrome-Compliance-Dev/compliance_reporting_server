const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const usersService = require("@/v2/users/users.service");

module.exports = {
  authenticate,
  refreshToken,
  revokeToken,
  register,
  registerFirstUser,
  verifyToken,
  verifyEmail,
  forgotPassword,
  validateResetToken,
  resetPassword,

  // Admin
  listUsers,
  listUsersByCustomer,
  getUserById,
  createUser,
  updateUser,
  deleteUser,
  inviteWithResource,
};

function normaliseError(err) {
  // Default to an actual Error instance
  if (!err) {
    const e = new Error("Unknown error");
    e.status = 500;
    e.statusCode = 500;
    return e;
  }

  // Helper to force status fields onto an Error
  const setStatus = (e, status) => {
    const s = Number(status) || 500;
    e.status = s;
    e.statusCode = s;
    return e;
  };

  // Sequelize-style errors (common source of "mystery" 500s)
  // https://sequelize.org/ - Unique constraint should be a 409.
  const isSequelizeUnique =
    err?.name === "SequelizeUniqueConstraintError" ||
    err?.name === "UniqueConstraintError" ||
    err?.original?.code === "23505"; // Postgres unique_violation

  if (isSequelizeUnique) {
    const msg =
      err?.message ||
      err?.errors?.[0]?.message ||
      "Conflict: unique constraint violated";
    const e = err instanceof Error ? err : new Error(msg);
    return setStatus(e, 409);
  }

  // If it's already an Error, ensure status fields are consistent.
  if (err instanceof Error) {
    // Prefer explicit status if present
    if (!err.status && err.statusCode) err.status = err.statusCode;
    if (!err.statusCode && err.status) err.statusCode = err.status;

    // If no status provided, try to infer the *intent* from the message.
    // This is specifically to stop "already registered" from becoming 500.
    if (!err.status && !err.statusCode) {
      const msg = (err.message || "").toLowerCase();
      if (
        msg.includes("already registered") ||
        msg.includes("already taken") ||
        msg.includes("duplicate")
      ) {
        return setStatus(err, 409);
      }

      // fall back to 500 with consistent fields
      return setStatus(err, 500);
    }

    return err;
  }

  // Services sometimes throw plain objects like { status, message }
  // Express tends to mishandle those unless we convert to Error.
  const e = new Error(err.message || "Request failed");
  const status = err.status || err.statusCode;

  // Infer status from message if none supplied
  if (!status) {
    const msg = (err.message || "").toLowerCase();
    if (
      msg.includes("already registered") ||
      msg.includes("already taken") ||
      msg.includes("duplicate")
    ) {
      return setStatus(e, 409);
    }
    return setStatus(e, 500);
  }

  setStatus(e, status);

  // Keep any useful metadata if present
  if (err.code) e.code = err.code;
  if (err.errors) e.errors = err.errors;
  if (err.details) e.details = err.details;

  return e;
}

/**
 * POST /api/v2/users/authenticate
 * Body: { email, password }
 */
async function authenticate(req, res, next) {
  const userId = req.auth?.id || null;
  const ip = req.ip;
  const device = req.headers["user-agent"];

  try {
    const email = (req.body?.email || "").trim();
    const password = req.body?.password || "";

    if (!email || !password) {
      return res
        .status(400)
        .json({ status: "error", message: "email and password are required" });
    }

    const result = await usersService.authenticate({
      email,
      password,
      ipAddress: ip,
    });

    await auditService.logEvent({
      customerId: result.customerId || null,
      userId: result.id || userId,
      ip,
      device,
      action: "UsersV2Authenticate",
      entity: "User",
      entityId: result.id,
      details: { email },
    });

    if (result?.refreshToken) {
      setTokenCookie(res, result.refreshToken);
    }

    // Do not return refreshToken in the body (cookie is the source of truth)
    const { refreshToken, ...safe } = result || {};
    return res.status(200).json({ status: "success", data: safe });
  } catch (error) {
    const e = normaliseError(error);

    logger.logEvent("error", "Error authenticating user", {
      action: "UsersV2Authenticate",
      ip,
      userId,
      error: e.message,
      statusCode: e.status || e.statusCode || 500,
    });

    return next(e);
  }
}

/**
 * POST /api/v2/users/refresh-token
 * Cookie: refreshToken
 */
async function refreshToken(req, res, next) {
  const ip = req.ip;
  const device = req.headers["user-agent"];

  try {
    const token = req.cookies?.refreshToken;

    if (!token || token === "undefined") {
      return unauthorised(res);
    }

    const result = await usersService.refreshToken({ token, ipAddress: ip });

    if (result?.refreshToken) {
      setTokenCookie(res, result.refreshToken);
    }

    // Do not return refreshToken in the body
    const { refreshToken: _rt, ...safe } = result || {};

    await auditService.logEvent({
      customerId: safe.customerId || null,
      userId: safe.id || null,
      ip,
      device,
      action: "UsersV2RefreshToken",
      entity: "RefreshToken",
      entityId: null,
      details: { ok: true },
    });

    return res.status(200).json({ status: "success", data: safe });
  } catch (error) {
    const e = normaliseError(error);

    if (e.status === 400 || e.statusCode === 400) {
      clearRefreshCookie(res);
      return unauthorised(res);
    }

    logger.logEvent("error", "Error refreshing token", {
      action: "UsersV2RefreshToken",
      ip,
      error: e.message,
      statusCode: e.status || e.statusCode || 500,
    });

    return next(e);
  }
}

/**
 * POST /api/v2/users/revoke-token
 * Body: { refreshToken? }
 * Cookie: refreshToken
 */
async function revokeToken(req, res, next) {
  const ip = req.ip;
  const device = req.headers["user-agent"];

  try {
    const token =
      req.body?.refreshToken || req.body?.token || req.cookies?.refreshToken;

    // Always clear the cookie (idempotent logout)
    const clearCookie = () => clearRefreshCookie(res);

    if (!token) {
      clearCookie();
      await auditService.logEvent({
        customerId: req.effectiveCustomerId || null,
        userId: req.auth?.id || null,
        ip,
        device,
        action: "UsersV2RevokeToken",
        entity: "RefreshToken",
        entityId: null,
        details: { ok: true, note: "no token provided" },
      });
      return res.status(200).json({ status: "success", data: { ok: true } });
    }

    const result = await usersService.revokeToken({ token, ipAddress: ip });

    clearCookie();

    await auditService.logEvent({
      customerId: req.effectiveCustomerId || null,
      userId: req.auth?.id || null,
      ip,
      device,
      action: "UsersV2RevokeToken",
      entity: "RefreshToken",
      entityId: null,
      details: { ok: result.ok === true },
    });

    return res.status(200).json({ status: "success", data: { ok: true } });
  } catch (error) {
    const e = normaliseError(error);

    // If token is already invalid, treat as success (matches v1 to avoid noisy loops)
    if (e.status === 400 || e.statusCode === 400) {
      clearRefreshCookie(res);
      return res.status(200).json({ status: "success", data: { ok: true } });
    }

    logger.logEvent("error", "Error revoking token", {
      action: "UsersV2RevokeToken",
      ip,
      error: e.message,
      statusCode: e.status || e.statusCode || 500,
    });

    return next(e);
  }
}

/**
 * POST /api/v2/users/register
 * Body: { customerId, firstName, lastName, email, role, ... }
 */
async function register(req, res, next) {
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const origin = req.get("origin") || req.headers.origin || null;

  try {
    const params = { ...(req.body || {}) };

    if (!params.customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "customerId is required" });
    }

    const created = await usersService.register(params, origin);

    await auditService.logEvent({
      customerId: params.customerId,
      userId: req.auth?.id || null,
      ip,
      device,
      action: "UsersV2Register",
      entity: "User",
      entityId: created.id,
      details: { email: created.email },
    });

    return res.status(201).json({ status: "success", data: created });
  } catch (error) {
    const e = normaliseError(error);

    logger.logEvent("error", "Error registering user", {
      action: "UsersV2Register",
      ip,
      error: e.message,
      statusCode: e.status || e.statusCode || 500,
    });

    return next(e);
  }
}

/**
 * POST /api/v2/users/register-first-user
 * Body: { customerId, firstName, lastName, email, password }
 */
async function registerFirstUser(req, res, next) {
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const origin = req.get("origin") || req.headers.origin || null;

  try {
    const params = { ...(req.body || {}) };

    if (!params.customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "customerId is required" });
    }

    const created = await usersService.registerFirstUser(params, origin);

    await auditService.logEvent({
      customerId: params.customerId,
      userId: created.id,
      ip,
      device,
      action: "UsersV2RegisterFirstUser",
      entity: "User",
      entityId: created.id,
      details: { email: created.email },
    });

    return res.status(201).json({ status: "success", data: created });
  } catch (error) {
    const e = normaliseError(error);

    logger.logEvent("error", "Error registering first user", {
      action: "UsersV2RegisterFirstUser",
      ip,
      error: e.message,
      statusCode: e.status || e.statusCode || 500,
    });

    return next(e);
  }
}

/**
 * POST /api/v2/users/verify-token
 * Body: { token }
 */
async function verifyToken(req, res, next) {
  try {
    const token = req.body?.token || req.query?.token || null;

    if (!token) {
      return res
        .status(400)
        .json({ status: "error", message: "token is required" });
    }

    const result = await usersService.verifyToken(token);
    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    return next(normaliseError(error));
  }
}

/**
 * POST /api/v2/users/verify-email
 * Body: { token, password }
 */
async function verifyEmail(req, res, next) {
  try {
    const token = req.body?.token || req.query?.token || null;
    const password = req.body?.password || null;

    if (!token || !password) {
      return res.status(400).json({
        status: "error",
        message: "token and password are required",
      });
    }

    const result = await usersService.verifyEmail({ token, password });
    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    return next(normaliseError(error));
  }
}

/**
 * POST /api/v2/users/forgot-password
 * Body: { customerId, email }
 */
async function forgotPassword(req, res, next) {
  const origin = req.get("origin") || req.headers.origin || null;

  try {
    const customerId = req.body?.customerId || req.effectiveCustomerId || null;
    const email = (req.body?.email || "").trim();

    if (!customerId || !email) {
      return res.status(400).json({
        status: "error",
        message: "customerId and email are required",
      });
    }

    const result = await usersService.forgotPassword(
      { email, customerId },
      origin,
    );
    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    return next(normaliseError(error));
  }
}

/**
 * POST /api/v2/users/validate-reset-token
 * Body: { token }
 */
async function validateResetToken(req, res, next) {
  try {
    const token = req.body?.token || req.query?.token || null;

    if (!token) {
      return res
        .status(400)
        .json({ status: "error", message: "token is required" });
    }

    await usersService.validateResetToken({ token });
    return res.status(200).json({ status: "success", data: { ok: true } });
  } catch (error) {
    return next(normaliseError(error));
  }
}

/**
 * POST /api/v2/users/reset-password
 * Body: { customerId, token, password }
 */
async function resetPassword(req, res, next) {
  try {
    const customerId = req.body?.customerId || req.effectiveCustomerId || null;
    const token = req.body?.token || null;
    const password = req.body?.password || null;

    if (!customerId || !token || !password) {
      return res.status(400).json({
        status: "error",
        message: "customerId, token and password are required",
      });
    }

    const result = await usersService.resetPassword({
      customerId,
      token,
      password,
    });
    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    return next(normaliseError(error));
  }
}

// -----------------------------------------------------------------------------
// Admin (tenant scoped)
// -----------------------------------------------------------------------------

/**
 * GET /api/v2/users
 * Boss/global list (requires effectiveCustomerId if service requires it)
 * Query: customerId (optional)
 */
async function listUsers(req, res, next) {
  const customerId = req.query?.customerId || req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const items = await usersService.getAll(customerId);

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "UsersV2ListUsers",
      entity: "User",
      entityId: customerId,
      details: { count: Array.isArray(items) ? items.length : 0 },
    });

    return res.status(200).json({ status: "success", data: { items } });
  } catch (error) {
    return next(normaliseError(error));
  }
}

/**
 * GET /api/v2/users/by-customer
 */
async function listUsersByCustomer(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const items = await usersService.getAllByCustomerId(customerId);

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "UsersV2ListUsersByCustomer",
      entity: "User",
      entityId: customerId,
      details: { count: Array.isArray(items) ? items.length : 0 },
    });

    return res.status(200).json({ status: "success", data: { items } });
  } catch (error) {
    return next(normaliseError(error));
  }
}

/**
 * GET /api/v2/users/:id
 */
async function getUserById(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const id = req.params.id;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const item = await usersService.getById(id, customerId);

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "UsersV2GetUserById",
      entity: "User",
      entityId: id,
      details: { ok: !!item },
    });

    return res.status(200).json({ status: "success", data: item });
  } catch (error) {
    return next(normaliseError(error));
  }
}

/**
 * POST /api/v2/users
 */
async function createUser(req, res, next) {
  const customerId = req.effectiveCustomerId || req.body?.customerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const created = await usersService.create({
      ...(req.body || {}),
      customerId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "UsersV2CreateUser",
      entity: "User",
      entityId: created.id,
      details: { email: created.email },
    });

    return res.status(201).json({ status: "success", data: created });
  } catch (error) {
    return next(normaliseError(error));
  }
}

/**
 * PUT /api/v2/users/:id
 */
async function updateUser(req, res, next) {
  const customerId = req.effectiveCustomerId || req.body?.customerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const id = req.params.id;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const updated = await usersService.update(id, {
      ...(req.body || {}),
      customerId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "UsersV2UpdateUser",
      entity: "User",
      entityId: id,
      details: { ok: true },
    });

    return res.status(200).json({ status: "success", data: updated });
  } catch (error) {
    return next(normaliseError(error));
  }
}

/**
 * DELETE /api/v2/users/:id
 */
async function deleteUser(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const id = req.params.id;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const result = await usersService.delete(id, customerId);

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "UsersV2DeleteUser",
      entity: "User",
      entityId: id,
      details: { ok: result.ok === true },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    return next(normaliseError(error));
  }
}

/**
 * POST /api/v2/users/invite-with-resource
 * Body: { user: {...}, resource: {...}, createdBy?, origin? }
 */
async function inviteWithResource(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }

    const payload = req.body || {};

    const result = await usersService.inviteWithResource({
      user: { ...(payload.user || {}), customerId },
      resource: payload.resource || {},
      createdBy: payload.createdBy || userId,
      origin: payload.origin || req.get("origin") || req.headers.origin || null,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "UsersV2InviteWithResource",
      entity: "User",
      entityId: result.user?.id || null,
      details: { email: result.user?.email || null },
    });

    return res.status(201).json({ status: "success", data: result });
  } catch (error) {
    return next(normaliseError(error));
  }
}

// --- Cookie/auth helpers (copied from v1 for v2 cookie-based flows) ---
function unauthorised(res) {
  return res.status(401).json({ status: "error", message: "Unauthorised" });
}

function setTokenCookie(res, token) {
  const isProd = process.env.NODE_ENV === "production";
  const options = {
    httpOnly: true,
    secure: isProd,
    sameSite: "Lax",
    path: "/",
  };
  if (isProd && process.env.COOKIE_DOMAIN_PROD) {
    options.domain = process.env.COOKIE_DOMAIN_PROD;
  }
  res.cookie("refreshToken", token, options);
}

function clearRefreshCookie(res) {
  const isProd = process.env.NODE_ENV === "production";
  const options = {
    httpOnly: true,
    secure: isProd,
    sameSite: "Lax",
    path: "/",
  };
  if (isProd && process.env.COOKIE_DOMAIN_PROD) {
    options.domain = process.env.COOKIE_DOMAIN_PROD;
  }
  res.clearCookie("refreshToken", options);
}

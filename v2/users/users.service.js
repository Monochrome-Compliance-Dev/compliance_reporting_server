const os = require("os");
const crypto = require("crypto");
const jwt = require("jsonwebtoken");
const bcrypt = require("bcryptjs");
const { Op } = require("sequelize");

const db = require("@/db/database");
const Role = require("@/helpers/role");
const { logger } = require("@/helpers/logger");
const { sendEmail } = require("@/helpers/send-email");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

const jwtSecret = process.env.JWT_SECRET;
const REFRESH_RACE_GRACE_MS = Number(process.env.REFRESH_RACE_GRACE_MS || 5000);
const ACTIVE_ENT_STATES = ["active", "trial", "grace"];

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
  getAll,
  getAllByCustomerId,
  getById,
  create,
  update,
  delete: _delete,
  inviteWithResource,
};

// -------------------------------
// Auth flows
// -------------------------------

async function authenticate({ email, password, ipAddress }) {
  if (!email || !password) {
    throw { status: 400, message: "email and password are required" };
  }

  // 1) Resolve tenant for this email (no RLS context required for lookup)
  const t0 = await db.sequelize.transaction();
  let customerId;
  try {
    const cidRows = await db.sequelize.query(
      'SELECT fn_get_customer_id_by_email(:email) AS "customerId"',
      { transaction: t0, replacements: { email } },
    );

    customerId = Array.isArray(cidRows?.[0])
      ? cidRows[0][0]?.customerId
      : cidRows?.[0]?.customerId;

    await t0.commit();
  } catch (err) {
    if (t0 && !t0.finished) {
      try {
        await t0.rollback();
      } catch (_) {}
    }
    throw err;
  }

  if (!customerId) {
    logger?.logEvent?.("warn", "Tenant lookup failed for email", {
      action: "Authenticate",
      email,
    });
    throw { status: 401, message: "Email or password is incorrect" };
  }

  // 2) Tenant-scoped auth inside RLS context
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const user = await db.User.scope("withHash").findOne({
      where: { email },
      transaction: t,
    });

    if (
      !user ||
      !user.verified ||
      !(await bcrypt.compare(password, user.passwordHash))
    ) {
      logger?.logEvent?.("warn", "Failed login attempt", {
        action: "Authenticate",
        email,
        ip: ipAddress,
        device: os.hostname(),
      });
      const e = { status: 401, message: "Email or password is incorrect" };
      throw e;
    }

    const jwtToken = generateJwtToken(user);
    const refreshToken = generateRefreshToken(user, ipAddress);
    await refreshToken.save({ transaction: t });

    const entitlements = await listActiveEntitlements({
      customerId: user.customerId,
      transaction: t,
    });

    await t.commit();
    return {
      ...basicDetails(user),
      jwtToken,
      refreshToken: refreshToken.token,
      entitlements,
    };
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function refreshToken({ token, ipAddress }) {
  if (!token) throw { status: 400, message: "token is required" };

  const t = await db.sequelize.transaction();

  try {
    // Load the presented token even if inactive (reuse detection)
    const presented = await db.RefreshToken.findOne({
      where: { token },
      transaction: t,
    });

    if (!presented) {
      logger?.logEvent?.("warn", "Refresh token not found (refresh)", {
        action: "GetRefreshToken",
        partialToken: token ? token.slice(0, 6) + "..." : "<none>",
      });
      throw { status: 400, message: "Invalid token: Token not found" };
    }

    const isActive = presented.expires > Date.now() && !presented.revoked;

    if (!isActive) {
      if (presented.replacedByToken) {
        // Grace window: tolerate near-simultaneous refresh from same device
        const replacement = await db.RefreshToken.findOne({
          where: { token: presented.replacedByToken },
          transaction: t,
        });

        const sameUser = replacement && replacement.userId === presented.userId;
        const repCreatedTs =
          replacement && (replacement.created || replacement.createdAt);
        const repCreatedMs = repCreatedTs
          ? new Date(repCreatedTs).getTime()
          : 0;
        const withinGrace =
          repCreatedMs && Date.now() - repCreatedMs <= REFRESH_RACE_GRACE_MS;
        const sameIp = replacement && replacement.createdByIp === ipAddress;

        if (replacement && sameUser && withinGrace && sameIp) {
          logger?.logEvent?.(
            "info",
            "Refresh race tolerated; using replacement",
            {
              action: "RefreshRace",
              partialToken: token.slice(0, 6) + "...",
              replacedBy: presented.replacedByToken.slice(0, 6) + "...",
            },
          );

          const replacementActive =
            replacement.expires > Date.now() && !replacement.revoked;
          if (!replacementActive) {
            await revokeTokenFamily(presented.replacedByToken, ipAddress, t);
            logger?.logEvent?.(
              "warn",
              "Replacement inactive during race; revoked family",
              {
                action: "RefreshReuse",
                partialToken: token.slice(0, 6) + "...",
                replacedBy: presented.replacedByToken.slice(0, 6) + "...",
              },
            );
            throw {
              status: 400,
              message: "Invalid token: Token is not active",
            };
          }

          const user = await replacement.getUser({ transaction: t });
          const newRefreshToken = generateRefreshToken(user, ipAddress);

          replacement.revoked = Date.now();
          replacement.revokedByIp = ipAddress;
          replacement.replacedByToken = newRefreshToken.token;

          await replacement.save({ transaction: t });
          await newRefreshToken.save({ transaction: t });

          const jwtToken = generateJwtToken(user);

          // Ensure tenant context for entitlement queries
          await db.sequelize.query("SET LOCAL app.current_customer_id = :cid", {
            transaction: t,
            replacements: { cid: user.customerId },
          });

          const entitlements = await listActiveEntitlements({
            customerId: user.customerId,
            transaction: t,
          });

          await t.commit();
          return {
            ...basicDetails(user),
            jwtToken,
            refreshToken: newRefreshToken.token,
            entitlements,
          };
        }

        // Not a benign race => revoke the entire descendant chain
        await revokeTokenFamily(presented.replacedByToken, ipAddress, t);
        logger?.logEvent?.(
          "warn",
          "Refresh token reuse detected; revoked family",
          {
            action: "RefreshReuse",
            partialToken: token.slice(0, 6) + "...",
            replacedBy: presented.replacedByToken.slice(0, 6) + "...",
          },
        );
      } else {
        logger?.logEvent?.(
          "warn",
          "Inactive refresh token presented (no replacement)",
          {
            action: "RefreshReuse",
            partialToken: token.slice(0, 6) + "...",
          },
        );
      }

      throw { status: 400, message: "Invalid token: Token is not active" };
    }

    // Token active => rotate
    const user = await presented.getUser({ transaction: t });
    const newRefreshToken = generateRefreshToken(user, ipAddress);

    presented.revoked = Date.now();
    presented.revokedByIp = ipAddress;
    presented.replacedByToken = newRefreshToken.token;

    await presented.save({ transaction: t });
    await newRefreshToken.save({ transaction: t });

    const jwtToken = generateJwtToken(user);

    await db.sequelize.query("SET LOCAL app.current_customer_id = :cid", {
      transaction: t,
      replacements: { cid: user.customerId },
    });

    const entitlements = await listActiveEntitlements({
      customerId: user.customerId,
      transaction: t,
    });

    await t.commit();

    return {
      ...basicDetails(user),
      jwtToken,
      refreshToken: newRefreshToken.token,
      entitlements,
    };
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function revokeToken({ token, ipAddress }) {
  if (!token) throw { status: 400, message: "token is required" };

  const t = await db.sequelize.transaction();
  try {
    const refreshToken = await getRefreshToken(token, { transaction: t });

    refreshToken.revoked = Date.now();
    refreshToken.revokedByIp = ipAddress;
    await refreshToken.save({ transaction: t });

    await t.commit();
    return { ok: true };
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

// -------------------------------
// Registration / verify / reset
// -------------------------------

async function register(params, origin) {
  if (!params?.customerId)
    throw { status: 400, message: "customerId is required" };

  const t = await beginTransactionWithCustomerContext(params.customerId);
  try {
    const existing = await db.User.findOne({
      where: { email: params.email },
      transaction: t,
    });

    if (existing) {
      await sendAlreadyRegisteredEmail(params.email, origin);
      throw {
        status: 409,
        message: `Email "${params.email}" is already registered`,
      };
    }

    const user = new db.User(params);
    user.verificationToken = randomTokenString();

    await user.save({ transaction: t });

    await sendVerificationEmail(user, origin);
    await t.commit();

    return basicDetails(user);
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function registerFirstUser(params, origin) {
  if (!params?.customerId)
    throw { status: 400, message: "customerId is required" };

  const t = await beginTransactionWithCustomerContext(params.customerId);
  try {
    const existing = await db.User.findOne({
      where: { email: params.email },
      transaction: t,
    });

    if (existing) {
      await sendAlreadyRegisteredEmail(params.email, origin);
      logger?.logEvent?.("warn", "Duplicate first user registration attempt", {
        action: "RegisterFirstUser",
        email: params.email,
        device: os.hostname(),
      });
      throw {
        status: 409,
        message: `Email "${params.email}" is already registered`,
      };
    }

    const user = new db.User(params);
    user.role = Role.Admin;
    user.verified = Date.now();
    user.passwordHash = await hash(params.password);

    await user.save({ transaction: t });

    await sendFirstUserWelcomeEmail(user, origin);
    await t.commit();

    return basicDetails(user);
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function verifyToken(token) {
  if (!token) throw { status: 400, message: "token is required" };

  const t = await db.sequelize.transaction();
  try {
    const user = await db.User.findOne({
      where: { verificationToken: token },
      transaction: t,
    });
    if (!user) throw { status: 401, message: "Verification failed" };

    await t.commit();
    return basicDetails(user);
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function verifyEmail(params) {
  const { token, password } = params || {};
  if (!token || !password) {
    throw { status: 400, message: "token and password are required" };
  }

  const t = await db.sequelize.transaction();
  try {
    const user = await db.User.findOne({
      where: { verificationToken: token },
      transaction: t,
    });

    if (!user) throw { status: 401, message: "Verification failed" };

    user.active = true;
    user.verified = Date.now();
    user.verificationToken = null;
    user.passwordHash = await hash(password);

    await user.save({ transaction: t });
    await t.commit();

    return basicDetails(user);
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function forgotPassword({ email, customerId }, origin) {
  if (!customerId) throw { status: 400, message: "customerId is required" };
  if (!email) throw { status: 400, message: "email is required" };

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const user = await db.User.findOne({ where: { email }, transaction: t });

    // Intentionally do not reveal whether a user exists.
    if (!user) {
      await t.commit();
      return { ok: true };
    }

    user.resetToken = randomTokenString();
    user.resetTokenExpires = new Date(Date.now() + 24 * 60 * 60 * 1000);
    await user.save({ transaction: t });

    await sendPasswordResetEmail(user, origin);
    await t.commit();

    return { ok: true };
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function validateResetToken({ token }, options = {}) {
  const user = await db.User.findOne({
    where: {
      resetToken: token,
      resetTokenExpires: { [Op.gt]: Date.now() },
    },
    ...options,
  });

  if (!user) throw { status: 401, message: "Invalid token" };
  return user;
}

async function resetPassword({ token, password, customerId }) {
  if (!customerId) throw { status: 400, message: "customerId is required" };
  if (!token || !password) {
    throw { status: 400, message: "token and password are required" };
  }

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const user = await validateResetToken({ token }, { transaction: t });

    user.passwordHash = await hash(password);
    user.passwordReset = Date.now();
    user.resetToken = null;

    await user.save({ transaction: t });
    await t.commit();

    return { ok: true };
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

// -------------------------------
// Admin (tenant scoped)
// -------------------------------

async function getAll(customerId) {
  if (!customerId) throw { status: 400, message: "customerId is required" };

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const users = await db.User.findAll({ transaction: t });
    await t.commit();
    return users.map((x) => basicDetails(x));
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function getAllByCustomerId(customerId) {
  if (!customerId) throw { status: 400, message: "customerId is required" };

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const users = await db.User.findAll({
      where: { customerId },
      transaction: t,
    });

    await t.commit();
    return users.map((x) => basicDetails(x));
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function getById(id, customerId) {
  if (!customerId) throw { status: 400, message: "customerId is required" };

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const user = await getUser(id, { transaction: t });
    await t.commit();
    return basicDetails(user);
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function create(params) {
  if (!params?.customerId)
    throw { status: 400, message: "customerId is required" };

  const t = await beginTransactionWithCustomerContext(params.customerId);
  try {
    // Seat cap check
    const customer = await db.Customer.findOne({
      where: { id: params.customerId },
      attributes: ["seats"],
      transaction: t,
    });

    const seats = customer?.seats ?? 1;
    const willBeActive = params.active !== false;

    const activeCount = await db.User.count({
      where: { customerId: params.customerId, active: true },
      transaction: t,
    });

    if (willBeActive && activeCount >= seats) {
      throw {
        status: 403,
        message: `Seat limit reached (${seats}). Please upgrade your subscription to add more users.`,
      };
    }

    const existing = await db.User.findOne({
      where: { email: params.email },
      transaction: t,
    });

    if (existing) {
      throw {
        status: 409,
        message: `Email "${params.email}" is already registered`,
      };
    }

    const user = new db.User(params);
    user.verified = Date.now();
    user.passwordHash = await hash(params.password);

    await user.save({ transaction: t });
    await t.commit();

    return basicDetails(user);
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function update(id, params) {
  if (!params?.customerId)
    throw { status: 400, message: "customerId is required" };

  const t = await beginTransactionWithCustomerContext(params.customerId);
  try {
    const user = await getUser(id, { transaction: t });

    if (
      params.email &&
      user.email !== params.email &&
      (await db.User.findOne({
        where: { email: params.email },
        transaction: t,
      }))
    ) {
      throw {
        status: 500,
        message: `Email "${params.email}" is already taken`,
      };
    }

    if (params.password) {
      params.passwordHash = await hash(params.password);
    }

    Object.assign(user, params);
    user.updated = Date.now();

    await user.save({ transaction: t });
    await t.commit();

    return basicDetails(user);
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function _delete(id, customerId) {
  if (!customerId) throw { status: 400, message: "customerId is required" };

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const user = await getUser(id, { transaction: t });

    await user.destroy({ transaction: t });

    logger?.logEvent?.("warn", "User account deleted", {
      action: "DeleteUser",
      userId: user.id,
      email: user.email,
    });

    await t.commit();
    return { ok: true };
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function inviteWithResource({ user, resource, createdBy, origin }) {
  if (!user?.customerId)
    throw { status: 400, message: "customerId is required" };

  const t = await beginTransactionWithCustomerContext(user.customerId);

  try {
    const existing = await db.User.findOne({
      where: { email: user.email },
      transaction: t,
    });

    if (existing) {
      await sendAlreadyRegisteredEmail(user.email, origin);
      throw {
        status: 400,
        message: `Email "${user.email}" is already registered`,
      };
    }

    const invited = new db.User({
      firstName: user.firstName,
      lastName: user.lastName,
      active: user.active,
      email: user.email,
      role: user.role || Role.User,
      customerId: user.customerId,
    });

    invited.verificationToken = randomTokenString();
    await invited.save({ transaction: t });

    const savedResource = await db.Resource.create(
      {
        name: resource.name,
        position: resource.position,
        hourlyRate: resource.hourlyRate ?? null,
        capacityHoursPerWeek: resource.capacityHoursPerWeek ?? null,
        userId: invited.id,
        customerId: user.customerId,
        createdBy: createdBy || null,
      },
      { transaction: t },
    );

    await t.commit();

    await sendVerificationEmail(invited, origin);

    return {
      user: basicDetails(invited),
      resource: {
        id: savedResource.id,
        name: savedResource.name,
        role: savedResource.role,
        hourlyRate: savedResource.hourlyRate,
        capacityHoursPerWeek: savedResource.capacityHoursPerWeek,
        userId: savedResource.userId,
        customerId: savedResource.customerId,
      },
    };
  } catch (err) {
    if (t && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

// -------------------------------
// Helpers
// -------------------------------

async function listActiveEntitlements({ customerId, transaction }) {
  const entRows = await db.FeatureEntitlement.findAll({
    where: {
      customerId,
      status: { [Op.in]: ACTIVE_ENT_STATES },
      [Op.or]: [{ validTo: null }, { validTo: { [Op.gte]: new Date() } }],
    },
    attributes: ["feature"],
    transaction,
  });

  return (entRows || []).map((r) => r.feature);
}

async function getUser(id, options = {}) {
  const user = await db.User.findByPk(id, options);
  if (!user) throw { status: 404, message: "User not found" };
  return user;
}

async function getRefreshToken(token, options = {}) {
  logger?.logEvent?.("debug", "Querying for refresh token", {
    action: "GetRefreshToken",
    partialToken: token.slice(0, 6) + "...",
  });

  const refreshToken = await db.RefreshToken.findOne({
    where: { token },
    ...options,
  });

  if (!refreshToken) {
    logger?.logEvent?.("warn", "Refresh token not found", {
      action: "GetRefreshToken",
      partialToken: token.slice(0, 6) + "...",
    });
    throw { status: 400, message: "Invalid token: Token not found" };
  }

  const isActive = refreshToken.expires > Date.now() && !refreshToken.revoked;
  if (!isActive) {
    logger?.logEvent?.("warn", "Refresh token is not active", {
      action: "GetRefreshToken",
      partialToken: token.slice(0, 6) + "...",
      expires: refreshToken.expires,
      revoked: refreshToken.revoked,
    });
    throw { status: 400, message: "Invalid token: Token is not active" };
  }

  logger?.logEvent?.("debug", "Refresh token is valid", {
    action: "GetRefreshToken",
    partialToken: token.slice(0, 6) + "...",
  });

  return refreshToken;
}

async function hash(password) {
  return bcrypt.hash(password, 10);
}

function generateJwtToken(user) {
  return jwt.sign(
    { id: user.id, role: user.role, customerId: user.customerId },
    jwtSecret,
    { expiresIn: "15m" },
  );
}

function generateRefreshToken(user, ipAddress) {
  return new db.RefreshToken({
    userId: user.id,
    token: randomTokenString(),
    expires: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
    createdByIp: ipAddress,
  });
}

function randomTokenString() {
  return crypto.randomBytes(40).toString("hex");
}

function basicDetails(user) {
  const {
    id,
    firstName,
    lastName,
    email,
    role,
    phone,
    created,
    updated,
    verified,
    customerId,
  } = user;

  return {
    id,
    firstName,
    lastName,
    email,
    phone,
    role,
    created,
    updated,
    isVerified: !!verified,
    customerId,
  };
}

async function sendVerificationEmail(user, origin) {
  let message;
  if (origin) {
    const verifyUrl = `${origin}/user/verify-email?token=${user.verificationToken}`;
    message = `<p>Please click the below link to verify your email address and create your password:</p>
                   <p><a href="${verifyUrl}">${verifyUrl}</a></p>`;
  } else {
    message = `<p>Please use the below token to verify your email address and create your password with the <code>/user/verify-email</code> api route:</p>
                   <p><code>${user.verificationToken}</code></p>`;
  }

  await sendEmail({
    to: user.email,
    subject: "Sign-up Verification API - Verify Email",
    html: `<h4>Verify Email</h4>
               <p>Thanks for registering!</p>
               ${message}`,
  });
}

async function sendFirstUserWelcomeEmail(user, origin) {
  const message = origin
    ? `<p>Welcome to the system! Please visit the <a href="${origin}/user/login">login page</a> to access your account.</p>`
    : `<p>Welcome to the system! You can log in using the <code>/user/login</code> api route.</p>`;

  await sendEmail({
    to: user.email,
    subject: "Sign-up Verification API - Welcome",
    html: `<h4>Welcome to Monochrome Compliance!</h4>
               ${message}`,
  });
}

async function sendAlreadyRegisteredEmail(email, origin) {
  const message = origin
    ? `<p>If you don't know your password please visit the <a href="${origin}/user/forgot-password">forgot password</a> page.</p>`
    : `<p>If you don't know your password you can reset it via the <code>/user/forgot-password</code> api route.</p>`;

  await sendEmail({
    to: email,
    subject: "Sign-up Verification API - Email Already Registered",
    html: `<h4>Email Already Registered</h4>
               <p>Your email <strong>${email}</strong> is already registered.</p>
               ${message}`,
  });
}

async function sendPasswordResetEmail(user, origin) {
  let message;
  if (origin) {
    const resetUrl = `${origin}/user/reset-password?token=${user.resetToken}`;
    message = `<p>Please click the below link to reset your password, the link will be valid for 1 day:</p>
                   <p><a href="${resetUrl}">${resetUrl}</a></p>`;
  } else {
    message = `<p>Please use the below token to reset your password with the <code>/user/reset-password</code> api route:</p>
                   <p><code>${user.resetToken}</code></p>`;
  }

  await sendEmail({
    to: user.email,
    subject: "Sign-up Verification API - Reset Password",
    html: `<h4>Reset Password Email</h4>
               ${message}`,
  });
}

// Revoke a token and all its descendants by following replacedByToken
async function revokeTokenFamily(token, ipAddress, transaction) {
  if (!token) return;

  let current = await db.RefreshToken.findOne({
    where: { token },
    transaction,
  });

  while (current) {
    if (!current.revoked) {
      current.revoked = Date.now();
      current.revokedByIp = ipAddress;
      await current.save({ transaction });
    }

    if (!current.replacedByToken) break;

    current = await db.RefreshToken.findOne({
      where: { token: current.replacedByToken },
      transaction,
    });
  }
}

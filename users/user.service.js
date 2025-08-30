const os = require("os");
const { logger } = require("../helpers/logger");
const jwtSecret = process.env.JWT_SECRET;
const jwt = require("jsonwebtoken");
const bcrypt = require("bcryptjs");
const crypto = require("crypto");
const { Op } = require("sequelize");
const { sendEmail } = require("../helpers/send-email");
const db = require("../db/database");
const Role = require("../helpers/role");
const {
  beginTransactionWithCustomerContext,
} = require("../helpers/setCustomerIdRLS");

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

async function authenticate({ email, password, ipAddress, customerId }) {
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
      logger.logEvent("warn", "Failed login attempt", {
        action: "Authenticate",
        email,
        ip: ipAddress,
        device: os.hostname(),
      });
      throw { status: 401, message: "Email or password is incorrect" };
    }

    // authentication successful so generate jwt and refresh tokens
    const jwtToken = generateJwtToken(user);
    const refreshToken = generateRefreshToken(user, ipAddress);

    // save refresh token
    await refreshToken.save({ transaction: t });

    // return basic details and tokens
    await t.commit();
    return {
      ...basicDetails(user),
      jwtToken,
      refreshToken: refreshToken.token,
    };
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function refreshToken({ token, ipAddress }) {
  const t = await db.sequelize.transaction();
  try {
    const refreshToken = await getRefreshToken(token, { transaction: t });
    const user = await refreshToken.getUser({ transaction: t });
    const newRefreshToken = generateRefreshToken(user, ipAddress);

    refreshToken.revoked = Date.now();
    refreshToken.revokedByIp = ipAddress;
    refreshToken.replacedByToken = newRefreshToken.token;
    await refreshToken.save({ transaction: t });
    await newRefreshToken.save({ transaction: t });

    // generate new jwt
    const jwtToken = generateJwtToken(user);

    // return basic details and tokens
    await t.commit();
    return {
      ...basicDetails(user),
      jwtToken,
      refreshToken: newRefreshToken.token,
    };
  } catch (err) {
    if (t && !t.finished) await t.rollback();
    throw err;
  } finally {
    if (t && !t.finished) await t.rollback();
  }
}

async function revokeToken({ token, ipAddress }) {
  const t = await db.sequelize.transaction();
  try {
    const refreshToken = await getRefreshToken(token, { transaction: t });

    // revoke token and save
    refreshToken.revoked = Date.now();
    refreshToken.revokedByIp = ipAddress;
    await refreshToken.save({ transaction: t });
    await t.commit();
  } catch (err) {
    if (t && !t.finished) await t.rollback();
    throw err;
  } finally {
    if (t && !t.finished) await t.rollback();
  }
}

async function register(params, origin) {
  if (!params.customerId)
    throw { status: 400, message: "customerId is required" };
  const t = await db.sequelize.transaction();
  try {
    if (
      await db.User.findOne({ where: { email: params.email }, transaction: t })
    ) {
      await sendAlreadyRegisteredEmail(params.email, origin);
      throw {
        status: 400,
        message: `Email "${params.email}" is already registered`,
      };
    }

    const user = new db.User(params);
    user.verificationToken = randomTokenString();

    await user.save({ transaction: t });

    await sendVerificationEmail(user, origin);
    await t.commit();
  } catch (err) {
    if (t && !t.finished) await t.rollback();
    throw err;
  } finally {
    if (t && !t.finished) await t.rollback();
  }
}

async function registerFirstUser(params, origin) {
  if (!params.customerId)
    throw { status: 400, message: "customerId is required" };
  const t = await beginTransactionWithCustomerContext(params.customerId);
  try {
    if (
      await db.User.findOne({ where: { email: params.email }, transaction: t })
    ) {
      await sendAlreadyRegisteredEmail(params.email, origin);
      logger.logEvent("warn", "Duplicate first user registration attempt", {
        action: "RegisterFirstUser",
        email: params.email,
        device: os.hostname(),
      });
      throw {
        status: 400,
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
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function verifyToken(token, customerId) {
  if (!customerId) throw { status: 400, message: "customerId is required" };
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const user = await db.User.findOne({
      where: { verificationToken: token },
      transaction: t,
    });
    if (!user) throw { status: 401, message: "Verification failed" };
    if (user.verified) {
      throw { status: 400, message: "Email already verified" };
    }

    await t.commit();
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function verifyEmail(params) {
  if (!params.customerId)
    throw { status: 400, message: "customerId is required" };
  const t = await beginTransactionWithCustomerContext(params.customerId);
  try {
    const { token, password } = params;
    const user = await db.User.findOne({
      where: { verificationToken: token },
      transaction: t,
    });

    if (!user) throw { status: 401, message: "Verification failed" };

    user.verified = Date.now();
    user.verificationToken = null;

    user.passwordHash = await hash(password);

    await user.save({ transaction: t });
    await t.commit();
    return basicDetails(user);
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function forgotPassword({ email, customerId }, origin) {
  if (!customerId) throw { status: 400, message: "customerId is required" };
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const user = await db.User.findOne({ where: { email }, transaction: t });

    if (!user) {
      await t.rollback();
      return;
    }

    user.resetToken = randomTokenString();
    user.resetTokenExpires = new Date(Date.now() + 24 * 60 * 60 * 1000);
    await user.save({ transaction: t });

    await sendPasswordResetEmail(user, origin);
    await t.commit();
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  } finally {
    if (!t.finished) await t.rollback();
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
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const user = await validateResetToken({ token }, { transaction: t });

    user.passwordHash = await hash(password);
    user.passwordReset = Date.now();
    user.resetToken = null;
    await user.save({ transaction: t });
    await t.commit();
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getAll(customerId) {
  if (!customerId) throw { status: 400, message: "customerId is required" };
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const users = await db.User.findAll({ transaction: t });
    await t.rollback();
    return users.map((x) => basicDetails(x));
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  } finally {
    if (!t.finished) await t.rollback();
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
    await t.rollback();
    return users.map((x) => basicDetails(x));
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getById(id, customerId) {
  if (!customerId) throw { status: 400, message: "customerId is required" };
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const user = await getUser(id, { transaction: t });
    await t.rollback();
    return basicDetails(user);
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function create(params) {
  if (!params.customerId)
    throw { status: 400, message: "customerId is required" };
  const t = await beginTransactionWithCustomerContext(params.customerId);
  try {
    if (
      await db.User.findOne({ where: { email: params.email }, transaction: t })
    ) {
      throw {
        status: 500,
        message: 'Email "' + params.email + '" is already registered',
      };
    }

    const user = new db.User(params);
    user.verified = Date.now();

    user.passwordHash = await hash(params.password);

    await user.save({ transaction: t });

    await t.commit();
    return basicDetails(user);
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function update(id, params) {
  if (!params.customerId)
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
        message: 'Email "' + params.email + '" is already taken',
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
    if (!t.finished) await t.rollback();
    throw err;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function _delete(id, customerId) {
  if (!customerId) throw { status: 400, message: "customerId is required" };
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const user = await getUser(id, { transaction: t });
    await user.destroy({ transaction: t });
    logger.logEvent("warn", "User account deleted", {
      action: "DeleteUser",
      userId: user.id,
      email: user.email,
    });
    await t.commit();
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function inviteWithResource({ user, resource, createdBy, origin }) {
  if (!user?.customerId)
    throw { status: 400, message: "customerId is required" };
  const t = await beginTransactionWithCustomerContext(user.customerId);
  try {
    // Block duplicate email
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

    // Create invited user (unverified, email verification required)
    const invited = new db.User({
      firstName: user.firstName || "",
      lastName: user.lastName || "",
      email: user.email,
      role: user.role || Role.User,
      customerId: user.customerId,
    });
    invited.verificationToken = randomTokenString();
    await invited.save({ transaction: t });

    await sendVerificationEmail(invited, origin);

    // Create linked resource with the invited user's id
    const savedResource = await db.Resource.create(
      {
        name: resource.name,
        role: resource.role || null,
        hourlyRate: resource.hourlyRate ?? null,
        capacityHoursPerWeek: resource.capacityHoursPerWeek ?? null,
        userId: invited.id,
        customerId: user.customerId,
        createdBy: createdBy || null,
      },
      { transaction: t }
    );

    await t.commit();
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
    if (t && !t.finished) await t.rollback();
    throw err;
  } finally {
    if (t && !t.finished) await t.rollback();
  }
}

// helper functions

async function getUser(id, options = {}) {
  const user = await db.User.findByPk(id, options);
  if (!user) throw { status: 404, message: "User not found" };
  return user;
}

async function getUserByToken(token, options = {}) {
  const refreshToken = await db.RefreshToken.findOne({
    where: { token },
    ...options,
  });
  if (!refreshToken) throw { status: 404, message: "Refresh token not found" };
  return await getUser(refreshToken.userId, options);
}

async function getRefreshToken(token, options = {}) {
  logger.logEvent("debug", "Querying for refresh token", {
    action: "GetRefreshToken",
    partialToken: token.slice(0, 6) + "...",
  });

  const refreshToken = await db.RefreshToken.findOne({
    where: { token },
    ...options,
  });

  if (!refreshToken) {
    logger.logEvent("warn", "Refresh token not found", {
      action: "GetRefreshToken",
      partialToken: token.slice(0, 6) + "...",
    });
    throw { status: 400, message: "Invalid token: Token not found" };
  }

  const isActive = refreshToken.expires > Date.now() && !refreshToken.revoked;
  if (!isActive) {
    logger.logEvent("warn", "Refresh token is not active", {
      action: "GetRefreshToken",
      partialToken: token.slice(0, 6) + "...",
      expires: refreshToken.expires,
      revoked: refreshToken.revoked,
    });
    throw { status: 400, message: "Invalid token: Token is not active" };
  }

  logger.logEvent("debug", "Refresh token is valid", {
    action: "GetRefreshToken",
    partialToken: token.slice(0, 6) + "...",
  });
  return refreshToken;
}

async function hash(password) {
  return await bcrypt.hash(password, 10);
}

function generateJwtToken(user) {
  // create a jwt token containing the user id that expires in 15 minutes
  return jwt.sign(
    { id: user.id, role: user.role, customerId: user.customerId },
    jwtSecret,
    {
      expiresIn: "15m",
    }
  );
}

function generateRefreshToken(user, ipAddress) {
  // create a refresh token that expires in 7 days
  return new db.RefreshToken({
    userId: user.id,
    token: randomTokenString(),
    expires: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000), // Set expiry to 7 days
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
    position,
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
    position,
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
  let message;
  if (origin) {
    message = `<p>Welcome to the system! Please visit the <a href="${origin}/user/login">login page</a> to access your account.</p>`;
  } else {
    message = `<p>Welcome to the system! You can log in using the <code>/user/login</code> api route.</p>`;
  }

  await sendEmail({
    to: user.email,
    subject: "Sign-up Verification API - Welcome",
    html: `<h4>Welcome to Monochrome Compliance!</h4>
               ${message}`,
  });
}

async function sendAlreadyRegisteredEmail(email, origin) {
  let message;
  if (origin) {
    message = `<p>If you don't know your password please visit the <a href="${origin}/user/forgot-password">forgot password</a> page.</p>`;
  } else {
    message = `<p>If you don't know your password you can reset it via the <code>/user/forgot-password</code> api route.</p>`;
  }

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

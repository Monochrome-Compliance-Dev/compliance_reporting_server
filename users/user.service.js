const os = require("os");
const { logger } = require("../helpers/logger");
const config = require("../helpers/config");
const jwt = require("jsonwebtoken");
const bcrypt = require("bcryptjs");
const crypto = require("crypto");
const { Op } = require("sequelize");
const { sendEmail } = require("../helpers/send-email");
const db = require("../helpers/db");
const Role = require("../helpers/role");

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
  getAllByClientId,
  getById,
  create,
  update,
  delete: _delete,
};

async function authenticate({ email, password, ipAddress }) {
  console.log("Authenticating user:", email, password, ipAddress);
  const user = await db.User.scope("withHash").findOne({
    where: { email },
  });
  console.log("User found:", user ? user.email : "No user found");
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
  await refreshToken.save();

  // return basic details and tokens
  logger.logEvent("info", "User authenticated", {
    action: "Authenticate",
    userId: user.id,
    email: user.email,
    ip: ipAddress,
    device: os.hostname(),
  });
  return {
    ...basicDetails(user),
    jwtToken,
    refreshToken: refreshToken.token,
  };
}

async function refreshToken({ token, ipAddress }) {
  const refreshToken = await getRefreshToken(token);
  const user = await refreshToken.getUser();
  const newRefreshToken = generateRefreshToken(user, ipAddress);

  refreshToken.revoked = Date.now();
  refreshToken.revokedByIp = ipAddress;
  refreshToken.replacedByToken = newRefreshToken.token;
  await refreshToken.save();
  await newRefreshToken.save();

  // generate new jwt
  const jwtToken = generateJwtToken(user);

  // return basic details and tokens
  logger.logEvent("info", "Refresh token rotated", {
    action: "RotateToken",
    userId: user.id,
    email: user.email,
    ip: ipAddress,
    device: os.hostname(),
  });
  return {
    ...basicDetails(user),
    jwtToken,
    refreshToken: newRefreshToken.token,
  };
}

async function revokeToken({ token, ipAddress }) {
  const refreshToken = await getRefreshToken(token);

  // revoke token and save
  refreshToken.revoked = Date.now();
  refreshToken.revokedByIp = ipAddress;
  await refreshToken.save();
  logger.logEvent("info", "Refresh token revoked", {
    action: "RevokeToken",
    userId: refreshToken.userId,
    ip: ipAddress,
    device: os.hostname(),
  });
}

async function register(params, origin) {
  // validate
  if (await db.User.findOne({ where: { email: params.email } })) {
    // send already registered error in email to prevent user enumeration
    await sendAlreadyRegisteredEmail(params.email, origin);
    logger.logEvent("warn", "Duplicate registration attempt", {
      action: "Register",
      email: params.email,
      device: os.hostname(),
    });
    throw {
      status: 400,
      message: `Email "${params.email}" is already registered`,
    };
  }

  // create user object
  const user = new db.User(params);
  user.verificationToken = randomTokenString();

  // save user
  await user.save();

  // send email
  await sendVerificationEmail(user, origin);
  logger.logEvent("info", "New user registered", {
    action: "Register",
    userId: user.id,
    email: user.email,
    device: os.hostname(),
  });
}

async function registerFirstUser(params, origin) {
  // validate
  if (await db.User.findOne({ where: { email: params.email } })) {
    // send already registered error in email to prevent user enumeration
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

  // create user object
  const user = new db.User(params);
  user.role = Role.Admin;
  user.verified = Date.now();

  // hash password
  user.passwordHash = await hash(params.password);

  // save user
  await user.save();

  // send email
  await sendFirstUserWelcomeEmail(user, origin);
  logger.logEvent("info", "First user registered", {
    action: "RegisterFirstUser",
    userId: user.id,
    email: user.email,
    device: os.hostname(),
  });
}

async function verifyToken(token) {
  const user = await db.User.findOne({
    where: { verificationToken: token },
  });
  if (!user) throw { status: 401, message: "Verification failed" };
  if (user.verified) {
    throw { status: 400, message: "Email already verified" };
  }

  logger.logEvent("info", "Verification token validated", {
    action: "VerifyToken",
    userId: user.id,
    email: user.email,
    device: os.hostname(),
  });
  return;
}

async function verifyEmail(params) {
  const { token, password } = params;
  const user = await db.User.findOne({
    where: { verificationToken: token },
  });

  if (!user) throw { status: 401, message: "Verification failed" };

  user.verified = Date.now();
  user.verificationToken = null;

  // hash password
  user.passwordHash = await hash(password);

  await user.save();
  logger.logEvent("info", "Email verified", {
    action: "VerifyEmail",
    userId: user.id,
    email: user.email,
  });
  return basicDetails(user);
}

async function forgotPassword({ email }, origin) {
  const user = await db.User.findOne({ where: { email } });

  // always return ok response to prevent email enumeration
  if (!user) return;

  // create reset token that expires after 24 hours
  user.resetToken = randomTokenString();
  user.resetTokenExpires = new Date(Date.now() + 24 * 60 * 60 * 1000);
  await user.save();

  // send email
  await sendPasswordResetEmail(user, origin);
  logger.logEvent("info", "Password reset token generated", {
    action: "ForgotPassword",
    userId: user.id,
    email: user.email,
  });
}

async function validateResetToken({ token }) {
  const user = await db.User.findOne({
    where: {
      resetToken: token,
      resetTokenExpires: { [Op.gt]: Date.now() },
    },
  });

  if (!user) throw { status: 401, message: "Invalid token" };

  return user;
}

async function resetPassword({ token, password }) {
  const user = await validateResetToken({ token });

  // update password and remove reset token
  user.passwordHash = await hash(password);
  user.passwordReset = Date.now();
  user.resetToken = null;
  await user.save();
  logger.logEvent("info", "Password reset successful", {
    action: "ResetPassword",
    userId: user.id,
    email: user.email,
  });
}

async function getAll() {
  const users = await db.User.findAll();
  return users.map((x) => basicDetails(x));
}

async function getAllByClientId(clientId) {
  const users = await db.User.findAll({
    where: { clientId },
  });
  return users.map((x) => basicDetails(x));
}

async function getById(id) {
  const user = await getUser(id);
  return basicDetails(user);
}

async function create(params) {
  // validate
  if (await db.User.findOne({ where: { email: params.email } })) {
    throw {
      status: 500,
      message: 'Email "' + params.email + '" is already registered',
    };
  }

  const user = new db.User(params);
  user.verified = Date.now();

  // hash password
  user.passwordHash = await hash(params.password);

  // save user
  await user.save();

  return basicDetails(user);
}

async function update(id, params) {
  const user = await getUser(id);

  // validate (if email was changed)
  if (
    params.email &&
    user.email !== params.email &&
    (await db.User.findOne({ where: { email: params.email } }))
  ) {
    throw {
      status: 500,
      message: 'Email "' + params.email + '" is already taken',
    };
  }

  // hash password if it was entered
  if (params.password) {
    params.passwordHash = await hash(params.password);
  }

  // copy params to user and save
  Object.assign(user, params);
  user.updated = Date.now();
  await user.save();
  logger.logEvent("info", "User updated", {
    action: "UpdateUser",
    userId: user.id,
    email: user.email,
  });
  return basicDetails(user);
}

async function _delete(id) {
  const user = await getUser(id);
  await user.destroy();
  logger.logEvent("warn", "User account deleted", {
    action: "DeleteUser",
    userId: user.id,
    email: user.email,
  });
}

// helper functions

async function getUser(id) {
  const user = await db.User.findByPk(id);
  if (!user) throw { status: 404, message: "User not found" };
  return user;
}

async function getUserByToken(token) {
  const refreshToken = await db.RefreshToken.findOne({ where: { token } });
  if (!refreshToken) throw { status: 404, message: "Refresh token not found" };
  return await getUser(refreshToken.userId);
}

async function getRefreshToken(token) {
  logger.logEvent("debug", "Querying for refresh token", {
    action: "GetRefreshToken",
    partialToken: token.slice(0, 6) + "...",
  });

  const refreshToken = await db.RefreshToken.findOne({ where: { token } });

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
    { id: user.id, role: user.role, clientId: user.clientId },
    config.jwtSecret,
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
    clientId,
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
    clientId,
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

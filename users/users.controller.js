﻿const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const Role = require("../helpers/role");
const userService = require("./user.service");
const { logger } = require("../helpers/logger");
const {
  authSchema,
  registerSchema,
  registerFirstUserSchema,
  forgotPasswordSchema,
  resetPasswordSchema,
  verifyEmailSchema,
  verifyTokenSchema,
  validateResetTokenSchema,
  createSchema,
} = require("./user.validator");

const setPasswordSchema = Joi.object({
  token: Joi.string().required(),
  password: Joi.string().min(6).required(),
  confirmPassword: Joi.string().valid(Joi.ref("password")).required(),
});

// routes
router.post("/authenticate", validateRequest(authSchema), authenticate);
router.post("/refresh-token", refreshToken);
router.post("/revoke-token", authorise(), revokeTokenSchema, revokeToken);
router.post("/register", validateRequest(registerSchema), register);
router.post(
  "/register-first-user",
  validateRequest(registerFirstUserSchema),
  registerFirstUser
);
router.post("/verify-email", validateRequest(verifyEmailSchema), verifyEmail);
router.post("/verify-token", validateRequest(verifyTokenSchema), verifyToken);
router.post(
  "/forgot-password",
  validateRequest(forgotPasswordSchema),
  forgotPassword
);
router.post(
  "/validate-reset-token",
  validateRequest(validateResetTokenSchema),
  validateResetToken
);
router.post(
  "/reset-password",
  validateRequest(resetPasswordSchema),
  resetPassword
);
router.post("/set-password", validateRequest(setPasswordSchema), setPassword);

router.get("/", authorise(["Admin", "Audit", "Boss"]), getAll);
router.get("/by-client", authorise(["Admin", "Boss"]), getAllByClientId);
router.get("/:id", authorise(), getById);
router.post(
  "/",
  authorise(["Admin", "Boss"]),
  validateRequest(createSchema),
  create
);
router.put("/:id", authorise(), updateSchema, update); // Need to resolve which schema to use
router.delete("/:id", authorise(["Admin", "Boss"]), _delete);

module.exports = router;

function authenticate(req, res, next) {
  const { email, password } = req.body;
  const ipAddress = req.ip;
  userService
    .authenticate({
      email,
      password,
      ipAddress,
      userAgent: req.headers["user-agent"],
      options: { transaction: req.dbTransaction },
    })
    .then(({ refreshToken, jwtToken, ...user }) => {
      setTokenCookie(res, refreshToken);
      res.json({ ...user, jwtToken });
      logger.logEvent("info", "User login successful", {
        action: "Authenticate",
        userId: user.id,
        email: user.email,
        ip: ipAddress,
        device: req.headers["user-agent"],
      });
    })
    .catch(next);
}

function refreshToken(req, res, next) {
  const token = req.cookies.refreshToken;
  const ipAddress = req.ip;

  if (!token || token === "undefined") return unauthorised(res);

  userService
    .refreshToken({
      token,
      ipAddress,
      userAgent: req.headers["user-agent"],
      options: { transaction: req.dbTransaction },
    })
    .then(({ refreshToken, jwtToken, ...user }) => {
      setTokenCookie(res, refreshToken);
      res.json({ ...user, jwtToken });
      logger.logEvent("info", "Refresh token issued", {
        action: "RefreshToken",
        userId: user.id,
        email: user.email,
        ip: ipAddress,
        device: req.headers["user-agent"],
      });
    })
    .catch((next) => {
      if (next.status === 400) {
        return unauthorised(res);
      }
      return res.status(500).json({ message: "Internal Server Error" });
    });
}

async function unauthorised(res) {
  return res.status(401).json({ message: "Unauthorised" });
}

function revokeTokenSchema(req, res, next) {
  const schema = Joi.object({
    refreshToken: Joi.string().empty(""), // Validate only refreshToken
  });
  return validateRequest(schema)(req, res, next);
}

function revokeToken(req, res, next) {
  const { body } = req;

  const token = body.refreshToken || req.cookies.refreshToken;
  const ipAddress = req.ip;

  if (!token) {
    return res.status(400).json({ message: "Token is required" });
  }

  if (!req.auth.ownsToken(token) && req.auth.role !== Role.Admin) {
    return res.status(401).json({ message: "Unauthorised" });
  }

  userService
    .revokeToken({
      token,
      ipAddress,
      userAgent: req.headers["user-agent"],
      options: { transaction: req.dbTransaction },
    })
    .then(() => {
      res.clearCookie("refreshToken", {
        httpOnly: true,
        secure: process.env.NODE_ENV === "production",
        sameSite: "Lax",
        path: "/",
        domain:
          process.env.NODE_ENV === "production"
            ? process.env.COOKIE_DOMAIN_PROD
            : process.env.COOKIE_DOMAIN_DEV,
      });
      logger.logEvent("info", "Refresh token revoked via controller", {
        action: "RevokeToken",
        token,
        ip: ipAddress,
        userId: req.auth.id,
        device: req.headers["user-agent"],
      });
      res.json({ message: "Token revoked" });
    })
    .catch((err) => {
      console.error("→ revokeToken error", JSON.stringify(err, null, 2));
      next(err);
    });
}

function register(req, res, next) {
  userService
    .register(req.body, req.get("origin"), req.headers["user-agent"], {
      transaction: req.dbTransaction,
    })
    .then(() =>
      res.json({
        message:
          "Registration successful, please check your email for verification instructions",
      })
    )
    .then(() => {
      logger.logEvent("info", "User registered via controller", {
        action: "Register",
        email: req.body.email,
        device: req.headers["user-agent"],
      });
    })
    .catch(next);
}

function registerFirstUser(req, res, next) {
  userService
    .registerFirstUser(req.body, req.get("origin"), req.headers["user-agent"], {
      transaction: req.dbTransaction,
    })
    .then(() =>
      res.json({
        message:
          "First user registration successful, please check your email for your welcome pack",
      })
    )
    .then(() => {
      logger.logEvent("info", "First user registered via controller", {
        action: "RegisterFirstUser",
        email: req.body.email,
        device: req.headers["user-agent"],
      });
    })
    .catch(next);
}

function verifyToken(req, res, next) {
  userService
    .verifyToken(req.body.token, { transaction: req.dbTransaction })
    .then(() => {
      res.json({ status: 200, message: "Token is valid" });
    })
    .catch((err) => {
      logger.logEvent("error", "Token verification failed", {
        action: "VerifyToken",
        error: err.message,
        stack: err.stack,
      });
      res.status(400).json({ status: 400, message: "Token is invalid" });
    });
}

function verifyEmail(req, res, next) {
  userService
    .verifyEmail(req.body, { transaction: req.dbTransaction })
    .then((user) => {
      res.clearCookie("refreshToken", {
        httpOnly: true,
        secure: process.env.NODE_ENV === "production",
        sameSite: "Lax",
        path: "/",
        domain:
          process.env.NODE_ENV === "production"
            ? process.env.COOKIE_DOMAIN_PROD
            : process.env.COOKIE_DOMAIN_DEV,
      });
      res.json(user);
    })
    .catch(next);
}

function forgotPassword(req, res, next) {
  userService
    .forgotPassword(req.body, req.get("origin"), {
      transaction: req.dbTransaction,
    })
    .then(() =>
      res.json({
        message: "Please check your email for password reset instructions",
      })
    )
    .then(() => {
      logger.logEvent("info", "Forgot password requested", {
        action: "ForgotPassword",
        email: req.body.email,
      });
    })
    .catch(next);
}

function validateResetToken(req, res, next) {
  userService
    .validateResetToken(req.body, { transaction: req.dbTransaction })
    .then(() => res.json({ message: "Token is valid" }))
    .catch(next);
}

function resetPassword(req, res, next) {
  userService
    .resetPassword(req.body, { transaction: req.dbTransaction })
    .then(() => {
      res.clearCookie("refreshToken", {
        httpOnly: true,
        secure: process.env.NODE_ENV === "production",
        sameSite: "Lax",
        path: "/",
        domain:
          process.env.NODE_ENV === "production"
            ? process.env.COOKIE_DOMAIN_PROD
            : process.env.COOKIE_DOMAIN_DEV,
      });
      res.json({ message: "Password reset successful, you can now login" });
    })
    .then(() => {
      logger.logEvent("info", "Password reset completed", {
        action: "ResetPassword",
        email: req.body.email,
      });
    })
    .catch(next);
}

function setPassword(req, res, next) {
  userService
    .setPassword(req.body, { transaction: req.dbTransaction })
    .then(({ refreshToken, jwtToken, ...user }) => {
      setTokenCookie(res, refreshToken);
      res.json({ ...user, jwtToken });
    })
    .catch(next);
}

function getAll(req, res, next) {
  userService
    .getAll({ transaction: req.dbTransaction })
    .then((users) => res.json(users))
    .catch(next);
}

function getAllByClientId(req, res, next) {
  const clientId = req.auth.clientId;

  userService
    .getAllByClientId(clientId, { transaction: req.dbTransaction })
    .then((users) => {
      if (users.length > 0) {
        logger.logEvent("info", "Fetched users by client ID", {
          action: "GetUsersByClient",
          clientId,
          requestedBy: req.auth.id,
        });
        res.json(users);
      } else {
        res
          .status(404)
          .json({ error: "No users found for this client", code: 404 });
      }
    })
    .catch(next);
}

function getById(req, res, next) {
  if (Number(req.params.id) !== req.user.id && req.user.role !== "Admin") {
    return res.status(401).json({ error: "Unauthorised", code: 401 });
  }

  userService
    .getById(req.params.id, { transaction: req.dbTransaction })
    .then((user) => {
      if (user) {
        logger.logEvent("info", "Fetched user by ID", {
          action: "GetUser",
          userId: req.params.id,
          requestedBy: req.user.id,
        });
        res.json(user);
      } else {
        res.status(404).json({ error: "User not found", code: 404 });
      }
    })
    .catch(next);
}

function create(req, res, next) {
  userService
    .create(req.body, { transaction: req.dbTransaction })
    .then((user) => res.json(user))
    .catch(next);
}

function updateSchema(req, res, next) {
  const schemaRules = {
    firstName: Joi.string().empty(""),
    lastName: Joi.string().empty(""),
    email: Joi.string().email().empty(""),
    password: Joi.string().min(6).empty(""),
    confirmPassword: Joi.string().valid(Joi.ref("password")).empty(""),
  };

  if (req.user.role === "Admin") {
    schemaRules.role = Joi.string().valid(Role.Admin, Role.User).empty("");
  }

  const schema = Joi.object(schemaRules).with("password", "confirmPassword");
  validateRequest(req, next, schema);
}

function update(req, res, next) {
  if (Number(req.params.id) !== req.user.id && req.user.role !== "Admin") {
    return res.status(401).json({ message: "Unauthorised" });
  }

  userService
    .update(req.params.id, req.body, { transaction: req.dbTransaction })
    .then((user) => res.json(user))
    .catch(next);
}

function _delete(req, res, next) {
  if (Number(req.params.id) !== req.user.id && req.user.role !== "Admin") {
    return res.status(401).json({ message: "Unauthorised" });
  }

  userService
    .delete(req.params.id, { transaction: req.dbTransaction })
    .then(() => {
      res.json({ message: "User deleted successfully" });
      logger.logEvent("warn", "User deleted via controller", {
        action: "DeleteUser",
        userId: req.params.id,
        requestedBy: req.user.id,
      });
    })
    .catch(next);
}

function setTokenCookie(res, token) {
  res.cookie("refreshToken", token, {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "Lax",
    path: "/",
    domain:
      process.env.NODE_ENV === "production"
        ? process.env.COOKIE_DOMAIN_PROD
        : process.env.COOKIE_DOMAIN_DEV,
  });
}

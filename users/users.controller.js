const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("@/middleware/validate-request");
const authorise = require("@/middleware/authorise");
const Role = require("@/helpers/role");
const userService = require("./user.service");
const { logger } = require("@/helpers/logger");
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
  inviteWithResourceSchema,
  refreshTokenSchema,
  revokeTokenSchema,
} = require("./user.validator");

const setPasswordSchema = Joi.object({
  token: Joi.string().required(),
  password: Joi.string().min(6).required(),
  confirmPassword: Joi.string().valid(Joi.ref("password")).required(),
});

// routes
router.post("/authenticate", validateRequest(authSchema), authenticate);
router.post(
  "/refresh-token",
  validateRequest(refreshTokenSchema),
  refreshToken
);
router.post("/revoke-token", validateRequest(revokeTokenSchema), revokeToken);
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

router.post(
  "/invite-with-resource",
  authorise(["Admin", "Boss"]),
  validateRequest(inviteWithResourceSchema),
  inviteWithResource
);

router.get("/", authorise(["Admin", "Audit", "Boss"]), getAll);
router.get("/by-customer", authorise(["Admin", "Boss"]), getAllByCustomerId);
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
  const ip = req.ip;
  const device = req.headers["user-agent"];
  userService
    .authenticate({
      email,
      password,
      ipAddress: ip,
    })
    .then(({ refreshToken, jwtToken, entitlements, ...user }) => {
      setTokenCookie(res, refreshToken);
      res.json({ ...user, jwtToken, entitlements });
      logger.logEvent("info", "User login successful", {
        action: "Authenticate",
        userId: user.id,
        email: user.email,
        ip,
        device,
      });
    })
    .catch(next);
}

function refreshToken(req, res, next) {
  const token = req.cookies.refreshToken;
  const ip = req.ip;
  const device = req.headers["user-agent"];

  if (!token || token === "undefined") return unauthorised(res);

  userService
    .refreshToken({
      token,
      ipAddress: ip,
    })
    .then(({ refreshToken, jwtToken, entitlements, ...user }) => {
      setTokenCookie(res, refreshToken);
      res.json({ ...user, jwtToken, entitlements });
      logger.logEvent("info", "Refresh token issued", {
        action: "RefreshToken",
        userId: user.id,
        email: user.email,
        ip,
        device,
        when: new Date(),
      });
    })
    .catch((err) => {
      if (err && err.status === 400) {
        // Clear any stale refresh cookie to prevent FE retry loops
        clearRefreshCookie(res);
        return unauthorised(res);
      }
      return res.status(500).json({ message: "Internal Server Error" });
    });
}

async function unauthorised(res) {
  return res.status(401).json({ message: "Unauthorised" });
}

function revokeToken(req, res, next) {
  const { body } = req;
  const ip = req.ip;
  const device = req.headers["user-agent"];

  const token = body.refreshToken || req.cookies.refreshToken;

  // Always clear the cookie, regardless of token validity
  const clearCookie = () => clearRefreshCookie(res);

  // If there is no token at all, treat as success (idempotent logout)
  if (!token) {
    clearCookie();
    logger.logEvent("info", "No refresh token provided; treated as revoked", {
      action: "RevokeToken",
      ip,
      device,
    });
    return res.json({ message: "Token revoked" });
  }

  // Proceed to revoke without requiring an authenticated user; possession of token is sufficient
  userService
    .revokeToken({
      token,
      ipAddress: ip,
    })
    .then(() => {
      clearCookie();
      logger.logEvent("info", "Refresh token revoked via controller", {
        action: "RevokeToken",
        token,
        ip,
        device,
      });
      res.json({ message: "Token revoked" });
    })
    .catch((err) => {
      // If token not found or not active, treat as success to avoid noisy loops
      if (err && err.status === 400) {
        clearCookie();
        logger.logEvent(
          "info",
          "Refresh token already invalid; treated as revoked",
          {
            action: "RevokeToken",
            token,
            ip,
            device,
          }
        );
        return res.json({ message: "Token revoked" });
      }
      console.error("→ revokeToken error", JSON.stringify(err, null, 2));
      next(err);
    });
}

function register(req, res, next) {
  const ip = req.ip;
  const device = req.headers["user-agent"];
  userService
    .register(req.body, req.get("origin"), device)
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
        ip,
        device,
      });
    })
    .catch(next);
}

function registerFirstUser(req, res, next) {
  const ip = req.ip;
  const device = req.headers["user-agent"];
  userService
    .registerFirstUser(req.body, req.get("origin"), device)
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
        ip,
        device,
      });
    })
    .catch(next);
}

function verifyToken(req, res, next) {
  console.log("verify in controller");
  userService
    .verifyToken(req.body.token)
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
    .verifyEmail(req.body)
    .then((user) => {
      clearRefreshCookie(res);
      res.json(user);
    })
    .catch(next);
}

function forgotPassword(req, res, next) {
  const ip = req.ip;
  const device = req.headers["user-agent"];
  userService
    .forgotPassword(req.body, req.get("origin"))
    .then(() =>
      res.json({
        message: "Please check your email for password reset instructions",
      })
    )
    .then(() => {
      logger.logEvent("info", "Forgot password requested", {
        action: "ForgotPassword",
        email: req.body.email,
        ip,
        device,
      });
    })
    .catch(next);
}

function validateResetToken(req, res, next) {
  userService
    .validateResetToken(req.body)
    .then(() => res.json({ message: "Token is valid" }))
    .catch(next);
}

function resetPassword(req, res, next) {
  const ip = req.ip;
  const device = req.headers["user-agent"];
  userService
    .resetPassword(req.body)
    .then(() => {
      clearRefreshCookie(res);
      res.json({ message: "Password reset successful, you can now login" });
    })
    .then(() => {
      logger.logEvent("info", "Password reset completed", {
        action: "ResetPassword",
        email: req.body.email,
        ip,
        device,
      });
    })
    .catch(next);
}

function setPassword(req, res, next) {
  userService
    .setPassword(req.body)
    .then(({ refreshToken, jwtToken, ...user }) => {
      setTokenCookie(res, refreshToken);
      res.json({ ...user, jwtToken });
    })
    .catch(next);
}

function getAll(req, res, next) {
  userService
    .getAll()
    .then((users) => res.json(users))
    .catch(next);
}

function getAllByCustomerId(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const ip = req.ip;
  const device = req.headers["user-agent"];

  userService
    .getAllByCustomerId(customerId)
    .then((users) => {
      if (users.length > 0) {
        logger.logEvent("info", "Fetched users by customer ID", {
          action: "GetUsersByCustomer",
          customerId,
          requestedBy: req.auth.id,
          ip,
          device,
        });
        res.json(users);
      } else {
        res
          .status(404)
          .json({ error: "No users found for this customer", code: 404 });
      }
    })
    .catch(next);
}

function getById(req, res, next) {
  const ip = req.ip;
  const device = req.headers["user-agent"];
  if (req.params.id !== req.auth?.id && req.auth?.role !== Role.Admin) {
    return res.status(401).json({ error: "Unauthorised", code: 401 });
  }

  userService
    .getById(req.params.id)
    .then((user) => {
      if (user) {
        logger.logEvent("info", "Fetched user by ID", {
          action: "GetUser",
          userId: req.params.id,
          requestedBy: req.auth.id,
          ip,
          device,
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
    .create(req.body)
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

  if (req.auth?.role === "Admin") {
    schemaRules.role = Joi.string().valid(Role.Admin, Role.User).empty("");
  }

  const schema = Joi.object(schemaRules).with("password", "confirmPassword");
  validateRequest(req, next, schema);
}

function update(req, res, next) {
  if (req.params.id !== req.auth?.id && req.auth?.role !== Role.Admin) {
    return res.status(401).json({ message: "Unauthorised" });
  }

  userService
    .update(req.params.id, req.body)
    .then((user) => res.json(user))
    .catch(next);
}

function _delete(req, res, next) {
  const ip = req.ip;
  const device = req.headers["user-agent"];
  if (req.params.id !== req.auth?.id && req.auth?.role !== Role.Admin) {
    return res.status(401).json({ message: "Unauthorised" });
  }

  userService
    .delete(req.params.id)
    .then(() => {
      res.json({ message: "User deleted successfully" });
      logger.logEvent("warn", "User deleted via controller", {
        action: "DeleteUser",
        userId: req.params.id,
        requestedBy: req.auth.id,
        ip,
        device,
      });
    })
    .catch(next);
}

function inviteWithResource(req, res, next) {
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const { user, resource, createdBy } = req.body;
  const createdById = createdBy || req.auth?.id;
  const actingCustomerId = req.effectiveCustomerId;
  const safeUser = { ...user, customerId: actingCustomerId };

  userService
    .inviteWithResource({
      user: safeUser,
      resource,
      createdBy: createdById,
      origin: req.get("origin"),
    })
    .then((result) => {
      logger.logEvent("info", "Invited user and created linked resource", {
        action: "InviteWithResource",
        invitedEmail: safeUser.email,
        resourceName: resource.name,
        customerId: actingCustomerId,
        createdBy: createdById,
        ip,
        device,
      });
      res.json(result);
    })
    .catch(next);
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

const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const Role = require("../helpers/role");
const userService = require("./user.service");
const {
  authSchema,
  registerSchema,
  _updateSchema,
  updatePasswordSchema,
  forgotPasswordSchema,
  resetPasswordSchema,
  verifyEmailSchema,
  validateResetTokenSchema,
  createSchema,
} = require("./user.validator");

// routes
router.post("/authenticate", validateRequest(authSchema), authenticate);
router.post("/refresh-token", refreshToken);
router.post("/revoke-token", authorise(), revokeTokenSchema, revokeToken);
router.post("/register", validateRequest(registerSchema), register);
router.post("/verify-email", validateRequest(verifyEmailSchema), verifyEmail);
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

router.get("/", authorise(Role.Admin), getAll);
router.get("/:id", authorise(), getById);
router.post("/", authorise(Role.Admin), validateRequest(createSchema), create);
router.put("/:id", authorise(), updateSchema, update); // Need to resolve which schema to use
router.delete("/:id", authorise(), _delete);

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
    })
    .then(({ refreshToken, ...user }) => {
      setTokenCookie(res, refreshToken);
      res.json(user);
    })
    .catch(next);
}

function refreshToken(req, res, next) {
  const token = req.cookies.refreshToken;
  const ipAddress = req.ip;

  if (!token || token === "undefined") return unauthorised(res);

  userService
    .refreshToken({ token, ipAddress, userAgent: req.headers["user-agent"] })
    .then(({ refreshToken, ...user }) => {
      setTokenCookie(res, refreshToken);
      res.json(user);
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
    .revokeToken({ token, ipAddress, userAgent: req.headers["user-agent"] })
    .then(() => {
      res.clearCookie("refreshToken", {
        httpOnly: true,
        secure: false,
        sameSite: "Lax",
        path: "/",
        domain: "localhost",
      });
      res.json({ message: "Token revoked" });
    })
    .catch((err) => {
      console.error("→ revokeToken error", JSON.stringify(err, null, 2));
      next(err);
    });
}

function register(req, res, next) {
  // console.log("req.body", req.body, req.get("origin"));
  userService
    .register(req.body, req.get("origin"), req.headers["user-agent"])
    .then(() =>
      res.json({
        message:
          "Registration successful, please check your email for verification instructions",
      })
    )
    .catch(next);
}

function verifyEmail(req, res, next) {
  userService
    .verifyEmail(req.body)
    .then(() => {
      res.clearCookie("refreshToken", {
        httpOnly: true,
        secure: false,
        sameSite: "Lax",
        path: "/",
        domain: "localhost",
      });
      res.json({ message: "Verification successful, you can now login" });
    })
    .catch(next);
}

function forgotPassword(req, res, next) {
  userService
    .forgotPassword(req.body, req.get("origin"))
    .then(() =>
      res.json({
        message: "Please check your email for password reset instructions",
      })
    )
    .catch(next);
}

function validateResetToken(req, res, next) {
  userService
    .validateResetToken(req.body)
    .then(() => res.json({ message: "Token is valid" }))
    .catch(next);
}

function resetPassword(req, res, next) {
  userService
    .resetPassword(req.body)
    .then(() => {
      res.clearCookie("refreshToken", {
        httpOnly: true,
        secure: false,
        sameSite: "Lax",
        path: "/",
        domain: "localhost",
      });
      res.json({ message: "Password reset successful, you can now login" });
    })
    .catch(next);
}

function getAll(req, res, next) {
  userService
    .getAll()
    .then((users) => res.json(users))
    .catch(next);
}

function getById(req, res, next) {
  if (Number(req.params.id) !== req.user.id && req.user.role !== Role.Admin) {
    return res.status(401).json({ error: "Unauthorised", code: 401 });
  }

  userService
    .getById(req.params.id)
    .then((user) =>
      user
        ? res.json(user)
        : res.status(404).json({ error: "User not found", code: 404 })
    )
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

  if (req.user.role === Role.Admin) {
    schemaRules.role = Joi.string().valid(Role.Admin, Role.User).empty("");
  }

  const schema = Joi.object(schemaRules).with("password", "confirmPassword");
  validateRequest(req, next, schema);
}

function update(req, res, next) {
  if (Number(req.params.id) !== req.user.id && req.user.role !== Role.Admin) {
    return res.status(401).json({ message: "Unauthorised" });
  }

  userService
    .update(req.params.id, req.body)
    .then((user) => res.json(user))
    .catch(next);
}

function _delete(req, res, next) {
  if (Number(req.params.id) !== req.user.id && req.user.role !== Role.Admin) {
    return res.status(401).json({ message: "Unauthorised" });
  }

  userService
    .delete(req.params.id)
    .then(() => res.json({ message: "User deleted successfully" }))
    .catch(next);
}

function setTokenCookie(res, token) {
  res.cookie("refreshToken", token, {
    httpOnly: true,
    secure: false,
    sameSite: "Lax",
    path: "/",
    domain: "localhost",
  });
}

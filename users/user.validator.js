const Joi = require("../middleware/joiSanitizer");

const authSchema = Joi.object({
  email: Joi.string().email().required(),
  password: Joi.string().required(),
});

const registerSchema = Joi.object({
  firstName: Joi.string().required(),
  lastName: Joi.string().required(),
  email: Joi.string().email().required(),
  phone: Joi.string().required(),
  position: Joi.string().required(),
  // password: Joi.string().min(6).required(),
  // confirmPassword: Joi.string().valid(Joi.ref("password")).required(),
  role: Joi.string()
    // .valid(
    //   Role.Admin,
    //   Role.User,
    //   Role.Approver,
    //   Role.Submitter,
    //   Role.Approver
    // )
    .required(),
  active: Joi.boolean().required(),
  clientId: Joi.string().required(),
});

const registerFirstUserSchema = Joi.object({
  firstName: Joi.string().required(),
  lastName: Joi.string().required(),
  email: Joi.string().email().required(),
  phone: Joi.string().required(),
  position: Joi.string().required(),
  password: Joi.string().min(8).required(),
  confirmPassword: Joi.string().valid(Joi.ref("password")).required(),
  role: Joi.string().required(),
  active: Joi.boolean().required(),
  clientId: Joi.string().required(),
});

const _updateSchema = Joi.object({
  firstName: Joi.string().empty(""),
  lastName: Joi.string().empty(""),
  email: Joi.string().email().empty(""),
  password: Joi.string().min(8).empty(""),
  confirmPassword: Joi.string().valid(Joi.ref("password")).empty(""),
}).with("password", "confirmPassword");

const updatePasswordSchema = Joi.object({
  oldPassword: Joi.string().required(),
  newPassword: Joi.string().min(8).required(),
  confirmPassword: Joi.string().valid(Joi.ref("newPassword")).required(),
}).with("newPassword", "confirmPassword");

const forgotPasswordSchema = Joi.object({
  email: Joi.string().email().required(),
});

const resetPasswordSchema = Joi.object({
  password: Joi.string().min(8).required(),
  confirmPassword: Joi.string().valid(Joi.ref("password")).required(),
}).with("password", "confirmPassword");

const verifyEmailSchema = Joi.object({
  token: Joi.string().required(),
  password: Joi.string().min(8).required(),
  confirmPassword: Joi.string().valid(Joi.ref("password")).required(),
}).with("password", "confirmPassword");

const validateResetTokenSchema = Joi.object({
  token: Joi.string().required(),
});

module.exports = {
  authSchema,
  registerSchema,
  registerFirstUserSchema,
  _updateSchema,
  updatePasswordSchema,
  forgotPasswordSchema,
  resetPasswordSchema,
  verifyEmailSchema,
  validateResetTokenSchema,
  createSchema: registerSchema,
};

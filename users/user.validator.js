const Joi = require("@/middleware/joiSanitizer");
const Role = require("@/helpers/role");

const authSchema = Joi.object({
  email: Joi.string().email().required(),
  password: Joi.string().required(),
}).meta({ requireCustomer: false });

const registerSchema = Joi.object({
  firstName: Joi.string().required(),
  lastName: Joi.string().required(),
  email: Joi.string().email().required(),
  phone: Joi.string().optional(),
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
  customerId: Joi.string().required(),
});

const registerFirstUserSchema = Joi.object({
  firstName: Joi.string().required(),
  lastName: Joi.string().required(),
  email: Joi.string().email().required(),
  phone: Joi.string().required(),
  password: Joi.string().min(8).required(),
  confirmPassword: Joi.string().valid(Joi.ref("password")).required(),
  role: Joi.string().required(),
  active: Joi.boolean().required(),
  customerId: Joi.string().required(),
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

const verifyTokenSchema = Joi.object({
  token: Joi.string().required(),
}).meta({ requireCustomer: false });

const verifyEmailSchema = Joi.object({
  token: Joi.string().required(),
  password: Joi.string().min(8).required(),
  confirmPassword: Joi.string().valid(Joi.ref("password")).required(),
}).meta({ requireCustomer: false });

const validateResetTokenSchema = Joi.object({
  token: Joi.string().required(),
});

const refreshTokenSchema = Joi.object({}).meta({ requireCustomer: false });

const revokeTokenSchema = Joi.object({
  refreshToken: Joi.string().allow("").optional(),
}).meta({ requireCustomer: false });

// Composite: invite user + create linked resource (Admin/Boss only)
const inviteWithResourceSchema = Joi.object({
  user: Joi.object({
    email: Joi.string().email().required(),
    role: Joi.string()
      .valid(Role.User, Role.Admin, Role.Boss)
      .default(Role.User),
    firstName: Joi.string().allow(""),
    lastName: Joi.string().allow(""),
    customerId: Joi.string().required(),
    active: Joi.boolean(),
  }).required(),
  resource: Joi.object({
    name: Joi.string().required(),
    position: Joi.string().required(),
    hourlyRate: Joi.number().min(0).allow(null),
    capacityHoursPerWeek: Joi.number().min(0).max(168).allow(null),
  }).required(),
  createdBy: Joi.string().allow(""),
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
  verifyTokenSchema,
  validateResetTokenSchema,
  refreshTokenSchema,
  revokeTokenSchema,
  createSchema: registerSchema,
  inviteWithResourceSchema,
};

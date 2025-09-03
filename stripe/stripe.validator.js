const Joi = require("../middleware/joiSanitizer");

// Base schema reflecting tbl_stripe_user
const base = Joi.object({
  // Tenant + linkage
  customerId: Joi.string().length(10).required(),
  userId: Joi.string().length(10).required(),

  // Stripe linkage (nullable)
  stripeCustomerId: Joi.string().max(255).allow(null).optional().sanitize(),
  stripeSubscriptionId: Joi.string().max(255).allow(null).optional().sanitize(),
  stripePriceId: Joi.string().max(255).allow(null).optional().sanitize(),

  // Plan & seats
  planCode: Joi.string().max(120).allow(null).optional().sanitize(),
  // Seats come from the frontend (/Users/darryllrobinson/Projects/compliance_reporting/src/features/users/FirstUserRegister.js), default 20 here as a safeguard.
  seats: Joi.number().integer().min(1).max(500).default(20),

  // Status/flags
  isActive: Joi.boolean().optional(),
  status: Joi.string().max(64).optional().sanitize(),

  // Timing
  trialEndsAt: Joi.date().optional(),
  introPriceEndsAt: Joi.date().optional(),

  // Audit
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),

  // Server-managed
  id: Joi.string().max(10),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
});

// POST: require createdBy; forbid updatedBy & server-managed fields
const stripeUserCreateSchema = base
  .fork(["createdBy"], (s) => s.required())
  .fork(["updatedBy", "id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PUT: require updatedBy; forbid server-managed fields
const stripeUserUpdateSchema = base
  .fork(["updatedBy"], (s) => s.required())
  .fork(["id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PATCH: partial updates; require updatedBy
const stripeUserPatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

module.exports = {
  stripeUserCreateSchema,
  stripeUserUpdateSchema,
  stripeUserPatchSchema,
};

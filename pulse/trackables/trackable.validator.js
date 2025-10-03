const Joi = require("@/middleware/joiSanitizer");

const base = Joi.object({
  name: Joi.string().max(255).required().sanitize(),
  startDate: Joi.date().optional(),
  endDate: Joi.date().optional(),
  status: Joi.string()
    .valid("draft", "budgeted", "ready", "active", "cancelled")
    .default("draft")
    .sanitize(),
  statusChangedAt: Joi.date().optional(),
  budgetHours: Joi.forbidden(),
  budgetAmount: Joi.forbidden(),

  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),

  id: Joi.string().max(10),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  customerId: Joi.string().length(10).required().sanitize(),
});

// POST: require createdBy; forbid updatedBy & server-managed fields
const trackableCreateSchema = base
  .fork(["createdBy"], (s) => s.required())
  .fork(["updatedBy", "id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PUT: require updatedBy; forbid server-managed fields
const trackableUpdateSchema = base
  .fork(["updatedBy"], (s) => s.required())
  .fork(["id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PATCH: partial updates; require updatedBy
const trackablePatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

module.exports = {
  trackableCreateSchema,
  trackableUpdateSchema,
  trackablePatchSchema,
};

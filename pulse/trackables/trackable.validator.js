const Joi = require("@/middleware/joiSanitizer");

const base = Joi.object({
  name: Joi.string().max(255).required().sanitize(),

  // optional tenant link
  clientId: Joi.string().length(10).optional().sanitize(),

  // required in create, optional in update (via forks below)
  startDate: Joi.date(),
  endDate: Joi.date().min(Joi.ref("startDate")),

  status: Joi.string()
    .valid("draft", "budgeted", "ready", "active", "cancelled")
    .default("draft")
    .sanitize(),
  statusChangedAt: Joi.date().optional(),

  // tenant + actor
  customerId: Joi.string().length(10).required().sanitize(),
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),

  // server-managed (never from FE)
  id: Joi.forbidden(),
  createdAt: Joi.forbidden(),
  updatedAt: Joi.forbidden(),
});

// POST: require createdBy and the planning fields
const trackableCreateSchema = base
  .fork(["createdBy", "startDate", "endDate"], (s) => s.required())
  .fork(["updatedBy"], (s) => s.forbidden());

// PUT: require updatedBy; allow partial edits to planning fields
const trackableUpdateSchema = base.fork(["updatedBy"], (s) => s.required());

// PATCH: partials; still require updatedBy
const trackablePatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

module.exports = {
  trackableCreateSchema,
  trackableUpdateSchema,
  trackablePatchSchema,
};

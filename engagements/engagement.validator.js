const Joi = require("../middleware/joiSanitizer");

const base = Joi.object({
  name: Joi.string().max(255).required().sanitize(),
  startDate: Joi.date().optional(),
  endDate: Joi.date().optional(),
  status: Joi.string()
    .valid("draft", "budgeted", "ready", "active", "cancelled")
    .sanitize()
    .required(),
  statusChangedAt: Joi.date().optional(),
  budgetHours: Joi.number().min(0).optional(),
  budgetAmount: Joi.number().min(0).optional(),

  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),

  id: Joi.string().max(10),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  customerId: Joi.string().length(10).required().sanitize(),
});

// POST: require createdBy; forbid updatedBy & server-managed fields
const engagementCreateSchema = base
  .fork(["createdBy"], (s) => s.required())
  .fork(["updatedBy", "id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PUT: require updatedBy; forbid server-managed fields
const engagementUpdateSchema = base
  .fork(["updatedBy"], (s) => s.required())
  .fork(["id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PATCH: partial updates; require updatedBy
const engagementPatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

module.exports = {
  engagementCreateSchema,
  engagementUpdateSchema,
  engagementPatchSchema,
};

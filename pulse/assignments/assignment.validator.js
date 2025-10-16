const Joi = require("@/middleware/joiSanitizer");

const base = Joi.object({
  budgetItemId: Joi.string().length(10).required().sanitize(),
  resourceId: Joi.string().length(10).required().sanitize(),
  assignmentPct: Joi.number().integer().min(0).max(100).required(),
  role: Joi.string().max(200).optional().sanitize(),
  rateOverride: Joi.number().min(0).optional(),
  startDate: Joi.date().optional(),
  endDate: Joi.date().optional(),
  dueDate: Joi.date().optional(),
  completedAt: Joi.date().optional(),
  assignedHoursPerWeek: Joi.number().min(0).optional(),
  notes: Joi.string().max(2000).optional().sanitize(),

  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),

  id: Joi.string().max(10),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  customerId: Joi.string().length(10).required().sanitize(),
});

// POST: require createdBy; forbid updatedBy and server-managed fields
const assignmentCreateSchema = base
  .fork(["createdBy"], (s) => s.required())
  .fork(["updatedBy", "id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PUT: require updatedBy; createdBy optional (or forbid if preferred)
const assignmentUpdateSchema = base
  .fork(["updatedBy"], (s) => s.required())
  .fork(["id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PATCH: partial updates allowed, but require updatedBy for audit trail
const assignmentPatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

module.exports = {
  assignmentCreateSchema,
  assignmentUpdateSchema,
  assignmentPatchSchema,
};

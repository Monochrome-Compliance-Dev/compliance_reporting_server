const Joi = require("../middleware/joiSanitizer");

const base = Joi.object({
  name: Joi.string().max(255).required().sanitize(),
  position: Joi.string().max(120).required().sanitize(),
  team: Joi.string().max(120).optional().allow(null).sanitize(),
  hourlyRate: Joi.number().min(0).optional(),
  capacityHoursPerWeek: Joi.number().integer().min(0).optional(),
  email: Joi.string().email().optional().sanitize(),
  userId: Joi.string().length(10).allow(null).optional().sanitize(),

  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),

  id: Joi.string().max(10),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  customerId: Joi.string().length(10).required(),
});

// POST: require createdBy; forbid updatedBy & server-managed fields
const resourceCreateSchema = base
  .fork(["createdBy"], (s) => s.required())
  .fork(["updatedBy", "id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PUT: require updatedBy; forbid server-managed fields
const resourceUpdateSchema = base
  .fork(["updatedBy"], (s) => s.required())
  .fork(["id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PATCH: partial updates; require updatedBy
const resourcePatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

module.exports = {
  resourceCreateSchema,
  resourceUpdateSchema,
  resourcePatchSchema,
};

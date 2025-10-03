const Joi = require("@/middleware/joiSanitizer");

const base = Joi.object({
  name: Joi.string().max(255).required().sanitize(),
  abn: Joi.string().max(20).optional().sanitize(),
  email: Joi.string().max(255).optional().sanitize(),
  phone: Joi.string().max(10).optional().sanitize(),

  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),

  id: Joi.string().max(10),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  customerId: Joi.string().length(10).required(),
});

// POST: require createdBy; forbid updatedBy & server-managed fields
const clientCreateSchema = base
  .fork(["createdBy"], (s) => s.required())
  .fork(["updatedBy", "id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PUT: require updatedBy; forbid server-managed fields
const clientUpdateSchema = base
  .fork(["updatedBy"], (s) => s.required())
  .fork(["id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PATCH: partial updates; require updatedBy
const clientPatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

module.exports = { clientCreateSchema, clientUpdateSchema, clientPatchSchema };

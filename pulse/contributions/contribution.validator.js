const Joi = require("@/middleware/joiSanitizer");

const base = Joi.object({
  resourceId: Joi.string().length(10).required().sanitize(),
  budgetItemId: Joi.string().length(10).required().sanitize(),
  assignmentId: Joi.string().length(10).optional().allow(null).sanitize(),
  effortHours: Joi.number().positive().precision(2).multiple(0.25).required(),
  notes: Joi.string().max(500).allow("", null).optional().sanitize(),
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),
  id: Joi.string().max(10),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  customerId: Joi.string().length(10).required(),
});

const contributionCreateSchema = base
  .fork(["createdBy"], (s) => s.required())
  .fork(["updatedBy", "id", "createdAt", "updatedAt"], (s) => s.forbidden());

const contributionUpdateSchema = base
  .fork(["updatedBy"], (s) => s.required())
  .fork(["id", "createdAt", "updatedAt"], (s) => s.forbidden());

const contributionPatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

module.exports = {
  contributionCreateSchema,
  contributionUpdateSchema,
  contributionPatchSchema,
};

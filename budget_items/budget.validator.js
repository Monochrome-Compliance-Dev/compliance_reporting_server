const Joi = require("../middleware/joiSanitizer");

const base = Joi.object({
  engagementId: Joi.string().length(10).required().sanitize(),
  activity: Joi.string().max(200).required().sanitize(),
  billingType: Joi.string().valid("hourly", "fixed").required().sanitize(),
  hours: Joi.number().min(0).optional(),
  rate: Joi.number().min(0).optional(),
  amount: Joi.number().min(0).optional(),
  billable: Joi.boolean().optional(),
  notes: Joi.string().max(2000).optional().sanitize(),

  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),

  id: Joi.string().max(10),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  customerId: Joi.string().length(10).required(),
});

// POST: require createdBy, forbid updatedBy and server-managed fields
const budgetItemCreateSchema = base
  .fork(["createdBy"], (s) => s.required())
  .fork(["updatedBy", "id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PUT: require updatedBy, forbid server-managed fields
const budgetItemUpdateSchema = base
  .fork(["updatedBy"], (s) => s.required())
  .fork(["id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PATCH: partial updates, require updatedBy
const budgetItemPatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

module.exports = {
  budgetItemCreateSchema,
  budgetItemUpdateSchema,
  budgetItemPatchSchema,
};

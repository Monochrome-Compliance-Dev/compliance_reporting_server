const Joi = require("@/middleware/joiSanitizer");

// ================= Budget Item Schemas =================
const itemBase = Joi.object({
  budgetId: Joi.string().length(10).required().sanitize(),

  sectionId: Joi.string().length(10).allow(null).optional().sanitize(),
  order: Joi.number().integer().min(0).optional(),
  resourceLabel: Joi.string().max(200).required().sanitize(),

  sectionName: Joi.string().max(200).optional().sanitize(),
  billingType: Joi.string().valid("hourly", "fixed").required().sanitize(),

  // Numeric fields (conditional rules applied below)
  hours: Joi.number().min(0).optional(),
  rate: Joi.number().min(0).optional(),
  amount: Joi.number().min(0).optional(),

  billable: Joi.boolean().optional(),
  notes: Joi.string().max(2000).allow("", null).optional().sanitize(),
  purpose: Joi.string().max(100).required().sanitize(),

  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),

  id: Joi.string().max(10),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  customerId: Joi.string().length(10).required(),
})
  // Enforce numeric combinations based on billingType
  .when(Joi.object({ billingType: Joi.valid("hourly") }).unknown(), {
    then: Joi.object({
      hours: Joi.number().min(0).required(),
      rate: Joi.number().min(0).required(),
      amount: Joi.number().min(0).max(0).optional(), // force 0/absent for hourly rows
    }),
  })
  .when(Joi.object({ billingType: Joi.valid("fixed") }).unknown(), {
    then: Joi.object({
      amount: Joi.number().min(0).required(),
      hours: Joi.number().min(0).max(0).optional(), // force 0/absent for fixed rows
      rate: Joi.number().min(0).max(0).optional(),
    }),
  });

const budgetItemCreateSchema = itemBase
  .fork(["createdBy"], (s) => s.required())
  .fork(["updatedBy", "id", "createdAt", "updatedAt"], (s) => s.forbidden());

const budgetItemUpdateSchema = itemBase
  .fork(["updatedBy"], (s) => s.required())
  .fork(["id", "createdAt", "updatedAt"], (s) => s.forbidden());

const budgetItemPatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

// ================= Budgets (Entity) Schemas =================
const budgetBase = Joi.object({
  name: Joi.string().max(200).required().sanitize(),
  status: Joi.string().valid("draft", "final", "archived").default("draft"),
  version: Joi.number().integer().min(1).default(1),
  currency: Joi.string().uppercase().length(3).default("AUD"),

  trackableId: Joi.string().length(10).optional().sanitize(),
  isActive: Joi.boolean().default(true),
  startsAt: Joi.date().optional(),
  endsAt: Joi.date().optional(),
  reason: Joi.string().max(200).allow("", null).optional().sanitize(),

  notes: Joi.string().max(2000).allow("", null).optional().sanitize(),

  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),

  id: Joi.string().max(10),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  deletedAt: Joi.date().optional(),

  customerId: Joi.string().length(10).required(),
});

const budgetCreateSchema = budgetBase
  .fork(["createdBy"], (s) => s.required())
  .fork(["updatedBy", "id", "createdAt", "updatedAt", "deletedAt"], (s) =>
    s.forbidden()
  );

const budgetUpdateSchema = budgetBase
  .fork(["updatedBy"], (s) => s.required())
  .fork(["id", "createdAt", "updatedAt", "deletedAt"], (s) => s.forbidden());

const budgetPatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

// ================= Budget Section Schemas =================
const sectionBase = Joi.object({
  name: Joi.string().max(200).required().sanitize(),
  budgetId: Joi.string().length(10).required().sanitize(),
  order: Joi.number().integer().min(0).optional(),
  notes: Joi.string().max(2000).allow("", null).optional().sanitize(),

  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),

  id: Joi.string().max(10),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  deletedAt: Joi.date().optional(),

  customerId: Joi.string().length(10).required(),
});

const sectionCreateSchema = sectionBase
  .fork(["createdBy"], (s) => s.required())
  .fork(["updatedBy", "id", "createdAt", "updatedAt", "deletedAt"], (s) =>
    s.forbidden()
  );

const sectionUpdateSchema = sectionBase
  .fork(["updatedBy"], (s) => s.required())
  .fork(["id", "createdAt", "updatedAt", "deletedAt"], (s) => s.forbidden());

const sectionPatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

module.exports = {
  // Items
  budgetItemCreateSchema,
  budgetItemUpdateSchema,
  budgetItemPatchSchema,
  // Budgets
  budgetCreateSchema,
  budgetUpdateSchema,
  budgetPatchSchema,
  // Sections
  sectionCreateSchema,
  sectionUpdateSchema,
  sectionPatchSchema,
};

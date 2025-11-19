const Joi = require("../../middleware/joiSanitizer");

const base = Joi.object({
  id: Joi.string().length(10),
  name: Joi.string().max(255).sanitize(),
  description: Joi.string().allow(null, "").sanitize(),
  product: Joi.string().valid("ptrs", "pulse").sanitize(),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  deletedAt: Joi.date().optional(),
});

const customerProfileCreateSchema = base
  .fork(["name", "product"], (schema) => schema.required())
  .fork(["id", "createdAt", "updatedAt", "deletedAt"], (schema) =>
    schema.forbidden()
  );

const customerProfileUpdateSchema = base.fork(
  ["id", "createdAt", "updatedAt", "deletedAt"],
  (schema) => schema.forbidden()
);

module.exports = {
  customerProfileCreateSchema,
  customerProfileUpdateSchema,
};

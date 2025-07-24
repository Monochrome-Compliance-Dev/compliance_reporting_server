const Joi = require("../middleware/joiSanitizer");

const productSchema = Joi.object({
  name: Joi.string().required(),
  code: Joi.string().alphanum().required(),
  type: Joi.string().valid("solution", "module", "addon").required(),
  amount: Joi.number().precision(2).required(),
  active: Joi.boolean().default(true),
  createdBy: Joi.string().alphanum().length(10).required(),
});

const productUpdateSchema = Joi.object({
  name: Joi.string(),
  code: Joi.string().alphanum(),
  type: Joi.string().valid("solution", "module", "addon"),
  amount: Joi.number().precision(2),
  active: Joi.boolean(),
  updatedBy: Joi.string().alphanum().length(10).required(),
});

module.exports = { productSchema, productUpdateSchema };

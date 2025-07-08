const Joi = require("../middleware/joiSanitizer");

const esgIndicatorSchema = Joi.object({
  code: Joi.string()
    .pattern(/^[a-zA-Z0-9_]+$/)
    .max(50)
    .sanitize()
    .required(),
  name: Joi.string().max(255).sanitize().required(),
  description: Joi.string().allow(null, "").sanitize(),
  category: Joi.string()
    .valid("environment", "social", "governance")
    .required(),
  clientId: Joi.string().length(10).required(),
});

const esgMetricSchema = Joi.object({
  indicatorId: Joi.string().length(10).required(),
  reportingPeriodId: Joi.string().length(10).required(),
  value: Joi.number().required(),
  unit: Joi.string().max(50).sanitize().required(),
  clientId: Joi.string().length(10).required(),
});

module.exports = {
  esgIndicatorSchema,
  esgMetricSchema,
};

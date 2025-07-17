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
  reportingPeriodId: Joi.string().length(10).required(),
  clientId: Joi.string().length(10),
  isTemplate: Joi.boolean(),
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),
});

const esgMetricSchema = Joi.object({
  indicatorId: Joi.string().length(10).required(),
  reportingPeriodId: Joi.string().length(10).required(),
  value: Joi.number().required(),
  unitId: Joi.string().length(10).required(),
  clientId: Joi.string().length(10),
  isTemplate: Joi.boolean(),
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),
});

const esgReportingPeriodSchema = Joi.object({
  name: Joi.string().max(255).sanitize().required(),
  startDate: Joi.string()
    .pattern(/^\d{4}-\d{2}-\d{2}$/)
    .required()
    .label("Start Date")
    .messages({
      "string.pattern.base": "Start Date must be in YYYY-MM-DD format",
    }),
  endDate: Joi.string()
    .pattern(/^\d{4}-\d{2}-\d{2}$/)
    .required()
    .label("End Date")
    .messages({
      "string.pattern.base": "End Date must be in YYYY-MM-DD format",
    }),
  clientId: Joi.string().length(10), // still optional, injected by server
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),
});

const esgUnitSchema = Joi.object({
  name: Joi.string().max(100).sanitize().required(),
  symbol: Joi.string().max(20).sanitize().required(),
  description: Joi.string().allow(null, "").sanitize(),
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),
});

const esgTemplateSchema = Joi.object({
  fieldType: Joi.string().valid("indicator", "metric").required(),
  fieldName: Joi.string().max(255).sanitize().required(),
  description: Joi.string().allow(null, "").sanitize(),
  category: Joi.string()
    .valid("environment", "social", "governance")
    .allow(null),
  defaultUnit: Joi.string().max(50).allow(null),
  clientId: Joi.string().length(10).allow(null), // null for global templates
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),
});

module.exports = {
  esgIndicatorSchema,
  esgMetricSchema,
  esgReportingPeriodSchema,
  esgUnitSchema,
  esgTemplateSchema,
};

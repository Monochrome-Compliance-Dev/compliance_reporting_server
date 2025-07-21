const Joi = require("../middleware/joiSanitizer");

const msSupplierRiskSchema = Joi.object({
  name: Joi.string().max(255).sanitize().required(),
  country: Joi.string().max(100).sanitize().required(),
  risk: Joi.string().valid("Low", "Medium", "High").required(),
  reviewed: Joi.string()
    .pattern(/^\d{4}-\d{2}-\d{2}$/)
    .label("Reviewed Date")
    .messages({
      "string.pattern.base": "Reviewed must be in YYYY-MM-DD format",
    }),
  clientId: Joi.string().length(10),
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),
});

const msTrainingSchema = Joi.object({
  employeeName: Joi.string().max(255).sanitize().required(),
  department: Joi.string().max(100).sanitize(),
  completed: Joi.boolean().required(),
  completedAt: Joi.string()
    .pattern(/^\d{4}-\d{2}-\d{2}$/)
    .label("Completed At")
    .messages({
      "string.pattern.base": "Completed At must be in YYYY-MM-DD format",
    }),
  clientId: Joi.string().length(10),
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),
});

const msGrievanceSchema = Joi.object({
  description: Joi.string().sanitize().required(),
  status: Joi.string().valid("Open", "Closed", "Investigating").required(),
  reportedAt: Joi.string()
    .pattern(/^\d{4}-\d{2}-\d{2}$/)
    .label("Reported At")
    .messages({
      "string.pattern.base": "Reported At must be in YYYY-MM-DD format",
    }),
  clientId: Joi.string().length(10),
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),
});

module.exports = {
  msSupplierRiskSchema,
  msTrainingSchema,
  msGrievanceSchema,
};

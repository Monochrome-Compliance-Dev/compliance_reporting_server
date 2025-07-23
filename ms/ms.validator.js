const Joi = require("../middleware/joiSanitizer");

const msSupplierRiskSchema = Joi.object({
  name: Joi.string().max(255).sanitize().required(),
  country: Joi.string().max(100).sanitize().required(),
  risk: Joi.string().valid("Low", "Medium", "High").required(),
  reviewed: Joi.string().optional().allow("", null),
  clientId: Joi.string().length(10),
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),
});

const msTrainingSchema = Joi.object({
  employeeName: Joi.string().max(255).sanitize().required(),
  department: Joi.string().max(100).sanitize(),
  completed: Joi.boolean(),
  completedAt: Joi.string().optional().allow("", null),
  clientId: Joi.string().length(10),
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),
});

const msGrievanceSchema = Joi.object({
  description: Joi.string().sanitize().required(),
  status: Joi.string().valid("Open", "Closed", "Investigating").required(),
  reportedAt: Joi.string().optional().allow("", null),
  clientId: Joi.string().length(10),
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),
});

module.exports = {
  msSupplierRiskSchema,
  msTrainingSchema,
  msGrievanceSchema,
};

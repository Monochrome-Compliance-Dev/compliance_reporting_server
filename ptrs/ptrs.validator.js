const Joi = require("../middleware/joiSanitizer");

const ptrsSchema = Joi.object({
  runName: Joi.string().max(100).required(),
  periodKey: Joi.string()
    .pattern(/^\d{4}-(01|07)$/)
    .required(),
  reportingPeriodStartDate: Joi.string()
    .pattern(/^\d{4}-\d{2}-\d{2}$/)
    .required(),
  reportingPeriodEndDate: Joi.string()
    .pattern(/^\d{4}-\d{2}-\d{2}$/)
    .required(),
  currentStep: Joi.number().integer().min(0).max(100).required(),
  status: Joi.string()
    .valid(
      "Created",
      "Cancelled",
      "In Progress",
      "Updated",
      "Received",
      "Accepted",
      "Rejected",
      "Submitted",
      "Deleted",
      "Validated"
    )
    .sanitize()
    .required(),
  createdBy: Joi.string().length(10).required(),
  id: Joi.string().max(10), // Optional for create; required for update
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  customerId: Joi.string().length(10).required(),
});

module.exports = {
  ptrsSchema,
};

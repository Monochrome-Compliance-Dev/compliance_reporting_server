const Joi = require("../middleware/joiSanitizer");

const reportSchema = Joi.object({
  ReportingPeriodStartDate: Joi.date().required(),
  ReportingPeriodEndDate: Joi.date().required(),
  code: Joi.string().alphanum().max(50).sanitize().required(),
  reportName: Joi.string().max(255).sanitize().required(),
  currentStep: Joi.number().integer().min(0).max(100).required(),
  reportStatus: Joi.string()
    .valid(
      "Created",
      "Cancelled",
      "In Progress",
      "Updated",
      "Received",
      "Accepted",
      "Rejected",
      "Submitted"
    )
    .sanitize()
    .required(),
  createdBy: Joi.string().alphanum().length(10).required(),
  id: Joi.string().alphanum().max(10), // Optional for create; required for update
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
});

module.exports = { reportSchema };

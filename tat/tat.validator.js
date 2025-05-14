const Joi = require("joi");
const validator = require("validator");

const sanitizeString = (value, helpers) => {
  return validator.escape(value);
};

const tatSchema = Joi.object({
  mostCommonPaymentTerm: Joi.number().integer().required(),
  receivableTermComparison: Joi.string()
    .valid("Shorter", "Same", "Longer")
    .required()
    .custom(sanitizeString, "Sanitize string"),
  rangeMinCurrent: Joi.number().integer().required(),
  rangeMaxCurrent: Joi.number().integer().required(),
  expectedMostCommonPaymentTerm: Joi.number().integer().required(),
  expectedRangeMin: Joi.number().integer().required(),
  expectedRangeMax: Joi.number().integer().required(),
  averagePaymentTime: Joi.number().required(),
  medianPaymentTime: Joi.number().required(),
  percentile80: Joi.number().integer().required(),
  percentile95: Joi.number().integer().required(),
  paidWithinTermsPercent: Joi.number().required(),
  paidWithin30DaysPercent: Joi.number().required(),
  paid31To60DaysPercent: Joi.number().required(),
  paidOver60DaysPercent: Joi.number().required(),
  createdBy: Joi.string()
    .length(10)
    .optional()
    .custom(sanitizeString, "Sanitize string"),
  updatedBy: Joi.string()
    .length(10)
    .optional()
    .custom(sanitizeString, "Sanitize string"),
});

module.exports = { tatSchema };

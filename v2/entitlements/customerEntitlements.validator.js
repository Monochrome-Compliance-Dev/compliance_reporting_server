const Joi = require("../../middleware/joiSanitizer");
const { SUPPORTED_FEATURE_KEYS } = require("./customerEntitlements.service");

// Single feature toggle entry
const featureToggleSchema = Joi.object({
  feature: Joi.string()
    .valid(...SUPPORTED_FEATURE_KEYS)
    .required()
    .sanitize(),
  enabled: Joi.boolean().required(),
});

// PUT /api/v2/customers/:customerId/entitlements
// Body: { features: [{ feature, enabled }, ...] }
const entitlementsUpdateSchema = Joi.object({
  features: Joi.array().items(featureToggleSchema).min(1).required(),
});

module.exports = {
  featureToggleSchema,
  entitlementsUpdateSchema,
};

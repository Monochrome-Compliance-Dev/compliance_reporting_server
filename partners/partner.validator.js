const Joi = require("../middleware/joiSanitizer");

const partnerSchema = Joi.object({
  name: Joi.string().max(255).sanitize().required(),
  contactName: Joi.string().max(255).sanitize().optional().allow(null, ""),
  contactEmail: Joi.string().email().sanitize().optional().allow(null, ""),
  discountRate: Joi.number().precision(2).min(0).max(100).optional(),
  createdBy: Joi.string().length(10).required(),
  updatedBy: Joi.string().length(10).optional().allow(null, ""),
});

module.exports = {
  partnerSchema,
};

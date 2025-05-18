const Joi = require("../middleware/joiSanitizer");

const entitySchema = Joi.object({
  entityName: Joi.string().required(),
  entityABN: Joi.string().allow(null, ""),
  startEntity: Joi.string().allow(null, ""),
  section7: Joi.string().allow(null, ""),
  cce: Joi.string().allow(null, ""),
  charity: Joi.string().allow(null, ""),
  connectionToAustralia: Joi.string().allow(null, ""),
  revenue: Joi.string().allow(null, ""),
  controlled: Joi.string().allow(null, ""),
  stoppedReason: Joi.string().allow(null, ""),
  completed: Joi.boolean().default(false),
  createdBy: Joi.allow(null, ""),
});

module.exports = { entitySchema };

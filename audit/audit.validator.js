const Joi = require("joi");

const auditSchema = Joi.object({
  fieldName: Joi.string().max(50).required(),
  oldValue: Joi.string().max(50).allow(null),
  newValue: Joi.string().max(50).allow(null),
  action: Joi.string().valid("create", "update", "delete").required(),
  step: Joi.string().max(20).allow(null),
  createdBy: Joi.string().length(10).required(),
  tcpId: Joi.string().length(10).required(),
  clientId: Joi.string().length(10).required(),
}).options({ abortEarly: false });

module.exports = { auditSchema };

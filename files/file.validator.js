const Joi = require("../middleware/joiSanitizer");

const fileCreateSchema = Joi.object({
  indicatorId: Joi.string().length(10).allow(null),
  metricId: Joi.string().length(10).allow(null),
  filename: Joi.string().max(255).sanitize().required(),
  storagePath: Joi.string().max(500).sanitize().required(),
  mimeType: Joi.string().max(100).sanitize().required(),
  fileSize: Joi.number().integer().required(),
  clientId: Joi.string().length(10),
});

const fileQuerySchema = Joi.object({
  indicatorId: Joi.string().length(10),
  metricId: Joi.string().length(10),
  clientId: Joi.string().length(10),
});

module.exports = {
  fileCreateSchema,
  fileQuerySchema,
};

const Joi = require("../middleware/joiSanitizer");

const saveBlogSchema = Joi.object({
  title: Joi.string().required(),
  slug: Joi.string().required(),
  content: Joi.string().required(),
});

const saveFaqSchema = Joi.object({
  content: Joi.string().required(),
});

module.exports = {
  saveBlogSchema,
  saveFaqSchema,
};

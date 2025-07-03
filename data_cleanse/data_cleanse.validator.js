const Joi = require("joi");
const validator = require("validator");

const sanitize = (value) => {
  if (typeof value === "string") {
    return validator.escape(value.trim());
  }
  return value;
};

const dcSchema = Joi.alternatives().try(
  Joi.object({
    name: Joi.string()
      .trim()
      .min(1)
      .max(100)
      .required()
      .custom((value, helpers) => {
        const sanitizedValue = sanitize(value);
        if (sanitizedValue !== value) {
          return helpers.error("any.invalid");
        }
        return sanitizedValue;
      }),
  }),
  Joi.array().items(
    Joi.object({
      name: Joi.string()
        .trim()
        .min(1)
        .max(100)
        .required()
        .custom((value, helpers) => {
          const sanitizedValue = sanitize(value);
          if (sanitizedValue !== value) {
            return helpers.error("any.invalid");
          }
          return sanitizedValue;
        }),
    })
  )
);

function validateAbnLookupSchema() {
  return dcSchema;
}

module.exports = { validateAbnLookupSchema };

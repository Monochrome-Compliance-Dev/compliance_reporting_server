const Joi = require("../middleware/joiSanitizer");

const bookingSchema = Joi.object({
  name: Joi.string().required(),
  email: Joi.string().email({ minDomainSegments: 2 }).required(),
  date: Joi.date().iso().required(),
  time: Joi.string().required(),
  reason: Joi.string().allow("", null),
  status: Joi.string()
    .valid("pending", "confirmed", "cancelled")
    .default("pending"),
});

const bookingUpdateSchema = Joi.object({
  name: Joi.string(),
  email: Joi.string().email({ minDomainSegments: 2 }),
  date: Joi.date().iso(),
  time: Joi.string(),
  reason: Joi.string().allow("", null),
  status: Joi.string().valid("pending", "confirmed", "cancelled"),
});

module.exports = { bookingSchema, bookingUpdateSchema };

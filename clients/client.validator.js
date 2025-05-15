const Joi = require("../middleware/joiSanitizer");
const { update } = require("./client.service");

const clientSchema = Joi.object({
  businessName: Joi.string().required(),
  abn: Joi.string().required(),
  acn: Joi.string().required(),
  addressline1: Joi.string().required(),
  city: Joi.string().required(),
  state: Joi.string().required(),
  postcode: Joi.string().required(),
  country: Joi.string().required(),
  postaladdressline1: Joi.string().required(),
  postalcity: Joi.string().required(),
  postalstate: Joi.string().required(),
  postalpostcode: Joi.string().required(),
  postalcountry: Joi.string().required(),
  industryCode: Joi.string().required(),
  contactFirst: Joi.string().required(),
  contactLast: Joi.string().required(),
  contactPosition: Joi.string().required(),
  contactEmail: Joi.string().email({ minDomainSegments: 2 }).required(),
  contactPhone: Joi.string()
    .pattern(/^[0-9\s\-\(\)]+$/)
    .message("Contact phone must be a valid phone number")
    .required(),
  active: Joi.boolean().default(true),
  createdBy: Joi.string()
    .alphanum()
    .length(10)
    .message("Created by must be a string of length 10")
    .required(),
});

const clientUpdateSchema = Joi.object({
  businessName: Joi.string(),
  abn: Joi.string(),
  acn: Joi.string(),
  addressline1: Joi.string(),
  addressline2: Joi.string(),
  addressline3: Joi.string(),
  city: Joi.string(),
  state: Joi.string(),
  postcode: Joi.string(),
  country: Joi.string(),
  postaladdressline1: Joi.string(),
  postaladdressline2: Joi.string(),
  postaladdressline3: Joi.string(),
  postalcity: Joi.string(),
  postalstate: Joi.string(),
  postalpostcode: Joi.string(),
  postalcountry: Joi.string(),
  industryCode: Joi.string(),
  contactFirst: Joi.string(),
  contactLast: Joi.string(),
  contactPosition: Joi.string(),
  contactEmail: Joi.string().email({ minDomainSegments: 2 }),
  contactPhone: Joi.string()
    .pattern(/^[0-9\s\-\(\)]+$/)
    .message("Contact phone must be a valid phone number"),
  active: Joi.boolean().default(true),
  updatedBy: Joi.string()
    .alphanum()
    .length(10)
    .message("Updated by must be a string of length 10")
    .required(),
});

module.exports = { clientSchema, clientUpdateSchema };

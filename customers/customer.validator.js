const Joi = require("../middleware/joiSanitizer");

const customerSchema = Joi.object({
  businessName: Joi.string().required(),
  abn: Joi.string().required(),
  acn: Joi.string().optional().allow(null, ""),
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
  controllingCorporationName: Joi.string().allow(null, ""),
  controllingCorporationAbn: Joi.string().allow(null, ""),
  controllingCorporationAcn: Joi.string().allow(null, ""),
  headEntityName: Joi.string().allow(null, ""),
  headEntityAbn: Joi.string().allow(null, ""),
  headEntityAcn: Joi.string().allow(null, ""),
  active: Joi.boolean().default(true),
  seats: Joi.number().integer().min(1).default(1),
  createdBy: Joi.string()
    .alphanum()
    .length(10)
    .message("Created by must be a string of length 10")
    .required(),
  paymentConfirmed: Joi.boolean().default(false),
  billingType: Joi.string().valid("CUSTOMER", "DIRECT", "PARTNER").required(),
  partnerId: Joi.string().alphanum().length(10).allow(null, ""),
});

const customerUpdateSchema = Joi.object({
  businessName: Joi.string(),
  abn: Joi.string(),
  acn: Joi.string().allow(null, ""),
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
  controllingCorporationName: Joi.string().allow(null, ""),
  controllingCorporationAbn: Joi.string().allow(null, ""),
  controllingCorporationAcn: Joi.string().allow(null, ""),
  headEntityName: Joi.string().allow(null, ""),
  headEntityAbn: Joi.string().allow(null, ""),
  headEntityAcn: Joi.string().allow(null, ""),
  active: Joi.boolean().default(true),
  seats: Joi.number().integer().min(1),
  updatedBy: Joi.string()
    .alphanum()
    .length(10)
    .message("Updated by must be a string of length 10")
    .required(),
  paymentConfirmed: Joi.boolean(),
  billingType: Joi.string().valid("DIRECT", "PARTNER").required(),
  partnerId: Joi.string().alphanum().length(10).allow(null, ""),
});

const featureEntitlementSchema = Joi.object({
  customerId: Joi.string().required(),
  feature: Joi.string().required(),
  status: Joi.string()
    .valid("active", "trial", "grace", "expired")
    .default("active"),
  source: Joi.string().valid("stripe", "manual", "promo").default("stripe"),
  validFrom: Joi.date().required(),
  validTo: Joi.date().optional(),
  createdBy: Joi.string().required(),
  updatedBy: Joi.string().optional(),
});

module.exports = {
  customerSchema,
  customerUpdateSchema,
  featureEntitlementSchema,
};

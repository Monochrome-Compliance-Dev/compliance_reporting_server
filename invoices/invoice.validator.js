const Joi = require("../middleware/joiSanitizer");

const invoiceSchema = Joi.object({
  billingType: Joi.string().valid("DIRECT", "PARTNER").required(),
  customerId: Joi.string().alphanum().length(10).allow(null, ""),
  partnerId: Joi.string().alphanum().length(10).allow(null, ""),
  reportingPeriodId: Joi.string().alphanum().length(10).required(),
  issuedAt: Joi.date().optional(),
  totalAmount: Joi.number().precision(2).required(),
  status: Joi.string()
    .valid("draft", "issued", "paid", "cancelled")
    .default("draft"),
  createdBy: Joi.string().alphanum().length(10).required(),
});

const invoiceUpdateSchema = Joi.object({
  billingType: Joi.string().valid("DIRECT", "PARTNER"),
  customerId: Joi.string().alphanum().length(10).allow(null, ""),
  partnerId: Joi.string().alphanum().length(10).allow(null, ""),
  reportingPeriodId: Joi.string().alphanum().length(10),
  issuedAt: Joi.date().optional(),
  totalAmount: Joi.number().precision(2),
  status: Joi.string().valid("draft", "issued", "paid", "cancelled"),
  updatedBy: Joi.string().alphanum().length(10).required(),
});

const generateInvoiceSchema = Joi.object({
  reportingPeriodId: Joi.string().required(),
});

module.exports = { invoiceSchema, invoiceUpdateSchema, generateInvoiceSchema };

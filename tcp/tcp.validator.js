const Joi = require("joi");
const validator = require("validator");

const sanitize = (value) => {
  if (typeof value === "string") {
    return validator.escape(value.trim());
  }
  return value;
};

const tcpSchema = Joi.object({
  payerEntityName: Joi.string().custom(sanitize).required(),
  payerEntityAbn: Joi.number().integer().optional(),
  payerEntityAcnArbn: Joi.number().integer().optional(),
  payeeEntityName: Joi.string().custom(sanitize).required(),
  payeeEntityAbn: Joi.number().integer().optional(),
  payeeEntityAcnArbn: Joi.number().integer().optional(),
  paymentAmount: Joi.number().precision(2).required(),
  description: Joi.string().custom(sanitize).allow("", null),
  supplyDate: Joi.date().optional(),
  paymentDate: Joi.date().required(),
  contractPoReferenceNumber: Joi.string().custom(sanitize).allow("", null),
  contractPoPaymentTerms: Joi.string().custom(sanitize).allow("", null),
  noticeForPaymentIssueDate: Joi.date().optional(),
  noticeForPaymentTerms: Joi.string().custom(sanitize).allow("", null),
  invoiceReferenceNumber: Joi.string().custom(sanitize).allow("", null),
  invoiceIssueDate: Joi.date().optional(),
  invoiceReceiptDate: Joi.date().optional(),
  invoiceAmount: Joi.number().precision(2).optional(),
  invoicePaymentTerms: Joi.string().custom(sanitize).allow("", null),
  invoiceDueDate: Joi.date().optional(),
  isTcp: Joi.boolean().required(),
  tcpExclusionComment: Joi.string().custom(sanitize).allow("", null),
  peppolEnabled: Joi.boolean().required(),
  rcti: Joi.boolean().required(),
  creditCardPayment: Joi.boolean().required(),
  creditCardNumber: Joi.string().custom(sanitize).allow("", null),
  partialPayment: Joi.boolean().required(),
  paymentTerm: Joi.number().integer().allow(null),
  excludedTcp: Joi.boolean().required(),
  explanatoryComments1: Joi.string().custom(sanitize).allow("", null),
  isSb: Joi.boolean().optional(),
  paymentTime: Joi.number().integer().optional(),
  explanatoryComments2: Joi.string().custom(sanitize).allow("", null),
  createdBy: Joi.string().custom(sanitize).length(10).optional(),
  updatedBy: Joi.string().custom(sanitize).length(10).optional(),
});

module.exports = { tcpSchema };

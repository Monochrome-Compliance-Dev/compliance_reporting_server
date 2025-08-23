const Joi = require("joi");
const validator = require("validator");

const sanitize = (value) => {
  if (typeof value === "string") {
    return validator.escape(value.trim());
  }
  return value;
};

// Common validators for AU identifiers
const ABN = Joi.string()
  .pattern(/^\d{11}$/)
  .allow(null)
  .empty("")
  .default(null); // 11 digits
const ACN_ARBN = Joi.string()
  .pattern(/^\d{9}$/)
  .allow(null)
  .empty("")
  .default(null); // 9 digits

const tcpSchema = Joi.array().items(
  Joi.object({
    payerEntityName: Joi.string().custom(sanitize).required(),
    payerEntityAbn: ABN,
    payerEntityAcnArbn: ACN_ARBN,
    payeeEntityName: Joi.string().custom(sanitize).required(),
    payeeEntityAbn: ABN,
    payeeEntityAcnArbn: ACN_ARBN,
    paymentAmount: Joi.number().precision(2).required(),
    description: Joi.string().custom(sanitize).allow("", null),
    transactionType: Joi.string().custom(sanitize).optional().allow("", null),
    isReconciled: Joi.boolean().optional().default(false),
    supplyDate: Joi.date().optional().allow(null),
    paymentDate: Joi.date().required(),
    contractPoReferenceNumber: Joi.string().custom(sanitize).allow("", null),
    contractPoPaymentTerms: Joi.string().custom(sanitize).allow("", null),
    noticeForPaymentIssueDate: Joi.date().optional().allow(null),
    noticeForPaymentTerms: Joi.string().custom(sanitize).allow("", null),
    invoiceReferenceNumber: Joi.string().custom(sanitize).allow("", null),
    invoiceIssueDate: Joi.date().optional().allow(null),
    invoiceReceiptDate: Joi.date().optional().allow(null),
    invoiceAmount: Joi.number()
      .precision(2)
      .optional()
      .allow(null)
      .empty("")
      .default(null),
    invoicePaymentTerms: Joi.string().custom(sanitize).allow("", null),
    invoiceDueDate: Joi.date().optional().allow(null),
    isTcp: Joi.boolean().required(),
    tcpExclusionComment: Joi.string().custom(sanitize).allow("", null),
    peppolEnabled: Joi.boolean().required(),
    rcti: Joi.boolean().required(),
    creditCardPayment: Joi.boolean().required(),
    creditCardNumber: Joi.string().custom(sanitize).allow("", null),
    partialPayment: Joi.boolean().required(),
    paymentTerm: Joi.number().integer().allow(null).empty("").default(null),
    excludedTcp: Joi.boolean().required(),
    explanatoryComments1: Joi.string().custom(sanitize).allow("", null),
    accountCode: Joi.string().custom(sanitize).allow("", null),
    isSb: Joi.boolean().optional(),
    paymentTime: Joi.number()
      .integer()
      .optional()
      .allow(null)
      .empty("")
      .default(null),
    explanatoryComments2: Joi.string().custom(sanitize).allow("", null),
    source: Joi.string().required(),
    createdBy: Joi.string().custom(sanitize).length(10).required(),
    updatedBy: Joi.string().custom(sanitize).length(10).optional(),
    ptrsId: Joi.string().required(),
    customerId: Joi.string().required(),
  })
);

const tcpBulkImportSchema = Joi.object({
  payerEntityName: Joi.string().custom(sanitize).required(),
  payerEntityAbn: ABN,
  payerEntityAcnArbn: ACN_ARBN,
  payeeEntityName: Joi.string().custom(sanitize).required(),
  payeeEntityAbn: ABN,
  payeeEntityAcnArbn: ACN_ARBN,
  paymentAmount: Joi.number().precision(2).required(),
  description: Joi.string().custom(sanitize).allow("", null),
  transactionType: Joi.string().custom(sanitize).optional().allow("", null),
  isReconciled: Joi.boolean().optional().default(false),
  supplyDate: Joi.date().optional().allow(null),
  paymentDate: Joi.date().required(),
  contractPoReferenceNumber: Joi.string().custom(sanitize).allow("", null),
  contractPoPaymentTerms: Joi.string().custom(sanitize).allow("", null),
  noticeForPaymentIssueDate: Joi.date().optional().allow(null),
  noticeForPaymentTerms: Joi.string().custom(sanitize).allow("", null),
  invoiceReferenceNumber: Joi.string().custom(sanitize).allow("", null),
  invoiceIssueDate: Joi.date().optional().allow(null),
  invoiceReceiptDate: Joi.date().optional().allow(null),
  invoiceAmount: Joi.number()
    .precision(2)
    .optional()
    .allow(null)
    .empty("")
    .default(null),
  invoicePaymentTerms: Joi.string().custom(sanitize).allow("", null),
  invoiceDueDate: Joi.date().optional().allow(null),
  accountCode: Joi.string().custom(sanitize).allow("", null),
  isTcp: Joi.boolean().optional(),
  tcpExclusionComment: Joi.string().custom(sanitize).allow("", null),
  peppolEnabled: Joi.boolean().optional(),
  rcti: Joi.boolean().optional(),
  creditCardPayment: Joi.boolean().optional(),
  creditCardNumber: Joi.string().custom(sanitize).allow("", null),
  partialPayment: Joi.boolean().optional(),
  paymentTerm: Joi.number().integer().allow(null).empty("").default(null),
  excludedTcp: Joi.boolean().optional(),
  explanatoryComments1: Joi.string().custom(sanitize).allow("", null),
  isSb: Joi.boolean().optional(),
  paymentTime: Joi.number()
    .integer()
    .optional()
    .allow(null)
    .empty("")
    .default(null),
  explanatoryComments2: Joi.string().custom(sanitize).allow("", null),
  source: Joi.string().required(),
  createdBy: Joi.string().custom(sanitize).length(10).required(),
  updatedBy: Joi.string().custom(sanitize).length(10).optional(),
  ptrsId: Joi.string().required(),
  customerId: Joi.string().required(),
});

module.exports = { tcpSchema, tcpBulkImportSchema };

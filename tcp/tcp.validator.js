const Joi = require("../middleware/joiSanitizer");

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
    payerEntityName: Joi.string().sanitize().required(),
    payerEntityAbn: ABN,
    payerEntityAcnArbn: ACN_ARBN,
    payeeEntityName: Joi.string().sanitize().required(),
    payeeEntityAbn: ABN,
    payeeEntityAcnArbn: ACN_ARBN,
    paymentAmount: Joi.number().precision(2).required(),
    description: Joi.string().sanitize().allow("", null),
    transactionType: Joi.string().sanitize().optional().allow("", null),
    isReconciled: Joi.boolean().optional().default(false),
    supplyDate: Joi.date().optional().allow(null),
    paymentDate: Joi.date().required(),
    contractPoReferenceNumber: Joi.string().sanitize().allow("", null),
    contractPoPaymentTerms: Joi.string().sanitize().allow("", null),
    noticeForPaymentIssueDate: Joi.date().optional().allow(null),
    noticeForPaymentTerms: Joi.string().sanitize().allow("", null),
    invoiceReferenceNumber: Joi.string().sanitize().allow("", null),
    invoiceIssueDate: Joi.date().optional().allow(null),
    invoiceReceiptDate: Joi.date().optional().allow(null),
    invoiceAmount: Joi.number()
      .precision(2)
      .optional()
      .allow(null)
      .empty("")
      .default(null),
    invoicePaymentTerms: Joi.string().sanitize().allow("", null),
    invoiceDueDate: Joi.date().optional().allow(null),
    isTcp: Joi.boolean().required(),
    tcpExclusionComment: Joi.string().sanitize().allow("", null),
    peppolEnabled: Joi.boolean().optional(),
    rcti: Joi.boolean().required(),
    creditCardPayment: Joi.boolean().required(),
    creditCardNumber: Joi.string().sanitize().allow("", null),
    partialPayment: Joi.boolean().required(),
    paymentTerm: Joi.number().integer().allow(null).empty("").default(null),
    excludedTcp: Joi.boolean().required(),
    explanatoryComments1: Joi.string().sanitize().allow("", null),
    accountCode: Joi.string().sanitize().allow("", null),
    isSb: Joi.boolean().optional(),
    paymentTime: Joi.number()
      .integer()
      .optional()
      .allow(null)
      .empty("")
      .default(null),
    explanatoryComments2: Joi.string().sanitize().allow("", null),
    source: Joi.string().required(),
    createdBy: Joi.string().sanitize().length(10).required(),
    updatedBy: Joi.string().sanitize().length(10).optional(),
    ptrsId: Joi.string().required(),
    customerId: Joi.string().required(),
  })
);

const tcpBulkImportSchema = Joi.object({
  payerEntityName: Joi.string().sanitize().required(),
  payerEntityAbn: ABN,
  payerEntityAcnArbn: ACN_ARBN,
  payeeEntityName: Joi.string().sanitize().required(),
  payeeEntityAbn: ABN,
  payeeEntityAcnArbn: ACN_ARBN,
  paymentAmount: Joi.number().precision(2).required(),
  description: Joi.string().sanitize().allow("", null),
  transactionType: Joi.string().sanitize().optional().allow("", null),
  isReconciled: Joi.boolean().optional().default(false),
  supplyDate: Joi.date().optional().allow(null),
  paymentDate: Joi.date().required(),
  contractPoReferenceNumber: Joi.string().sanitize().allow("", null),
  contractPoPaymentTerms: Joi.string().sanitize().allow("", null),
  noticeForPaymentIssueDate: Joi.date().optional().allow(null),
  noticeForPaymentTerms: Joi.string().sanitize().allow("", null),
  invoiceReferenceNumber: Joi.string().sanitize().allow("", null),
  invoiceIssueDate: Joi.date().optional().allow(null),
  invoiceReceiptDate: Joi.date().optional().allow(null),
  invoiceAmount: Joi.number()
    .precision(2)
    .optional()
    .allow(null)
    .empty("")
    .default(null),
  invoicePaymentTerms: Joi.string().sanitize().allow("", null),
  invoiceDueDate: Joi.date().optional().allow(null),
  accountCode: Joi.string().sanitize().allow("", null),
  isTcp: Joi.boolean().optional(),
  tcpExclusionComment: Joi.string().sanitize().allow("", null),
  peppolEnabled: Joi.boolean().optional(),
  rcti: Joi.boolean().optional(),
  creditCardPayment: Joi.boolean().optional(),
  creditCardNumber: Joi.string().sanitize().allow("", null),
  partialPayment: Joi.boolean().optional(),
  paymentTerm: Joi.number().integer().allow(null).empty("").default(null),
  excludedTcp: Joi.boolean().optional(),
  explanatoryComments1: Joi.string().sanitize().allow("", null),
  isSb: Joi.boolean().optional(),
  paymentTime: Joi.number()
    .integer()
    .optional()
    .allow(null)
    .empty("")
    .default(null),
  explanatoryComments2: Joi.string().sanitize().allow("", null),
  source: Joi.string().required(),
  createdBy: Joi.string().sanitize().length(10).required(),
  updatedBy: Joi.string().sanitize().length(10).optional(),
  ptrsId: Joi.string().required(),
  customerId: Joi.string().required(),
});

// --- Reference Models ---

const govEntityRefSchema = Joi.object({
  id: Joi.string().max(10), // Optional for create; required for update
  abn: Joi.string().max(14).allow(null, "").sanitize(),
  name: Joi.string().max(255).sanitize().required(),
  category: Joi.string().max(64).allow(null, "").sanitize(),
  createdBy: Joi.string().length(10).required(),
  updatedBy: Joi.string().length(10).optional(),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  deletedAt: Joi.date().optional(),
});

const employeeRefSchema = Joi.object({
  id: Joi.string().max(10), // Optional for create; required for update
  customerId: Joi.string().length(12).required(),
  name: Joi.string().max(255).sanitize().required(),
  abn: Joi.string().max(14).allow(null, "").sanitize(),
  accountCode: Joi.string().max(64).allow(null, "").sanitize(),
  notes: Joi.string().allow(null, "").sanitize(),
  createdBy: Joi.string().length(10).required(),
  updatedBy: Joi.string().length(10).optional(),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  deletedAt: Joi.date().optional(),
});

const intraCompanyRefSchema = Joi.object({
  id: Joi.string().max(10), // Optional for create; required for update
  customerId: Joi.string().length(12).required(),
  counterpartyAbn: Joi.string().max(14).allow(null, "").sanitize(),
  counterpartyName: Joi.string().max(255).sanitize().required(),
  accountCodePattern: Joi.string().max(64).allow(null, "").sanitize(),
  notes: Joi.string().allow(null, "").sanitize(),
  createdBy: Joi.string().length(10).required(),
  updatedBy: Joi.string().length(10).optional(),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  deletedAt: Joi.date().optional(),
});

const exclusionKeywordCustomerRefSchema = Joi.object({
  id: Joi.string().max(10), // Optional for create; required for update
  customerId: Joi.string().length(12).required(),
  field: Joi.string().valid("description", "accountCode").required(),
  term: Joi.string().max(255).required().sanitize(),
  matchType: Joi.string().valid("contains", "equals", "regex").required(),
  createdBy: Joi.string().length(10).required(),
  updatedBy: Joi.string().length(10).optional(),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  deletedAt: Joi.date().optional(),
});

module.exports = {
  tcpSchema,
  tcpBulkImportSchema,
  govEntityRefSchema,
  employeeRefSchema,
  intraCompanyRefSchema,
  exclusionKeywordCustomerRefSchema,
};

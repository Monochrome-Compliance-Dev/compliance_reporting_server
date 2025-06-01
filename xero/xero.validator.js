const Joi = require("joi");

const xeroOrganisationSchema = Joi.object({
  clientId: Joi.string().length(10).required(),
  organisationId: Joi.string().optional(),
  organisationName: Joi.string().optional(),
  organisationLegalName: Joi.string().optional(),
  organisationAbn: Joi.string().optional(),
});

const xeroTokenSchema = Joi.object({
  access_token: Joi.string().required(),
  refresh_token: Joi.string().required(),
  expires: Joi.date().optional(),
  created: Joi.date().optional(),
  createdByIp: Joi.string().optional(),
  revoked: Joi.date().optional(),
  revokedByIp: Joi.string().optional(),
  replacedByToken: Joi.string().optional(),
  clientId: Joi.string().length(10).required(),
});

const xeroInvoiceSchema = Joi.object({
  clientId: Joi.string().length(10).required(),
  invoiceReferenceNumber: Joi.string().optional(),
  invoiceIssueDate: Joi.date().optional(),
  invoiceDueDate: Joi.date().optional(),
  invoiceAmount: Joi.number().optional(),
  invoicePaymentTerms: Joi.string().optional(),
  payeeEntityName: Joi.string().optional(),
  payeeEntityAbn: Joi.string().optional(),
  payeeEntityAcnArbn: Joi.string().optional(),
  paymentAmount: Joi.number().optional(),
  paymentDate: Joi.date().optional(),
  description: Joi.string().optional(),
});

const xeroPaymentSchema = Joi.object({
  clientId: Joi.string().length(10).required(),
  paymentReferenceNumber: Joi.string().optional(),
  paymentAmount: Joi.number().optional(),
  paymentDate: Joi.date().optional(),
  paymentMethod: Joi.string().optional(),
  payerEntityName: Joi.string().optional(),
  payerEntityAbn: Joi.string().optional(),
  payerEntityAcnArbn: Joi.string().optional(),
  description: Joi.string().optional(),
});

const xeroContactSchema = Joi.object({
  clientId: Joi.string().length(10).required(),
  contactId: Joi.string().optional(),
  contactName: Joi.string().optional(),
  contactAbn: Joi.string().optional(),
  contactAcnArbn: Joi.string().optional(),
  paymentTerms: Joi.string().optional(),
});

const credentialsSchema = Joi.object({
  clientId: Joi.string().length(10).required(),
  username: Joi.string().required(),
  password: Joi.string().required(),
});

module.exports = {
  xeroTokenSchema,
  xeroInvoiceSchema,
  xeroPaymentSchema,
  xeroContactSchema,
  xeroOrganisationSchema,
  credentialsSchema,
};

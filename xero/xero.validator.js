const Joi = require("joi");
const { report } = require("./xero.controller");

const xeroOrganisationSchema = Joi.object({
  customerId: Joi.string().length(10).required(),
  reportId: Joi.string().required(),
  OrganisationID: Joi.string().optional().allow(null, ""),
  Name: Joi.string().optional().allow(null, ""),
  LegalName: Joi.string().optional().allow(null, ""),
  RegistrationNumber: Joi.string().optional().allow(null, ""),
  TaxNumber: Joi.string().optional().allow(null, ""),
  PaymentTerms: Joi.string().optional().allow(null, ""),
  source: Joi.string().required(),
  createdBy: Joi.string().required(),
});

const xeroTokenSchema = Joi.object({
  access_token: Joi.string().required(),
  refresh_token: Joi.string().required(),
  expires: Joi.date().optional(),
  created: Joi.date().optional(),
  createdByIp: Joi.string().optional().allow(null, ""),
  revoked: Joi.date().optional(),
  revokedByIp: Joi.string().optional().allow(null, ""),
  replacedByToken: Joi.string().optional().allow(null, ""),
  customerId: Joi.string().length(10).required(),
  tenantId: Joi.string().required(),
});

const xeroInvoiceSchema = Joi.object({
  customerId: Joi.string().length(10).required(),
  reportId: Joi.string().required(),
  tenantId: Joi.string().required(),
  InvoiceID: Joi.string().optional().allow(null, ""),
  InvoiceNumber: Joi.string().optional().allow(null, ""),
  Reference: Joi.string().optional().allow(null, ""),
  LineItems: Joi.array()
    .items(
      Joi.object({
        Description: Joi.string().optional().allow(null, ""),
        Quantity: Joi.number().optional(),
        UnitAmount: Joi.number().optional(),
        AccountCode: Joi.string().optional().allow(null, ""),
        TaxType: Joi.string().optional().allow(null, ""),
        TaxAmount: Joi.number().optional(),
      })
    )
    .optional(),
  Type: Joi.string().optional().allow(null, ""),
  Contact: Joi.object({
    ContactID: Joi.string().optional().allow(null, ""),
    Name: Joi.string().optional().allow(null, ""),
    CompanyNumber: Joi.string().optional().allow(null, ""),
    TaxNumber: Joi.string().optional().allow(null, ""),
    PaymentTerms: Joi.string().optional().allow(null, ""),
  }).optional(),
  DateString: Joi.string().optional().allow(null, ""),
  DueDateString: Joi.string().optional().allow(null, ""),
  Payments: Joi.array()
    .items(
      Joi.object({
        PaymentID: Joi.string().optional().allow(null, ""),
        Amount: Joi.number().optional(),
        Date: Joi.date().optional(),
        IsReconciled: Joi.boolean().optional(),
        Status: Joi.string().optional().allow(null, ""),
        Invoice: Joi.object({
          InvoiceID: Joi.string().optional().allow(null, ""),
          InvoiceNumber: Joi.string().optional().allow(null, ""),
        }).optional(),
      })
    )
    .optional(),
  Status: Joi.string().optional().allow(null, ""),
  AmountDue: Joi.number().optional(),
  AmountPaid: Joi.number().optional(),
  AmountCredited: Joi.number().optional(),
  Url: Joi.string().optional(),
  Total: Joi.number().optional(),
  source: Joi.string().required(),
  createdBy: Joi.string().required(),
});

const xeroPaymentSchema = Joi.object({
  customerId: Joi.string().length(10).required(),
  reportId: Joi.string().required(),
  tenantId: Joi.string().required(),
  Reference: Joi.string().optional().allow(null, ""),
  Amount: Joi.number().optional(),
  PaymentID: Joi.string().optional().allow(null, ""),
  Date: Joi.date().optional(),
  IsReconciled: Joi.boolean().optional(),
  Status: Joi.string().optional().allow(null, ""),
  PaymentType: Joi.string().optional().allow(null, ""),
  Invoice: Joi.object().optional().allow(null, ""),
  source: Joi.string().required(),
  createdBy: Joi.string().required(),
});

const xeroContactSchema = Joi.object({
  customerId: Joi.string().length(10).required(),
  reportId: Joi.string().required(),
  ContactID: Joi.string().optional().allow(null, ""),
  Name: Joi.string().optional().allow(null, ""),
  CompanyNumber: Joi.string().optional().allow(null, ""),
  TaxNumber: Joi.string().optional().allow(null, ""),
  PaymentTerms: Joi.string().optional().allow(null, ""),
  source: Joi.string().required(),
  createdBy: Joi.string().required(),
});

const credentialsSchema = Joi.object({
  customerId: Joi.string().length(10).required(),
  username: Joi.string().required(),
  password: Joi.string().required(),
});

const XeroBankTxnSchema = Joi.object({
  customerId: Joi.string().length(10).required(),
  reportId: Joi.string().required(),
  BankTransactionID: Joi.string().optional().allow(null, ""),
  Type: Joi.string().optional().allow(null, ""),
  Contact: Joi.object().optional().allow(null, ""),
  LineItems: Joi.array()
    .items(
      Joi.object({
        Description: Joi.string().optional().allow(null, ""),
        Quantity: Joi.number().optional(),
        UnitAmount: Joi.number().optional(),
        AccountCode: Joi.string().optional().allow(null, ""),
        TaxType: Joi.string().optional().allow(null, ""),
        TaxAmount: Joi.number().optional(),
      })
    )
    .optional(),
  BankAccount: Joi.object().optional().allow(null, ""),
  LineAmountTypes: Joi.string().optional().allow(null, ""),
  SubTotal: Joi.number().optional(),
  TotalTax: Joi.number().optional(),
  Total: Joi.number().optional(),
  CurrencyCode: Joi.string().optional(),
  Reconciled: Joi.boolean().optional(),
  Status: Joi.string().optional(),
  Url: Joi.string().optional(),
  Reference: Joi.string().optional(),
  DateString: Joi.string().optional(),
  tenantId: Joi.string().required(),
  source: Joi.string().required(),
  createdBy: Joi.string().required(),
});

module.exports = {
  xeroTokenSchema,
  xeroInvoiceSchema,
  xeroPaymentSchema,
  xeroContactSchema,
  xeroOrganisationSchema,
  XeroBankTxnSchema,
  credentialsSchema,
};

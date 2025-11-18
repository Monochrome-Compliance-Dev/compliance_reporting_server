const Joi = require("../../middleware/joiSanitizer");

// Base schema reflecting tbl_customer for v2 Boss-managed customers
const base = Joi.object({
  // Server-managed
  id: Joi.string().length(10),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  deletedAt: Joi.date().optional(),

  // Core identity
  businessName: Joi.string().max(255).sanitize(),
  abn: Joi.string().max(32).sanitize(),
  industryCode: Joi.string().max(32).sanitize(),

  // Address
  addressline1: Joi.string().max(255).sanitize(),
  addressline2: Joi.string().max(255).allow(null).optional().sanitize(),
  city: Joi.string().max(120).sanitize(),
  state: Joi.string().max(120).sanitize(),
  postcode: Joi.string().max(16).sanitize(),
  country: Joi.string().max(120).sanitize(),

  // Primary contact
  contactFirst: Joi.string().max(120).sanitize(),
  contactLast: Joi.string().max(120).sanitize(),
  contactPosition: Joi.string().max(120).sanitize(),
  contactEmail: Joi.string().email().max(255).sanitize(),
  contactPhone: Joi.string().max(64).sanitize(),

  // Commercial fields
  seats: Joi.number().integer().min(1).max(500),
  billingType: Joi.string()
    .valid("CUSTOMER", "DIRECT", "PARTNER")
    .optional()
    .sanitize(),
  partnerId: Joi.string().length(10).allow(null).optional(),
  active: Joi.boolean().optional(),
  paymentConfirmed: Joi.boolean().optional(),

  // Audit
  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),
});

// POST /api/v2/customers
// Boss creates a new customer
const customerCreateSchema = base
  .fork(
    [
      "businessName",
      "abn",
      "industryCode",
      "addressline1",
      "city",
      "state",
      "postcode",
      "country",
      "contactFirst",
      "contactLast",
      "contactPosition",
      "contactEmail",
      "contactPhone",
      "createdBy",
    ],
    (schema) => schema.required()
  )
  .fork(
    [
      "id",
      "createdAt",
      "updatedAt",
      "deletedAt",
      "updatedBy", // createdBy is required, updatedBy is server-managed on create
    ],
    (schema) => schema.forbidden()
  );

// PUT /api/v2/customers/:id
// Boss updates an existing customer
const customerUpdateSchema = base
  .fork(["updatedBy"], (schema) => schema.required())
  .fork(
    [
      "id",
      "createdAt",
      "updatedAt",
      "deletedAt",
      "createdBy", // createdBy should not be changed once set
    ],
    (schema) => schema.forbidden()
  );

// PATCH /api/v2/customers/:id
// Partial update; only require updatedBy, allow other known keys plus future fields
const customerPatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

module.exports = {
  customerCreateSchema,
  customerUpdateSchema,
  customerPatchSchema,
};

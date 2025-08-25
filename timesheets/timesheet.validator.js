const Joi = require("../middleware/joiSanitizer");

// ------- Timesheet (header) -------
const baseTimesheet = Joi.object({
  resourceId: Joi.string().length(10).required().sanitize(),
  weekKey: Joi.date().required(), // Monday of the week (DATEONLY)
  status: Joi.string()
    .valid("draft", "submitted", "approved", "cancelled")
    .sanitize()
    .required(),

  submittedAt: Joi.date().optional(),
  submittedBy: Joi.string().length(10).optional(),
  approvedAt: Joi.date().optional(),
  approvedBy: Joi.string().length(10).optional(),

  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),

  id: Joi.string().max(10),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  customerId: Joi.string().length(10).required(),
});

const timesheetCreateSchema = baseTimesheet
  .fork(["createdBy"], (s) => s.required())
  .fork(["updatedBy", "id", "createdAt", "updatedAt"], (s) => s.forbidden());

const timesheetUpdateSchema = baseTimesheet
  .fork(["updatedBy"], (s) => s.required())
  .fork(["id", "createdAt", "updatedAt"], (s) => s.forbidden());

const timesheetPatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

// ------- Timesheet rows (lines) -------
const baseRow = Joi.object({
  timesheetId: Joi.string().length(10).required().sanitize(),
  date: Joi.date().required(),
  engagementId: Joi.string().length(10).optional().sanitize(),
  budgetItemId: Joi.string().length(10).optional().sanitize(),
  hours: Joi.number().min(0).required(),
  billable: Joi.boolean().optional(),
  notes: Joi.string().max(2000).optional().sanitize(),

  createdBy: Joi.string().length(10),
  updatedBy: Joi.string().length(10),

  id: Joi.string().max(10),
  createdAt: Joi.date().optional(),
  updatedAt: Joi.date().optional(),
  customerId: Joi.string().length(10).required(),
});

// POST /:id/rows — timesheetId comes from the URL param; make body.timesheetId optional
const timesheetRowCreateSchema = baseRow
  .fork(["timesheetId"], (s) => s.optional())
  .fork(["createdBy"], (s) => s.required())
  .fork(["updatedBy", "id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PUT /rows/:rowId — require updatedBy; server-managed fields forbidden
const timesheetRowUpdateSchema = baseRow
  .fork(["timesheetId"], (s) => s.optional())
  .fork(["updatedBy"], (s) => s.required())
  .fork(["id", "createdAt", "updatedAt"], (s) => s.forbidden());

// PATCH /rows/:rowId — partial; require updatedBy
const timesheetRowPatchSchema = Joi.object({
  updatedBy: Joi.string().length(10).required(),
}).unknown(true);

module.exports = {
  timesheetCreateSchema,
  timesheetUpdateSchema,
  timesheetPatchSchema,
  timesheetRowCreateSchema,
  timesheetRowUpdateSchema,
  timesheetRowPatchSchema,
};

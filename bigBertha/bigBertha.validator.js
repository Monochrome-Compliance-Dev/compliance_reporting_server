const Joi = require("../middleware/joiSanitizer");

// POST /api/big-bertha/uploads/local (multipart/form-data)
// We can only validate non-file fields here; file presence is enforced by the middleware
const uploadLocalSchema = Joi.object({
  ptrsId: Joi.string().length(10).required().sanitize(),
});

// POST /api/big-bertha/ingest/start
// Body is JSON created by the FE after a successful /uploads/local
const ingestStartSchema = Joi.object({
  filePath: Joi.string().max(4096).required().sanitize(),
  customerId: Joi.string().length(10).required().sanitize(),
  ptrsId: Joi.string().length(10).required().sanitize(),
  originalName: Joi.string().max(255).optional().allow(null).sanitize(),
  sizeBytes: Joi.number().integer().min(0).optional(),
  format: Joi.string().valid("csv").default("csv"),
});

// GET /api/big-bertha/ingest/:jobId
const jobIdParamSchema = Joi.object({
  jobId: Joi.string().min(6).max(50).required().sanitize(),
});

// GET /api/big-bertha/ptrs/:ptrsId/(rows|errors)
const ptrsIdParamSchema = Joi.object({
  ptrsId: Joi.string().length(10).required().sanitize(),
});

const pageQuerySchema = Joi.object({
  customerId: Joi.string().length(10).required().sanitize(),
  limit: Joi.number().integer().min(1).max(1000).default(100),
  cursor: Joi.string().max(128).optional().allow(null, "").sanitize(),
});

module.exports = {
  uploadLocalSchema,
  ingestStartSchema,
  jobIdParamSchema,
  ptrsIdParamSchema,
  pageQuerySchema,
};

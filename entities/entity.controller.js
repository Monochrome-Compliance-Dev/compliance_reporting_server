const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const entityService = require("./entity.service");
const upload = require("../middleware/upload");
const sendAttachmentEmail = require("../helpers/send-email");

// routes
router.get("/", authorise(), getAll);
router.get("/report/:id", authorise(), getAllByReportId);
router.get("/entity/:id", authorise(), getEntityByReportId);
router.get("/:id", authorise(), getById);
router.post("/", create);
router.post("/send-email", upload.single("attachment"), sendPdfEmail);
router.put("/:id", authorise(), updateSchema, update);
router.delete("/:id", authorise(), _delete);

module.exports = router;

function getAll(req, res, next) {
  entityService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getAllByReportId(req, res, next) {
  entityService
    .getAllByReportId(req.params.id)
    .then((entity) => (entity ? res.json(entity) : res.sendSentityus(404)))
    .catch(next);
}

function getEntityByReportId(req, res, next) {
  entityService
    .getEntityByReportId(req.params.id)
    .then((entity) => (entity ? res.json(entity) : res.sendSentityus(404)))
    .catch(next);
}

function getById(req, res, next) {
  entityService
    .getById(req.params.id)
    .then((entity) => (entity ? res.json(entity) : res.sendSentityus(404)))
    .catch(next);
}

function createSchema(req, res, next) {
  const schema = Joi.object({
    mostCommonPaymentTerm: Joi.string().required(),
    receivableTermComarison: Joi.string().required(),
    rangeMinCurrent: Joi.integer().required(),
    rangeMaxCurrent: Joi.integer().required(),
    expectedMostCommonPaymentTerm: Joi.integer().required(),
    expectedRangeMin: Joi.integer().required(),
    expectedRangeMax: Joi.integer().required(),
    averagePaymentTime: Joi.float().required(),
    medianPaymentTime: Joi.float().required(),
    percentile80: Joi.integer().required(),
    percentile95: Joi.integer().required(),
    paidWithinTermsPercent: Joi.float().required(),
    paidWithin30DaysPercent: Joi.float().required(),
    paid31To60DaysPercent: Joi.float().required(),
    paidOver60DaysPercent: Joi.float().required(),
    reportId: Joi.number().required(),
    createdBy: Joi.number().required(),
  });
  validateRequest(req, next, schema);
}

function create(req, res, next) {
  entityService
    .create(req.body)
    .then((entity) => res.json(entity))
    .catch((error) => {
      console.error("Error creating entity:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function sendPdfEmail(req, res, next) {
  sendAttachmentEmail(req.body)
    .then(() => res.json({ message: "Email sent successfully" }))
    .catch((error) => {
      console.error("Error sending email:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function update(req, res, next) {
  entityService
    .update(req.params.id, req.body)
    .then((entity) => res.json(entity))
    .catch(next);
}

function updateSchema(req, res, next) {
  const schema = Joi.object({
    mostCommonPaymentTerm: Joi.string().allow(null, ""),
    receivableTermComarison: Joi.string().allow(null, ""),
    rangeMinCurrent: Joi.integer().allow(null),
    rangeMaxCurrent: Joi.integer().allow(null),
    expectedMostCommonPaymentTerm: Joi.integer().allow(null),
    expectedRangeMin: Joi.integer().allow(null),
    expectedRangeMax: Joi.integer().allow(null),
    averagePaymentTime: Joi.float().allow(null),
    medianPaymentTime: Joi.float().allow(null),
    percentile80: Joi.integer().allow(null),
    percentile95: Joi.integer().allow(null),
    paidWithinTermsPercent: Joi.float().allow(null),
    paidWithin30DaysPercent: Joi.float().allow(null),
    paid31To60DaysPercent: Joi.float().allow(null),
    paidOver60DaysPercent: Joi.float().allow(null),
    reportId: Joi.number().required(),
    updatedBy: Joi.number().required(),
  });
  validateRequest(req, next, schema);
}

function _delete(req, res, next) {
  entityService
    .delete(req.params.id)
    .then(() => res.json({ message: "Entity deleted successfully" }))
    .catch((error) => {
      console.error("Error deleting entity:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

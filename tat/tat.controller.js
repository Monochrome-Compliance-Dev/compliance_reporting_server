const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const tatService = require("./tat.service");
const setClientContext = require("../middleware/set-client-context");
const { tatSchema } = require("./tat.validator");
const { logger } = require("../helpers/logger");

// routes
router.get("/", authorise(), setClientContext, getAll);
router.get("/report/:id", authorise(), setClientContext, getAllByReportId);
router.get("/tat/:id", authorise(), setClientContext, getTatByReportId);
router.get("/:id", authorise(), setClientContext, getById);
router.post(
  "/",
  authorise(),
  validateRequest(tatSchema),
  setClientContext,
  create
);
router.put(
  "/:id",
  authorise(),
  validateRequest(tatSchema),
  setClientContext,
  update
);
router.delete("/:id", authorise(), setClientContext, _delete);

module.exports = router;

function getAll(req, res, next) {
  tatService
    .getAll(req.auth.clientId)
    .then((entities) => res.json(entities))
    .catch(next);
}

function getAllByReportId(req, res, next) {
  tatService
    .getAllByReportId(req.params.id, req.auth.clientId)
    .then((tat) => (tat ? res.json(tat) : res.sendStatus(404)))
    .catch(next);
}

function getTatByReportId(req, res, next) {
  tatService
    .getTatByReportId(req.params.id, req.auth.clientId)
    .then((tat) => (tat ? res.json(tat) : res.sendStatus(404)))
    .catch(next);
}

function getById(req, res, next) {
  tatService
    .getById(req.params.id, req.auth.clientId)
    .then((tat) => (tat ? res.json(tat) : res.sendStatus(404)))
    .catch(next);
}

// function createSchema(req, res, next) {
//   const schema = Joi.object({
//     mostCommonPaymentTerm: Joi.string().required(),
//     receivableTermComarison: Joi.string().required(),
//     rangeMinCurrent: Joi.integer().required(),
//     rangeMaxCurrent: Joi.integer().required(),
//     expectedMostCommonPaymentTerm: Joi.integer().required(),
//     expectedRangeMin: Joi.integer().required(),
//     expectedRangeMax: Joi.integer().required(),
//     averagePaymentTime: Joi.float().required(),
//     medianPaymentTime: Joi.float().required(),
//     percentile80: Joi.integer().required(),
//     percentile95: Joi.integer().required(),
//     paidWithinTermsPercent: Joi.float().required(),
//     paidWithin30DaysPercent: Joi.float().required(),
//     paid31To60DaysPercent: Joi.float().required(),
//     paidOver60DaysPercent: Joi.float().required(),
//     reportId: Joi.string().required(),
//     createdBy: Joi.string().required(),
//   });
//   validateRequest(req, next, schema);
// }

function create(req, res, next) {
  tatService
    .create(req.body, req.auth.clientId)
    .then((tat) => res.json(tat))
    .catch((error) => {
      logger.logEvent("error", "Error creating TAT record", {
        action: "CreateTAT",
        error: error.message,
        clientId: req.auth.clientId,
      });
      next(error); // Pass the error to the global error handler
    });
}

function update(req, res, next) {
  tatService
    .update(req.params.id, req.body, req.auth.clientId)
    .then((tat) => res.json(tat))
    .catch(next);
}

// function updateSchema(req, res, next) {
//   const schema = Joi.object({
//     mostCommonPaymentTerm: Joi.string().allow(null, ""),
//     receivableTermComarison: Joi.string().allow(null, ""),
//     rangeMinCurrent: Joi.integer().allow(null),
//     rangeMaxCurrent: Joi.integer().allow(null),
//     expectedMostCommonPaymentTerm: Joi.integer().allow(null),
//     expectedRangeMin: Joi.integer().allow(null),
//     expectedRangeMax: Joi.integer().allow(null),
//     averagePaymentTime: Joi.float().allow(null),
//     medianPaymentTime: Joi.float().allow(null),
//     percentile80: Joi.integer().allow(null),
//     percentile95: Joi.integer().allow(null),
//     paidWithinTermsPercent: Joi.float().allow(null),
//     paidWithin30DaysPercent: Joi.float().allow(null),
//     paid31To60DaysPercent: Joi.float().allow(null),
//     paidOver60DaysPercent: Joi.float().allow(null),
//     reportId: Joi.string().required(),
//     updatedBy: Joi.string().required(),
//   });
//   validateRequest(req, next, schema);
// }

function _delete(req, res, next) {
  tatService
    .delete(req.params.id, req.auth.clientId)
    .then(() => res.json({ message: "Tat deleted successfully" }))
    .catch((error) => {
      logger.logEvent("error", "Error deleting TAT record", {
        action: "DeleteTAT",
        error: error.message,
        clientId: req.auth.clientId,
        tatId: req.params.id,
      });
      next(error); // Pass the error to the global error handler
    });
}

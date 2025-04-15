const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const Role = require("../helpers/role");
const paymentService = require("./payment.service");
const { add } = require("winston");

// routes
router.get("/", authorise(), getAll);
router.get("/:id", authorise(), getByReportId);
router.post("/", authorise(), createSchema, create);
router.put("/:id", authorise(), updateSchema, update);
router.delete("/:id", authorise(), _delete);

module.exports = router;

function getAll(req, res, next) {
  paymentService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getByReportId(req, res, next) {
  paymentService
    .getByReportId(req.params.id)
    .then((payment) => (payment ? res.json(payment) : res.sendStatus(404)))
    .catch(next);
}

function createSchema(req, res, next) {
  const schema = Joi.object({
    StandardPaymentPeriodInCalendarDays: Joi.string().allow(null, ""), // Allow empty values
    ChangesToStandardPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    DetailsOfChangesToStandardPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    ShortestActualStandardPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    ChangeShortestActualPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    DetailChangeShortestActualPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    LongestActualStandardPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    ChangeLongestActualPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    DetailChangeLongestActualPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    createdBy: Joi.number().required(),
    updatedBy: Joi.number(),
    reportId: Joi.number().required(),
  });
  validateRequest(req, next, schema);
}

function create(req, res, next) {
  paymentService
    .create(req.body)
    .then((payment) => res.json(payment))
    .catch((error) => {
      console.error("Error creating payment:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function updateSchema(req, res, next) {
  console.log("Update Schema Request Body:", req.body); // Log the request body for debugging
  console.log("Update Schema Request Params:", req.params); // Log the request params for debugging
  const schema = Joi.object({
    StandardPaymentPeriodInCalendarDays: Joi.string().allow(null, ""), // Allow empty values
    ChangesToStandardPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    DetailsOfChangesToStandardPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    ShortestActualStandardPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    ChangeShortestActualPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    DetailChangeShortestActualPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    LongestActualStandardPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    ChangeLongestActualPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    DetailChangeLongestActualPaymentPeriod: Joi.string().allow(null, ""), // Allow empty values
    createdBy: Joi.number().required(),
    updatedBy: Joi.number().required(),
    reportId: Joi.number().required(),
  });
  validateRequest(req, next, schema);
}

function update(req, res, next) {
  paymentService
    .update(req.params.id, req.body)
    .then((payment) => res.json(payment))
    .catch(next);
}

function _delete(req, res, next) {
  paymentService
    .delete(req.params.id)
    .then(() => res.json({ message: "Payment deleted successfully" }))
    .catch((error) => {
      console.error("Error deleting payment:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

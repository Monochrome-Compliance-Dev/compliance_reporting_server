const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const reportService = require("./report.service");
const setClientContext = require("../middleware/set-client-context");
const { reportSchema } = require("./report.validator");

// routes
router.get("/", authorise(), setClientContext, getAll);
router.get("/report/:id", authorise(), setClientContext, getById);
router.post(
  "/",
  authorise(),
  validateRequest(reportSchema),
  setClientContext,
  create
);
router.put("/:id", authorise(), updateSchema, setClientContext, update);
router.delete("/:id", authorise(), setClientContext, _delete);

module.exports = router;

function getAll(req, res, next) {
  const clientId = req.auth.clientId; // Assumes clientId is embedded in the JWT and available on req.user
  reportService
    .getAll(clientId)
    .then((reports) => res.json(reports))
    .catch(next);
}

function getById(req, res, next) {
  reportService
    .getById(req.params.id, req.auth.clientId)
    .then((report) => (report ? res.json(report) : res.sendStatus(404)))
    .catch(next);
}

function createSchema(req, res, next) {
  const schema = Joi.object({
    ReportingPeriodStartDate: Joi.string().required(),
    ReportingPeriodEndDate: Joi.string().required(),
    code: Joi.string().required(),
    reportName: Joi.string().required(),
    createdBy: Joi.string().required(),
    reportStatus: Joi.string().required(),
    clientId: Joi.string().required(),
  });
  validateRequest(req, next, schema);
}

function create(req, res, next) {
  reportService
    .create(req.body, req.auth.clientId)
    .then((report) => res.json(report))
    .catch((error) => {
      console.error("Error creating report:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function updateSchema(req, res, next) {
  const schema = Joi.object({
    reportName: Joi.string(),
    code: Joi.string().required(),
    ReportingPeriodStartDate: Joi.string(),
    ReportingPeriodEndDate: Joi.string(),
    reportName: Joi.string(),
    createdBy: Joi.string(),
    updatedBy: Joi.string(),
    submittedDate: Joi.date().allow(null),
    submittedBy: Joi.string().allow(null),
    reportStatus: Joi.string(),
    clientId: Joi.string().required(),
  });
  validateRequest(req, next, schema);
}

function update(req, res, next) {
  reportService
    .update(req.params.id, req.body, req.auth.clientId)
    .then((report) => res.json(report))
    .catch(next);
}

function _delete(req, res, next) {
  reportService
    .delete(req.params.id, req.auth.clientId)
    .then(() => res.json({ message: "Report deleted successfully" }))
    .catch((error) => {
      console.error("Error deleting report:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

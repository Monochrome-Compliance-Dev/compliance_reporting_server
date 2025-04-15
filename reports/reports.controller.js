const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const Role = require("../helpers/role");
const reportService = require("./report.service");

// routes
router.get("/", authorise(), getAll);
router.get("/report/:clientId", authorise(), getAllById);
router.get("/:id", authorise(), getById);
router.post("/", authorise(), createSchema, create);
router.put("/:id", authorise(), updateSchema, update);
router.delete("/:id", authorise(), _delete);

module.exports = router;

function getAll(req, res, next) {
  reportService
    .getAll()
    .then((reports) => res.json(reports))
    .catch(next);
}

function getAllById(req, res, next) {
  console.log("Client ID:", req.params); // Log the client ID for debugging
  reportService
    .getAllById(req.params.clientId)
    .then((reports) => {
      if (!reports || reports.length === 0) {
        console.log("No reports found for clientId:", req.params.clientId);
        return res.status(404).json({ message: "No reports found" });
      }
      res.json(reports);
    })
    .catch((error) => {
      console.error("Error fetching report:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function getById(req, res, next) {
  reportService
    .getById(req.params.id)
    .then((report) => (report ? res.json(report) : res.sendStatus(404)))
    .catch(next);
}

function createSchema(req, res, next) {
  const schema = Joi.object({
    ReportingPeriodStartDate: Joi.string().required(),
    ReportingPeriodEndDate: Joi.string().required(),
    reportName: Joi.string().required(),
    createdBy: Joi.number().required(),
    reportStatus: Joi.string().required(),
    clientId: Joi.number().required(),
  });
  validateRequest(req, next, schema);
}

function create(req, res, next) {
  reportService
    .create(req.body)
    .then((report) => res.json(report))
    .catch((error) => {
      console.error("Error creating report:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function updateSchema(req, res, next) {
  const schema = Joi.object({
    reportName: Joi.string(),
    ReportingPeriodStartDate: Joi.string(),
    ReportingPeriodEndDate: Joi.string(),
    reportName: Joi.string(),
    createdBy: Joi.number(),
    updatedBy: Joi.number(),
    submittedDate: Joi.date(),
    submittedBy: Joi.number(),
    reportStatus: Joi.string(),
    clientId: Joi.number().required(),
  });
  validateRequest(req, next, schema);
}

function update(req, res, next) {
  reportService
    .update(req.params.id, req.body)
    .then((report) => res.json(report))
    .catch(next);
}

function _delete(req, res, next) {
  reportService
    .delete(req.params.id)
    .then(() => res.json({ message: "Report deleted successfully" }))
    .catch((error) => {
      console.error("Error deleting report:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

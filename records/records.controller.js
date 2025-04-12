const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const Role = require("../helpers/role");
const recordService = require("./record.service");
// const { record } = require("../../compliance_recording/src/data/recordFields");

// routes
router.get("/", authorise(), getAll);
router.get("/records/:clientId", authorise(), getAllById);
router.get("/:id", authorise(), getById);
router.post("/", authorise(), createSchema, create);
router.put("/:id", authorise(), updateSchema, update);
router.delete("/:id", authorise(), _delete);

module.exports = router;

function getAll(req, res, next) {
  recordService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getAllById(req, res, next) {
  console.log("Client ID:", req.params); // Log the client ID for debugging
  recordService
    .getAllById(req.params.clientId)
    .then((records) => (records ? res.json(records) : res.sendStatus(404)))
    .catch((error) => {
      console.error("Error fetching records:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function getById(req, res, next) {
  recordService
    .getById(req.params.id)
    .then((record) => (record ? res.json(record) : res.sendStatus(404)))
    .catch(next);
}

function createSchema(req, res, next) {
  const schema = Joi.object({
    ReportingPeriodStartDate: Joi.string().required(),
    ReportingPeriodEndDate: Joi.string().required(),
    recordName: Joi.string().required(),
    createdBy: Joi.number().required(),
    recordStatus: Joi.string().required(),
    clientId: Joi.number().required(),
  });
  validateRequest(req, next, schema);
}

function create(req, res, next) {
  recordService
    .create(req.body)
    .then((record) => res.json(record))
    .catch((error) => {
      console.error("Error creating record:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function updateSchema(req, res, next) {
  const schema = Joi.object({
    recordName: Joi.string().required(),
    abn: Joi.string().required(),
    acn: Joi.string().required(),
    addressline1: Joi.string().required(),
    city: Joi.string().required(),
    state: Joi.string().required(),
    postcode: Joi.string().required(),
    country: Joi.string().required(),
    postaladdressline1: Joi.string().required(),
    postalcity: Joi.string().required(),
    postalstate: Joi.string().required(),
    postalpostcode: Joi.string().required(),
    postalcountry: Joi.string().required(),
    industryCode: Joi.string().required(),
    contactFirst: Joi.string().required(),
    contactLast: Joi.string().required(),
    contactPosition: Joi.string().required(),
    contactEmail: Joi.string().required(),
    contactPhone: Joi.string().required(),
  });
  validateRequest(req, next, schema);
}

function update(req, res, next) {
  recordService
    .update(req.params.id, req.body)
    .then((record) => res.json(record))
    .catch(next);
}

function _delete(req, res, next) {
  recordService
    .delete(req.params.id)
    .then(() => res.json({ message: "Record deleted successfully" }))
    .catch((error) => {
      console.error("Error deleting record:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

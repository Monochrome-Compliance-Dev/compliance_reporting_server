const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const Role = require("../helpers/role");
const financeService = require("./finance.service");
const { add } = require("winston");

// routes
router.get("/", authorise(), getAll);
router.get("/:id", authorise(), getById);
router.post("/", authorise(), createSchema, create);
router.put("/:id", authorise(), updateSchema, update);
router.delete("/:id", authorise(), _delete);

module.exports = router;

function getAll(req, res, next) {
  financeService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getById(req, res, next) {
  financeService
    .getById(req.params.id)
    .then((finance) => (finance ? res.json(finance) : res.sendStatus(404)))
    .catch(next);
}

function createSchema(req, res, next) {
  const schema = Joi.object({
    businessName: Joi.string().required(),
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
    active: Joi.boolean().required(),
  });
  validateRequest(req, next, schema);
}

function create(req, res, next) {
  financeService
    .create(req.body)
    .then((finance) => res.json(finance))
    .catch((error) => {
      console.error("Error creating finance:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function updateSchema(req, res, next) {
  const schema = Joi.object({
    businessName: Joi.string().required(),
    abn: Joi.string().required(),
    acn: Joi.string().required(),
    addressline1: Joi.string(),
    addressline2: Joi.string(),
    addressline3: Joi.string(),
    city: Joi.string(),
    state: Joi.string(),
    postcode: Joi.string(),
    country: Joi.string(),
    postaladdressline1: Joi.string(),
    postaladdressline2: Joi.string(),
    postaladdressline3: Joi.string(),
    postalcity: Joi.string(),
    postalstate: Joi.string(),
    postalpostcode: Joi.string(),
    postalcountry: Joi.string(),
    industryCode: Joi.string(),
    contactFirst: Joi.string(),
    contactLast: Joi.string(),
    contactPosition: Joi.string(),
    contactEmail: Joi.string(),
    contactPhone: Joi.string(),
    controllingCorporationName: Joi.string().allow(null, ""), // Allow empty values
    controllingCorporationAbn: Joi.string().allow(null, ""), // Allow empty values
    controllingCorporationAcn: Joi.string().allow(null, ""), // Allow empty values
    headEntityName: Joi.string().allow(null, ""), // Allow empty values
    headEntityAbn: Joi.string().allow(null, ""), // Allow empty values
    headEntityAcn: Joi.string().allow(null, ""), // Allow empty values
    active: Joi.boolean(),
  });
  validateRequest(req, next, schema);
}

function update(req, res, next) {
  financeService
    .update(req.params.id, req.body)
    .then((finance) => res.json(finance))
    .catch(next);
}

function _delete(req, res, next) {
  financeService
    .delete(req.params.id)
    .then(() => res.json({ message: "Finance deleted successfully" }))
    .catch((error) => {
      console.error("Error deleting finance:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

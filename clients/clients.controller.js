const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const Role = require("../helpers/role");
const clientService = require("./client.service");

// routes
router.get("/", authorise(), getAll);
router.get("/:id", authorise(), getById);
router.post("/", authorise(), createSchema, create);
router.put("/:id", authorise(), updateSchema, update);
router.delete("/:id", authorise(), _delete);

module.exports = router;

function getAll(req, res, next) {
  clientService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getById(req, res, next) {
  clientService
    .getById(req.params.id)
    .then((client) => (client ? res.json(client) : res.sendStatus(404)))
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
  clientService
    .create(req.body)
    .then((client) => res.json(client))
    .catch((error) => {
      console.error("Error creating client:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function updateSchema(req, res, next) {
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
  });
  validateRequest(req, next, schema);
}

function update(req, res, next) {
  clientService
    .update(req.params.id, req.body)
    .then((client) => res.json(client))
    .catch(next);
}

function _delete(req, res, next) {
  clientService
    .delete(req.params.id)
    .then(() => res.json({ message: "Client deleted successfully" }))
    .catch((error) => {
      console.error("Error deleting client:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

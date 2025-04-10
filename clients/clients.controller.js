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
    BusinessName: Joi.string().required(),
    ABN: Joi.string().required(),
    ACN: Joi.string().required(),
    BusinessIndustryCode: Joi.string(),
  });
  validateRequest(req, next, schema);
}

function create(req, res, next) {
  clientService
    .create(req.body)
    .then((client) => res.json(client))
    .catch(next);
}

function updateSchema(req, res, next) {
  const schema = Joi.object({
    BusinessName: Joi.string().required(),
    ABN: Joi.string().required(),
    ACN: Joi.string().required(),
    BusinessIndustryCode: Joi.string(),
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
    .catch(next);
}

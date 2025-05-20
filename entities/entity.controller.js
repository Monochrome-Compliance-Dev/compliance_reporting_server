const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const entityService = require("./entity.service");
const { logger } = require("../helpers/logger");
const { entitySchema } = require("./entity.validator");

// Routes
router.post("/", validateRequest(entitySchema), create);

function create(req, res, next) {
  logger.logEvent("info", "Creating entity", {
    action: "CreateEntity",
    payload: req.body,
  });
  entityService
    .create(req.body)
    .then(() => res.status(201).json({ message: "Entity created" }))
    .catch(next);
}

module.exports = router;

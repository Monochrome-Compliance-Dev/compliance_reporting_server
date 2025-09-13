const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const entityService = require("./entity.service");
const authorise = require("../middleware/authorise");
const { logger } = require("../helpers/logger");
const { entitySchema } = require("./entity.validator");

const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Routes
router.post("/", requirePtrs, validateRequest(entitySchema), create);

function create(req, res, next) {
  logger.logEvent("info", "Creating entity", {
    action: "CreateEntity",
    payload: req.body,
  });
  entityService
    .create(req.body)
    .then((entity) =>
      res.status(201).json({ id: entity.id, message: "Entity created" })
    )
    .catch(next);
}

module.exports = router;

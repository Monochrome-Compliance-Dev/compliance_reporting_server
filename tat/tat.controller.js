const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const tatService = require("./tat.service");
const { tatSchema } = require("./tat.validator");
const { logger } = require("../helpers/logger");

// routes
router.get("/", authorise(), getAll);
router.get("/report/:id", authorise(), getAllByReportId);
router.get("/tat/:id", authorise(), getTatByReportId);
router.get("/:id", authorise(), getById);
router.post("/", authorise(), validateRequest(tatSchema), create);
router.put("/:id", authorise(), validateRequest(tatSchema), update);
router.delete("/:id", authorise(), _delete);

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

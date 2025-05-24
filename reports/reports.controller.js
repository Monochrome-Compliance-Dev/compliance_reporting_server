const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();
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
router.put(
  "/:id",
  authorise(),
  validateRequest(reportSchema),
  setClientContext,
  update
);
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

function create(req, res, next) {
  reportService
    .create(req.auth.clientId, req.body)
    .then((report) => {
      console.log("report: ", report);
      res.json(report);
    })
    .catch((error) => {
      logger.logEvent("error", "Error creating report", {
        action: "CreateReport",
        clientId: req.auth.clientId,
        error: error.message,
      });
      next(error); // Pass the error to the global error handler
    });
}

function update(req, res, next) {
  reportService
    .update(req.params.id, req.body, req.auth.clientId)
    .then((report) => {
      logger.logEvent("info", "Report updated", {
        action: "UpdateReport",
        reportId: req.params.id,
        clientId: req.auth.clientId,
        userId: req.auth.id,
      });
      res.json(report);
    })
    .catch((error) => {
      logger.logEvent("error", "Error updating report", {
        action: "UpdateReport",
        reportId: req.params.id,
        clientId: req.auth.clientId,
        error: error.message,
      });
      next(error);
    });
}

function _delete(req, res, next) {
  reportService
    .delete(req.params.id, req.auth.clientId)
    .then(() => {
      logger.logEvent("warn", "Report deleted", {
        action: "DeleteReport",
        reportId: req.params.id,
        clientId: req.auth.clientId,
        userId: req.auth.id,
      });
      res.json({ message: "Report deleted successfully" });
    })
    .catch((error) => {
      logger.logEvent("error", "Error deleting report", {
        action: "DeleteReport",
        reportId: req.params.id,
        clientId: req.auth.clientId,
        error: error.message,
      });
      next(error); // Pass the error to the global error handler
    });
}

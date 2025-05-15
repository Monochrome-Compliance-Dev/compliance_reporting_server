const logger = require("../helpers/logger");
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
    .then((report) => res.json(report))
    .catch((error) => {
      logger.error("Error creating report:", error);
      next(error); // Pass the error to the global error handler
    });
}

function update(req, res, next) {
  reportService
    .update(req.params.id, req.body, req.auth.clientId)
    .then((report) => {
      logger.info(`Report ${req.params.id} updated by user ${req.auth.id}`);
      res.json(report);
    })
    .catch((error) => {
      logger.error(`Error updating report ${req.params.id}:`, error);
      next(error);
    });
}

function _delete(req, res, next) {
  reportService
    .delete(req.params.id, req.auth.clientId)
    .then(() => {
      logger.info(`Report ${req.params.id} deleted by user ${req.auth.id}`);
      res.json({ message: "Report deleted successfully" });
    })
    .catch((error) => {
      logger.error(`Error deleting report ${req.params.id}:`, error);
      next(error); // Pass the error to the global error handler
    });
}

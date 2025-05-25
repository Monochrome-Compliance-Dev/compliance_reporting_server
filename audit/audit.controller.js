const express = require("express");
const router = express.Router();
const authorise = require("../middleware/authorise");
const auditService = require("./audit.service");
const setClientContext = require("../middleware/set-client-context");
const validateRequest = require("../middleware/validate-request");
const { auditSchema } = require("./audit.validator");
const { logger } = require("../helpers/logger");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

const tcpService = require("../tcp/tcp.service");

// routes
router.get("/", authorise(), setClientContext, getAll);
router.get("/:id", authorise(), setClientContext, getById);
// Only allow GET routes for audit, POST logic is moved to TCP controller

module.exports = router;

async function getAll(req, res, next) {
  const clientId = req.auth.clientId; // Assumes clientId is embedded in the JWT and available on req.auth
  auditService
    .getAll(clientId)
    .then((audits) => {
      logger.logEvent("info", "Fetched all audits", {
        action: "GetAllAudits",
        clientId,
        userId: req.auth.id,
        count: Array.isArray(audits) ? audits.length : undefined,
      });
      res.json(audits);
    })
    .catch((error) => {
      logger.logEvent("error", "Error fetching all audits", {
        action: "GetAllAudits",
        clientId,
        userId: req.auth.id,
        error: error.message,
      });
      next(error); // Pass the error to the global error handler
    });
}

async function getById(req, res, next) {
  auditService
    .getById(req.params.id, req.auth.clientId)
    .then((audit) => {
      logger.logEvent("info", "Fetched audit by ID", {
        action: "GetAuditById",
        clientId: req.auth.clientId,
        auditId: req.params.id,
      });
      res.json(audit);
    })
    .catch((error) => {
      logger.logEvent("error", "Error fetching audit by ID", {
        action: "GetAuditById",
        clientId: req.auth.clientId,
        auditId: req.params.id,
        error: error.message,
      });
      next(error); // Pass the error to the global error handler
    });
}

// The POST /audit logic is now handled directly in TCP controller patchRecord.

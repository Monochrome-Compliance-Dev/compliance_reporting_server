const express = require("express");
const router = express.Router();
const authorise = require("../middleware/authorise");
const auditService = require("./audit.service");
const { logger } = require("../helpers/logger");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

// routes
router.get("/", authorise(), getAll);
router.get("/:id", authorise(), getById);
// Only allow GET routes for audit, POST logic is moved to TCP controller

module.exports = router;

async function getAll(req, res, next) {
  auditService
    .getAll({ transaction: req.dbTransaction })
    .then((audits) => {
      logger.logEvent("info", "Fetched all audits", {
        action: "GetAllAudits",
        clientId: req.auth.clientId,
        userId: req.auth.id,
        count: Array.isArray(audits) ? audits.length : undefined,
      });
      req.dbTransaction.commit();
      res.json(audits);
    })
    .catch((error) => {
      logger.logEvent("error", "Error fetching all audits", {
        action: "GetAllAudits",
        clientId: req.auth.clientId,
        userId: req.auth.id,
        error: error.message,
      });
      req.dbTransaction.rollback();
      next(error);
    });
}

async function getById(req, res, next) {
  auditService
    .getById(req.params.id, { transaction: req.dbTransaction })
    .then((audit) => {
      logger.logEvent("info", "Fetched audit by ID", {
        action: "GetAuditById",
        clientId: req.auth.clientId,
        auditId: req.params.id,
      });
      req.dbTransaction.commit();
      res.json(audit);
    })
    .catch((error) => {
      logger.logEvent("error", "Error fetching audit by ID", {
        action: "GetAuditById",
        clientId: req.auth.clientId,
        auditId: req.params.id,
        error: error.message,
      });
      req.dbTransaction.rollback();
      next(error);
    });
}

// The POST /audit logic is now handled directly in TCP controller patchRecord.

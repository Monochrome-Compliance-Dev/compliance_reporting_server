const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const reportService = require("./report.service");
const { reportSchema } = require("./report.validator");

// routes
router.get("/", authorise(), getAll);
router.get("/report/:id", authorise(), getById);
router.post("/", authorise(), validateRequest(reportSchema), create);
router.put("/:id", authorise(), validateRequest(reportSchema), update);
router.delete("/:id", authorise(), _delete);

module.exports = router;

function getAll(req, res, next) {
  reportService
    .getAll({ transaction: req.dbTransaction })
    .then((reports) => {
      logger.logEvent("info", "Fetched all reports", {
        action: "GetAllReports",
        userId: req.auth.id,
        count: Array.isArray(reports) ? reports.length : undefined,
      });
      res.json(reports);
    })
    .catch((error) => {
      logger.logEvent("error", "Error fetching all reports", {
        action: "GetAllReports",
        userId: req.auth.id,
        error: error.message,
      });
      next(error);
    });
}

function getById(req, res, next) {
  reportService
    .getById(req.params.id, { transaction: req.dbTransaction }) // Add the transaction here
    .then((report) => {
      if (report) {
        logger.logEvent("info", "Fetched report by ID", {
          action: "GetReportById",
          reportId: req.params.id,
          userId: req.auth.id,
        });
        res.json(report);
      } else {
        logger.logEvent("warn", "Report not found", {
          action: "GetReportById",
          reportId: req.params.id,
          userId: req.auth.id,
        });
        res.sendStatus(404);
      }
    })
    .catch((error) => {
      logger.logEvent("error", "Error fetching report by ID", {
        action: "GetReportById",
        reportId: req.params.id,
        userId: req.auth.id,
        error: error.message,
      });
      next(error);
    });
}

async function create(req, res, next) {
  // console.log("Creating report with data:", req.body, req.auth);
  try {
    const report = await reportService.create(req.body, {
      transaction: req.dbTransaction,
    });
    logger.logEvent("info", "Report created", {
      action: "CreateReport",
      reportId: report.id,
      userId: req.auth.id,
    });
    await req.dbTransaction.commit();
    res.json(report);
  } catch (error) {
    logger.logEvent("error", "Error creating report", {
      action: "CreateReport",
      error: error.message,
    });
    await req.dbTransaction.rollback();
    next(error);
  }
}

function update(req, res, next) {
  reportService
    .update(req.params.id, req.body, { transaction: req.dbTransaction })
    .then((report) => {
      logger.logEvent("info", "Report updated", {
        action: "UpdateReport",
        reportId: req.params.id,
        userId: req.auth.id,
      });
      req.dbTransaction.commit();
      res.json(report);
    })
    .catch((error) => {
      logger.logEvent("error", "Error updating report", {
        action: "UpdateReport",
        reportId: req.params.id,
        error: error.message,
      });
      req.dbTransaction.rollback();
      next(error);
    });
}

async function _delete(req, res, next) {
  try {
    const { id } = req.params;
    const clientId = req.auth.clientId;

    if (!clientId) {
      logger.logEvent("warn", "No clientId found for RLS - skipping");
      return res.status(400).json({ message: "Client ID missing" });
    }

    await reportService.delete(id, clientId, {
      transaction: req.dbTransaction,
    });
    res.status(204).send();
  } catch (err) {
    next(err);
  }
}

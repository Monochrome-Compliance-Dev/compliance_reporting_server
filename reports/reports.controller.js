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
router.patch("/:id", authorise(), patch);
router.delete("/:id", authorise(), _delete);

module.exports = router;

async function getAll(req, res, next) {
  const timestamp = new Date().toISOString();
  try {
    const clientId = req.auth?.clientId;
    const userId = req.auth?.id;
    logger.logEvent("info", "Fetching all reports", {
      action: "GetAllReports",
      userId,
      clientId,
      timestamp,
    });
    const reports = await reportService.getAll({ clientId });
    logger.logEvent("info", "Fetched all reports", {
      action: "GetAllReports",
      userId,
      clientId,
      count: Array.isArray(reports) ? reports.length : undefined,
      timestamp,
    });
    res.json({ status: "success", data: reports });
  } catch (error) {
    logger.logEvent("error", "Error fetching all reports", {
      action: "GetAllReports",
      userId: req.auth?.id,
      clientId: req.auth?.clientId,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

async function getById(req, res, next) {
  const timestamp = new Date().toISOString();
  const id = req.params.id;
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  logger.logEvent("info", "Fetching report by ID", {
    action: "GetReportById",
    id,
    clientId,
    userId,
    timestamp,
  });
  try {
    const report = await reportService.getById({ id, clientId });
    if (report) {
      logger.logEvent("info", "Fetched report by ID", {
        action: "GetReportById",
        id,
        clientId,
        userId,
        timestamp,
      });
      res.json({ status: "success", data: report });
    } else {
      logger.logEvent("warn", "Report not found", {
        action: "GetReportById",
        id,
        clientId,
        userId,
        timestamp,
      });
      res.sendStatus(404);
    }
  } catch (error) {
    logger.logEvent("error", "Error fetching report by ID", {
      action: "GetReportById",
      id,
      clientId,
      userId,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

async function create(req, res, next) {
  const timestamp = new Date().toISOString();
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  logger.logEvent("info", "Creating report", {
    action: "CreateReport",
    clientId,
    userId,
    timestamp,
  });
  try {
    const report = await reportService.create({ data: req.body, clientId });
    logger.logEvent("info", "Report created", {
      action: "CreateReport",
      id: report.id,
      clientId,
      userId,
      timestamp,
    });
    res.json({ status: "success", data: report });
  } catch (error) {
    logger.logEvent("error", "Error creating report", {
      action: "CreateReport",
      clientId,
      userId,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

async function update(req, res, next) {
  const timestamp = new Date().toISOString();
  const id = req.params.id;
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  logger.logEvent("info", "Updating report", {
    action: "UpdateReport",
    id,
    clientId,
    userId,
    timestamp,
  });
  try {
    const report = await reportService.update({ id, data: req.body, clientId });
    logger.logEvent("info", "Report updated", {
      action: "UpdateReport",
      id,
      clientId,
      userId,
      timestamp,
    });
    res.json({ status: "success", data: report });
  } catch (error) {
    logger.logEvent("error", "Error updating report", {
      action: "UpdateReport",
      id,
      clientId,
      userId,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

async function patch(req, res, next) {
  const timestamp = new Date().toISOString();
  const id = req.params.id;
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  logger.logEvent("info", "Patching report", {
    action: "PatchReport",
    id,
    clientId,
    userId,
    timestamp,
  });
  try {
    if (!clientId) {
      logger.logEvent("warn", "No clientId found for RLS - skipping", {
        action: "PatchReport",
        id,
        userId,
        timestamp,
      });
      return res.status(400).json({ message: "Client ID missing" });
    }
    const report = await reportService.patch({ id, data: req.body, clientId });
    logger.logEvent("info", "Report patched", {
      action: "PatchReport",
      id,
      clientId,
      userId,
      timestamp,
    });
    res.json({ status: "success", data: report });
  } catch (error) {
    logger.logEvent("error", "Error patching report", {
      action: "PatchReport",
      id,
      clientId,
      userId,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

async function _delete(req, res, next) {
  const timestamp = new Date().toISOString();
  const id = req.params.id;
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  logger.logEvent("info", "Deleting report", {
    action: "DeleteReport",
    id,
    clientId,
    userId,
    timestamp,
  });
  try {
    if (!clientId) {
      logger.logEvent("warn", "No clientId found for RLS - skipping", {
        action: "DeleteReport",
        id,
        userId,
        timestamp,
      });
      return res.status(400).json({ message: "Client ID missing" });
    }
    await reportService.delete({ id, clientId });
    logger.logEvent("info", "Report deleted", {
      action: "DeleteReport",
      id,
      clientId,
      userId,
      timestamp,
    });
    res.status(204).json({ status: "success" });
  } catch (error) {
    logger.logEvent("error", "Error deleting report", {
      action: "DeleteReport",
      id,
      clientId,
      userId,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

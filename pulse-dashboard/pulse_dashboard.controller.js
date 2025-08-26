const express = require("express");
const router = express.Router();
const authorise = require("../middleware/authorise");
const pulseDashboardService = require("./pulse_dashboard.service");
const { logger } = require("../helpers/logger");

// NOTE: This controller is mounted under '/pulse'.
// Therefore all routes here intentionally include the '/dashboard/...' prefix
// so the final paths are '/pulse/dashboard/:id', etc.

// Main dashboard route
router.get("/dashboard/:id", authorise(), getDashboard);

// Granular routes for each metric (mirrors example style)
router.get("/dashboard/:id/totals", authorise(), getTotals);
router.get("/dashboard/:id/status", authorise(), getStatus);
router.get("/dashboard/:id/weekly-burn", authorise(), getWeeklyBurn);
router.get("/dashboard/:id/overruns", authorise(), getOverruns);
router.get("/dashboard/:id/utilisation", authorise(), getUtilisation);
router.get("/dashboard/:id/billable", authorise(), getBillable);
router.get("/dashboard/:id/revenue", authorise(), getRevenue);
router.get("/dashboard/:id/timeliness", authorise(), getTimeliness);
router.get("/dashboard/:id/turnaround", authorise(), getTurnaround);

module.exports = router;

function getDashboard(req, res, next) {
  const customerId = req.auth?.customerId;
  const orgId = req.params.id;
  logger.logEvent("info", "Pulse: controller getDashboard invoked", {
    action: "PulseGetDashboard",
    customerId,
    orgId,
    path: req.originalUrl,
  });
  pulseDashboardService
    .getDashboard(orgId, customerId)
    .then((dashboard) => {
      logger.logEvent("info", "Pulse: controller getDashboard success", {
        action: "PulseGetDashboard",
        customerId,
        orgId,
      });
      return dashboard ? res.json(dashboard) : res.sendStatus(404);
    })
    .catch((err) => {
      logger.logEvent("error", "Pulse: controller getDashboard failed", {
        action: "PulseGetDashboard",
        customerId,
        orgId,
        error: err?.message,
      });
      next(err);
    });
}

function getTotals(req, res, next) {
  const customerId = req.auth?.customerId;
  const orgId = req.params.id;
  logger.logEvent("info", "Pulse: controller getTotals invoked", {
    action: "PulseGetTotals",
    customerId,
    orgId,
  });
  pulseDashboardService
    .getTotals(orgId, customerId)
    .then((totals) => (totals ? res.json(totals) : res.sendStatus(404)))
    .catch((err) => {
      logger.logEvent("error", "Pulse: controller getTotals failed", {
        action: "PulseGetTotals",
        customerId,
        orgId,
        error: err?.message,
      });
      next(err);
    });
}

function getStatus(req, res, next) {
  const customerId = req.auth?.customerId;
  const orgId = req.params.id;
  logger.logEvent("info", "Pulse: controller getStatus invoked", {
    action: "PulseGetStatus",
    customerId,
    orgId,
  });
  pulseDashboardService
    .getEngagementStatus(orgId, customerId)
    .then((status) => (status ? res.json(status) : res.sendStatus(404)))
    .catch((err) => {
      logger.logEvent("error", "Pulse: controller getStatus failed", {
        action: "PulseGetStatus",
        customerId,
        orgId,
        error: err?.message,
      });
      next(err);
    });
}

function getWeeklyBurn(req, res, next) {
  const customerId = req.auth?.customerId;
  const orgId = req.params.id;
  logger.logEvent("info", "Pulse: controller getWeeklyBurn invoked", {
    action: "PulseGetWeeklyBurn",
    customerId,
    orgId,
  });
  pulseDashboardService
    .getWeeklyBurn(orgId, customerId)
    .then((burn) => (burn ? res.json(burn) : res.sendStatus(404)))
    .catch((err) => {
      logger.logEvent("error", "Pulse: controller getWeeklyBurn failed", {
        action: "PulseGetWeeklyBurn",
        customerId,
        orgId,
        error: err?.message,
      });
      next(err);
    });
}

function getOverruns(req, res, next) {
  const customerId = req.auth?.customerId;
  const orgId = req.params.id;
  logger.logEvent("info", "Pulse: controller getOverruns invoked", {
    action: "PulseGetOverruns",
    customerId,
    orgId,
  });
  pulseDashboardService
    .getOverruns(orgId, customerId)
    .then((overruns) => (overruns ? res.json(overruns) : res.sendStatus(404)))
    .catch((err) => {
      logger.logEvent("error", "Pulse: controller getOverruns failed", {
        action: "PulseGetOverruns",
        customerId,
        orgId,
        error: err?.message,
      });
      next(err);
    });
}

function getUtilisation(req, res, next) {
  const customerId = req.auth?.customerId;
  const orgId = req.params.id;
  logger.logEvent("info", "Pulse: controller getUtilisation invoked", {
    action: "PulseGetUtilisation",
    customerId,
    orgId,
  });
  pulseDashboardService
    .getResourceUtilisation(orgId, customerId)
    .then((utilisation) =>
      utilisation ? res.json(utilisation) : res.sendStatus(404)
    )
    .catch((err) => {
      logger.logEvent("error", "Pulse: controller getUtilisation failed", {
        action: "PulseGetUtilisation",
        customerId,
        orgId,
        error: err?.message,
      });
      next(err);
    });
}

function getBillable(req, res, next) {
  const customerId = req.auth?.customerId;
  const orgId = req.params.id;
  logger.logEvent("info", "Pulse: controller getBillable invoked", {
    action: "PulseGetBillable",
    customerId,
    orgId,
  });
  pulseDashboardService
    .getBillableSplit(orgId, customerId)
    .then((billable) => (billable ? res.json(billable) : res.sendStatus(404)))
    .catch((err) => {
      logger.logEvent("error", "Pulse: controller getBillable failed", {
        action: "PulseGetBillable",
        customerId,
        orgId,
        error: err?.message,
      });
      next(err);
    });
}

function getRevenue(req, res, next) {
  const customerId = req.auth?.customerId;
  const orgId = req.params.id;
  logger.logEvent("info", "Pulse: controller getRevenue invoked", {
    action: "PulseGetRevenue",
    customerId,
    orgId,
  });
  pulseDashboardService
    .getRevenueBars(orgId, customerId)
    .then((revenue) => (revenue ? res.json(revenue) : res.sendStatus(404)))
    .catch((err) => {
      logger.logEvent("error", "Pulse: controller getRevenue failed", {
        action: "PulseGetRevenue",
        customerId,
        orgId,
        error: err?.message,
      });
      next(err);
    });
}

function getTimeliness(req, res, next) {
  const customerId = req.auth?.customerId;
  const orgId = req.params.id;
  logger.logEvent("info", "Pulse: controller getTimeliness invoked", {
    action: "PulseGetTimeliness",
    customerId,
    orgId,
  });
  pulseDashboardService
    .getAssignmentTimeliness(orgId, customerId)
    .then((timeliness) =>
      timeliness ? res.json(timeliness) : res.sendStatus(404)
    )
    .catch((err) => {
      logger.logEvent("error", "Pulse: controller getTimeliness failed", {
        action: "PulseGetTimeliness",
        customerId,
        orgId,
        error: err?.message,
      });
      next(err);
    });
}

function getTurnaround(req, res, next) {
  const customerId = req.auth?.customerId;
  const orgId = req.params.id;
  logger.logEvent("info", "Pulse: controller getTurnaround invoked", {
    action: "PulseGetTurnaround",
    customerId,
    orgId,
  });
  pulseDashboardService
    .getTurnaround(orgId, customerId)
    .then((turnaround) =>
      turnaround ? res.json(turnaround) : res.sendStatus(404)
    )
    .catch((err) => {
      logger.logEvent("error", "Pulse: controller getTurnaround failed", {
        action: "PulseGetTurnaround",
        customerId,
        orgId,
        error: err?.message,
      });
      next(err);
    });
}

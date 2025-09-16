const express = require("express");
const router = express.Router();
const authorise = require("../middleware/authorise");
const dashboardService = require("./dashboard.service");
const { logger } = require("../helpers/logger");

const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Dashboard analytics routes only
router.get("/:id/metrics", requirePtrs, getDashboardMetrics);
router.get("/:id/metrics/previous", requirePtrs, getDashboardPreviousMetrics);
router.get("/:id/flags", requirePtrs, getDashboardFlags);
router.get("/:id/snapshot", requirePtrs, getDashboardSnapshot);
router.get("/:id/signals", requirePtrs, getDashboardSignals);
router.get("/:id/extended-metrics", requirePtrs, getDashboardExtendedMetrics);

module.exports = router;

async function getDashboardMetrics(req, res, next) {
  const ptrsId = req.params.id;
  const customerId = req.auth.customerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const metrics = await dashboardService.getDashboardMetrics(
      ptrsId,
      customerId
    );
    if (!metrics) return res.sendStatus(404);
    return res.json(metrics);
  } catch (error) {
    logger.logEvent("error", "PtrsGetDashboardMetrics failed", {
      action: "PtrsGetDashboardMetrics",
      ptrsId,
      customerId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getDashboardPreviousMetrics(req, res, next) {
  const ptrsId = req.params.id;
  const customerId = req.auth.customerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const metrics = await dashboardService.getPreviousDashboardMetrics(
      ptrsId,
      customerId
    );
    if (!metrics) return res.sendStatus(404);
    return res.json(metrics);
  } catch (error) {
    logger.logEvent("error", "PtrsGetDashboardPreviousMetrics failed", {
      action: "PtrsGetDashboardPreviousMetrics",
      ptrsId,
      customerId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getDashboardFlags(req, res, next) {
  const ptrsId = req.params.id;
  const customerId = req.auth.customerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const flags = await dashboardService.getDashboardFlags(ptrsId, customerId);
    if (!flags) return res.sendStatus(404);
    return res.json(flags);
  } catch (error) {
    logger.logEvent("error", "PtrsGetDashboardFlags failed", {
      action: "PtrsGetDashboardFlags",
      ptrsId,
      customerId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getDashboardSnapshot(req, res, next) {
  const ptrsId = req.params.id;
  const customerId = req.auth.customerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const snapshot = await dashboardService.getDashboardSnapshot(
      ptrsId,
      customerId
    );
    if (!snapshot) return res.sendStatus(404);
    return res.json(snapshot);
  } catch (error) {
    logger.logEvent("error", "PtrsGetDashboardSnapshot failed", {
      action: "PtrsGetDashboardSnapshot",
      ptrsId,
      customerId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getDashboardSignals(req, res, next) {
  const ptrsId = req.params.id;
  const customerId = req.auth.customerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const { start, end } = req.query || {};
  try {
    const signals = await dashboardService.getDashboardSignals(
      ptrsId,
      customerId,
      { start, end }
    );
    if (!signals) return res.sendStatus(404);
    return res.json(signals);
  } catch (error) {
    logger.logEvent("error", "PtrsGetDashboardSignals failed", {
      action: "PtrsGetDashboardSignals",
      ptrsId,
      customerId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getDashboardExtendedMetrics(req, res, next) {
  const ptrsId = req.params.id;
  const customerId = req.auth.customerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const { start, end } = req.query || {};
  try {
    const metrics = await dashboardService.getDashboardExtendedMetrics(
      ptrsId,
      customerId,
      { start, end }
    );
    if (!metrics) return res.sendStatus(404);
    return res.json(metrics);
  } catch (error) {
    logger.logEvent("error", "PtrsGetDashboardExtendedMetrics failed", {
      action: "PtrsGetDashboardExtendedMetrics",
      ptrsId,
      customerId,
      userId,
      ip,
      device,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

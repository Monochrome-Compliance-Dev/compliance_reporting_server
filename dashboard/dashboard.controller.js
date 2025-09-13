const express = require("express");
const router = express.Router();
const authorise = require("../middleware/authorise");
const dashboardService = require("./dashboard.service");

const requirePulse = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "pulse",
});

// Dashboard analytics routes only
router.get("/:id/metrics", requirePulse, getDashboardMetrics);
router.get("/:id/metrics/previous", requirePulse, getDashboardPreviousMetrics);
router.get("/:id/flags", requirePulse, getDashboardFlags);
router.get("/:id/snapshot", requirePulse, getDashboardSnapshot);
router.get("/:id/signals", requirePulse, getDashboardSignals);
router.get("/:id/extended-metrics", requirePulse, getDashboardExtendedMetrics);

module.exports = router;

function getDashboardMetrics(req, res, next) {
  dashboardService
    .getDashboardMetrics(req.params.id, req.auth.customerId)
    .then((metrics) => (metrics ? res.json(metrics) : res.sendStatus(404)))
    .catch(next);
}

function getDashboardPreviousMetrics(req, res, next) {
  dashboardService
    .getPreviousDashboardMetrics(req.params.id, req.auth.customerId)
    .then((metrics) => (metrics ? res.json(metrics) : res.sendStatus(404)))
    .catch(next);
}

function getDashboardFlags(req, res, next) {
  dashboardService
    .getDashboardFlags(req.params.id, req.auth.customerId)
    .then((flags) => (flags ? res.json(flags) : res.sendStatus(404)))
    .catch(next);
}

function getDashboardSnapshot(req, res, next) {
  dashboardService
    .getDashboardSnapshot(req.params.id, req.auth.customerId)
    .then((snapshot) => (snapshot ? res.json(snapshot) : res.sendStatus(404)))
    .catch(next);
}

function getDashboardSignals(req, res, next) {
  dashboardService
    .getDashboardSignals(req.params.id, req.auth.customerId)
    .then((signals) => (signals ? res.json(signals) : res.sendStatus(404)))
    .catch(next);
}

function getDashboardExtendedMetrics(req, res, next) {
  dashboardService
    .getDashboardExtendedMetrics(req.params.id, req.auth.customerId)
    .then((metrics) => (metrics ? res.json(metrics) : res.sendStatus(404)))
    .catch(next);
}

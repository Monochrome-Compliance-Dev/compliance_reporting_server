const express = require("express");
const router = express.Router();
const authorise = require("../middleware/authorise");
const dashboardService = require("./dashboard.service");

// Dashboard analytics routes only
router.get("/:id/metrics", authorise(), getDashboardMetrics);
router.get("/:id/metrics/previous", authorise(), getDashboardPreviousMetrics);
router.get("/:id/flags", authorise(), getDashboardFlags);
router.get("/:id/snapshot", authorise(), getDashboardSnapshot);
router.get("/:id/signals", authorise(), getDashboardSignals);

module.exports = router;

function getDashboardMetrics(req, res, next) {
  dashboardService
    .getDashboardMetrics(req.params.id, req.auth.clientId)
    .then((metrics) => (metrics ? res.json(metrics) : res.sendStatus(404)))
    .catch(next);
}

function getDashboardPreviousMetrics(req, res, next) {
  dashboardService
    .getPreviousDashboardMetrics(req.params.id, req.auth.clientId)
    .then((metrics) => (metrics ? res.json(metrics) : res.sendStatus(404)))
    .catch(next);
}

function getDashboardFlags(req, res, next) {
  dashboardService
    .getDashboardFlags(req.params.id, req.auth.clientId)
    .then((flags) => (flags ? res.json(flags) : res.sendStatus(404)))
    .catch(next);
}

function getDashboardSnapshot(req, res, next) {
  dashboardService
    .getDashboardSnapshot(req.params.id, req.auth.clientId)
    .then((snapshot) => (snapshot ? res.json(snapshot) : res.sendStatus(404)))
    .catch(next);
}

function getDashboardSignals(req, res, next) {
  dashboardService
    .getDashboardSignals(req.params.id, req.auth.clientId)
    .then((signals) => (signals ? res.json(signals) : res.sendStatus(404)))
    .catch(next);
}

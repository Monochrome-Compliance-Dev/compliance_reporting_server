const express = require("express");
const router = express.Router();
const esgService = require("./esg.service");
const auditService = require("../audit/audit.service");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const { esgIndicatorSchema, esgMetricSchema } = require("./esg.validator");

// routes
router.post(
  "/indicators",
  authorise(),
  validateRequest(esgIndicatorSchema),
  createIndicator
);
router.post(
  "/metrics",
  authorise(),
  validateRequest(esgMetricSchema),
  createMetric
);
router.post("/reporting-periods", authorise(), createReportingPeriod);
router.get("/reporting-periods", authorise(), getReportingPeriodsByClient);
router.get("/metrics", authorise(), getMetricsByClient);
router.get(
  "/indicators/:reportingPeriodId",
  authorise(),
  getIndicatorsByReportingPeriodId
);
router.get(
  "/metrics/:reportingPeriodId",
  authorise(),
  getMetricsByReportingPeriodId
);
router.delete("/indicators/:indicatorId", authorise(), deleteIndicator);
router.delete("/metrics/:metricId", authorise(), deleteMetric);

async function getReportingPeriodsByClient(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    const periods = await esgService.getReportingPeriodsByClient(clientId);

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetReportingPeriods",
      entity: "ReportingPeriod",
      details: { count: periods.length },
    });

    res.json(periods);
  } catch (err) {
    next(err);
  }
}

async function getIndicatorsByReportingPeriodId(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const reportingPeriodId = req.params.reportingPeriodId;

    const indicators = await esgService.getIndicatorsByReportingPeriodId(
      clientId,
      reportingPeriodId
    );

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetIndicatorsByReportingPeriod",
      entity: "ESGIndicator",
      entityId: reportingPeriodId,
      details: { count: indicators.length },
    });

    res.json(indicators);
  } catch (err) {
    next(err);
  }
}

async function getMetricsByReportingPeriodId(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const reportingPeriodId = req.params.reportingPeriodId;

    const metrics = await esgService.getMetricsByReportingPeriodId(
      clientId,
      reportingPeriodId
    );

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetMetricsByReportingPeriod",
      entity: "ESGMetric",
      entityId: reportingPeriodId,
      details: { count: metrics.length },
    });

    res.json(metrics);
  } catch (err) {
    next(err);
  }
}

module.exports = router;

async function createIndicator(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { code, name, description, category, reportingPeriodId } = req.body;

    const indicator = await esgService.createIndicator({
      clientId,
      code,
      name,
      description,
      category,
      reportingPeriodId,
    });

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "CreateESGIndicator",
      entity: "ESGIndicator",
      entityId: indicator.id,
      details: { code, name, description, category, reportingPeriodId },
    });

    res
      .status(201)
      .json(indicator.get ? indicator.get({ plain: true }) : indicator);
  } catch (err) {
    next(err);
  }
}

async function createMetric(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { indicatorId, reportingPeriodId, value, unit } = req.body;

    const metric = await esgService.createMetric({
      clientId,
      indicatorId,
      reportingPeriodId,
      value,
      unit,
    });

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "CreateESGMetric",
      entity: "ESGMetric",
      entityId: metric.id,
      details: { indicatorId, reportingPeriodId, value, unit },
    });

    res.status(201).json(metric.get ? metric.get({ plain: true }) : metric);
  } catch (err) {
    next(err);
  }
}

async function getMetricsByClient(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    const metrics = await esgService.getMetricsByClient(clientId);

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetESGMetrics",
      entity: "ESGMetric",
      details: { count: metrics.length },
    });

    res.json(metrics);
  } catch (err) {
    next(err);
  }
}

// Handler for creating ESG reporting periods
async function createReportingPeriod(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { name, startDate, endDate } = req.body;

    const period = await esgService.createReportingPeriod({
      clientId,
      name,
      startDate,
      endDate,
    });

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "CreateReportingPeriod",
      entity: "ReportingPeriod",
      entityId: period.id,
      details: { name, startDate, endDate },
    });

    res.status(201).json(period.get ? period.get({ plain: true }) : period);
  } catch (err) {
    next(err);
  }
}

// Handler for deleting an ESG indicator
async function deleteIndicator(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const indicatorId = req.params.indicatorId;

    await esgService.deleteIndicator(clientId, indicatorId);

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "DeleteESGIndicator",
      entity: "ESGIndicator",
      entityId: indicatorId,
    });

    res.status(204).send();
  } catch (err) {
    next(err);
  }
}

// Handler for deleting an ESG metric
async function deleteMetric(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const metricId = req.params.metricId;

    await esgService.deleteMetric(clientId, metricId);

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "DeleteESGMetric",
      entity: "ESGMetric",
      entityId: metricId,
    });

    res.status(204).send();
  } catch (err) {
    next(err);
  }
}

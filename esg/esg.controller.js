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

async function getReportingPeriodsByClient(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;

    const periods = await esgService.getReportingPeriodsByClient(clientId);

    await auditService.logEvent({
      clientId,
      userId,
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
    const reportingPeriodId = req.params.reportingPeriodId;

    const indicators = await esgService.getIndicatorsByReportingPeriodId(
      clientId,
      reportingPeriodId
    );

    await auditService.logEvent({
      clientId,
      userId,
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
    const reportingPeriodId = req.params.reportingPeriodId;

    const metrics = await esgService.getMetricsByReportingPeriodId(
      clientId,
      reportingPeriodId
    );

    await auditService.logEvent({
      clientId,
      userId,
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

    const metrics = await esgService.getMetricsByClient(clientId);

    await auditService.logEvent({
      clientId,
      userId,
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

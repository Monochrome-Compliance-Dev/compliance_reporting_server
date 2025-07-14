const esgAnalytics = require("./esg_analytics.service");
const express = require("express");
const router = express.Router();
const esgService = require("./esg.service");
const auditService = require("../audit/audit.service");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const {
  esgIndicatorSchema,
  esgMetricSchema,
  esgUnitSchema,
} = require("./esg.validator");

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
router.get("/reporting-periods/:id", authorise(), getReportingPeriodById);
router.get("/metrics", authorise(), getMetricsByClient);
router.get(
  "/indicators/:reportingPeriodId",
  authorise(),
  getIndicatorsByReportingPeriodId
);
router.get(
  "/metrics/by-reporting-period/:reportingPeriodId",
  authorise(),
  getMetricsByReportingPeriodId
);

// Dashboard analytics routes
router.get(
  "/dashboard/category-totals/:reportingPeriodId",
  authorise(),
  getCategoryTotals
);

router.get(
  "/dashboard/indicators-with-metrics/:reportingPeriodId",
  authorise(),
  getAllIndicatorsWithLatestMetrics
);

// New route for dashboard totals by indicator
router.get(
  "/dashboard/totals-by-indicator/:reportingPeriodId",
  authorise(),
  getTotalsByIndicator
);

// Get a single metric by ID
router.get("/metrics/:metricId", authorise(), getMetricById);
router.delete("/indicators/:indicatorId", authorise(), deleteIndicator);
router.delete("/metrics/:metricId", authorise(), deleteMetric);

// Approval workflow endpoints
router.post(
  "/reporting-periods/:id/submit",
  authorise(),
  submitReportingPeriod
);
router.post(
  "/reporting-periods/:id/approve",
  authorise(),
  approveReportingPeriod
);
router.post(
  "/reporting-periods/:id/rollback",
  authorise(),
  rollbackReportingPeriod
);

// Clone templates for reporting period
router.post(
  "/reporting-periods/:id/clone-templates",
  authorise(),
  cloneTemplatesForReportingPeriod
);

// Unit routes
router.post("/units", authorise(), validateRequest(esgUnitSchema), createUnit);
router.get("/units", authorise(), getUnitsByClient);
router.get("/units/:id", authorise(), getUnitById);
router.put(
  "/units/:id",
  authorise(),
  validateRequest(esgUnitSchema),
  updateUnit
);
router.delete("/units/:id", authorise(), deleteUnit);

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
    const { code, name, description, category, reportingPeriodId, isTemplate } =
      req.body;

    const indicator = await esgService.createIndicator({
      clientId,
      code,
      name,
      description,
      category,
      reportingPeriodId,
      isTemplate,
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
    const { indicatorId, reportingPeriodId, value, unit, isTemplate } =
      req.body;

    const metric = await esgService.createMetric({
      clientId,
      indicatorId,
      reportingPeriodId,
      value,
      unit,
      isTemplate,
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

// Approval workflow handlers
async function submitReportingPeriod(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;

    const period = await esgService.getReportingPeriodById(clientId, id);
    if (!period) return res.status(404).json({ error: "Not found" });

    if (period.status !== "Draft")
      return res.status(400).json({ error: "Only Draft can be submitted." });

    await esgService.updateReportingPeriod(clientId, id, {
      status: "PendingApproval",
      submittedBy: userId,
      submittedAt: new Date(),
    });

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "SubmitReportingPeriod",
      entity: "ReportingPeriod",
      entityId: id,
      details: { status: "PendingApproval" },
    });

    res.json({ message: "Reporting period submitted for approval." });
  } catch (err) {
    next(err);
  }
}

async function approveReportingPeriod(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;

    const period = await esgService.getReportingPeriodById(clientId, id);
    if (!period) return res.status(404).json({ error: "Not found" });

    if (period.status !== "PendingApproval")
      return res
        .status(400)
        .json({ error: "Only PendingApproval can be approved." });

    await esgService.updateReportingPeriod(clientId, id, {
      status: "Approved",
      approvedBy: userId,
      approvedAt: new Date(),
    });

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "ApproveReportingPeriod",
      entity: "ReportingPeriod",
      entityId: id,
      details: { status: "Approved" },
    });

    res.json({ message: "Reporting period approved and locked." });
  } catch (err) {
    next(err);
  }
}

async function rollbackReportingPeriod(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;

    const period = await esgService.getReportingPeriodById(clientId, id);
    if (!period) return res.status(404).json({ error: "Not found" });

    if (period.status !== "PendingApproval" && period.status !== "Approved")
      return res
        .status(400)
        .json({ error: "Only PendingApproval or Approved can rollback." });

    await esgService.updateReportingPeriod(clientId, id, {
      status: "Draft",
    });

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "RollbackReportingPeriod",
      entity: "ReportingPeriod",
      entityId: id,
      details: { status: "Draft" },
    });

    res.json({ message: "Reporting period rolled back to Draft." });
  } catch (err) {
    next(err);
  }
}

async function getReportingPeriodById(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;

    const period = await esgService.getReportingPeriodById(clientId, id);
    if (!period) return res.status(404).json({ error: "Not found" });

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetReportingPeriod",
      entity: "ReportingPeriod",
      entityId: id,
    });

    res.json(period.get ? period.get({ plain: true }) : period);
  } catch (err) {
    next(err);
  }
}

async function getMetricById(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const metricId = req.params.metricId;

    const metric = await esgService.getMetricById(clientId, metricId);
    if (!metric) return res.status(404).json({ error: "Not found" });

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetMetric",
      entity: "ESGMetric",
      entityId: metricId,
    });

    res.json(metric.get ? metric.get({ plain: true }) : metric);
  } catch (err) {
    next(err);
  }
}

// Unit controller handlers
async function createUnit(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { name, symbol, description } = req.body;

    const unit = await esgService.createUnit({
      clientId,
      name,
      symbol,
      description,
    });

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "CreateUnit",
      entity: "Unit",
      entityId: unit.id,
      details: { name, symbol },
    });

    res.status(201).json(unit);
  } catch (err) {
    next(err);
  }
}

async function getUnitsByClient(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    const units = await esgService.getUnitsByClient(clientId);

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetUnits",
      entity: "Unit",
      details: { count: units.length },
    });

    res.json(units);
  } catch (err) {
    next(err);
  }
}

async function getUnitById(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;

    const unit = await esgService.getUnitById(clientId, id);
    if (!unit) return res.status(404).json({ error: "Not found" });

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetUnit",
      entity: "Unit",
      entityId: id,
    });

    res.json(unit);
  } catch (err) {
    next(err);
  }
}

async function updateUnit(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;
    const { name, symbol, description } = req.body;

    await esgService.updateUnit(clientId, id, { name, symbol, description });

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "UpdateUnit",
      entity: "Unit",
      entityId: id,
      details: { name, symbol },
    });

    res.json({ message: "Unit updated." });
  } catch (err) {
    next(err);
  }
}

async function deleteUnit(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;

    await esgService.deleteUnit(clientId, id);

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "DeleteUnit",
      entity: "Unit",
      entityId: id,
    });

    res.status(204).send();
  } catch (err) {
    next(err);
  }
}

// Handler to clone templates for a reporting period
async function cloneTemplatesForReportingPeriod(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const reportingPeriodId = req.params.id;

    const result = await esgService.cloneTemplatesForReportingPeriod(
      clientId,
      reportingPeriodId
    );

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "CloneTemplatesForPeriod",
      entity: "ReportingPeriod",
      entityId: reportingPeriodId,
      details: { message: "Templates cloned" },
    });

    res.json(result);
  } catch (err) {
    next(err);
  }
}

// Handler to get category totals for dashboard
async function getCategoryTotals(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const reportingPeriodId = req.params.reportingPeriodId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    const totals = await esgAnalytics.getCategoryTotals(
      clientId,
      reportingPeriodId
    );

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetCategoryTotals",
      entity: "ESGDashboard",
      entityId: reportingPeriodId,
      details: { count: totals.length },
    });

    res.json(totals);
  } catch (err) {
    next(err);
  }
}

// Handler to get all indicators with latest metrics for dashboard
async function getAllIndicatorsWithLatestMetrics(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const reportingPeriodId = req.params.reportingPeriodId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    const data = await esgAnalytics.getAllIndicatorsWithLatestMetrics(
      clientId,
      reportingPeriodId
    );

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetIndicatorsWithLatestMetrics",
      entity: "ESGDashboard",
      entityId: reportingPeriodId,
      details: { count: data.length },
    });

    res.json(data);
  } catch (err) {
    next(err);
  }
}

// Handler to get totals by indicator for dashboard
async function getTotalsByIndicator(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const reportingPeriodId = req.params.reportingPeriodId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    const totals = await esgAnalytics.getTotalsByIndicator(
      clientId,
      reportingPeriodId
    );

    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetTotalsByIndicator",
      entity: "ESGDashboard",
      entityId: reportingPeriodId,
      details: { count: totals.length },
    });

    res.json(totals);
  } catch (err) {
    next(err);
  }
}

const express = require("express");
const router = express.Router();
const msService = require("./ms.service");
const msAnalytics = require("./ms_analytics.service");
const auditService = require("../audit/audit.service");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const {
  supplierRiskSchema,
  trainingSchema,
  grievanceSchema,
} = require("./ms.validator");

// Reporting Period Routes
router.get("/reporting-periods", authorise(), getReportingPeriods);
router.get("/reporting-periods/:id", authorise(), getReportingPeriodById);
router.post("/reporting-periods", authorise(), createReportingPeriod);

// --- Nested Reporting Period Routes ---
router.get(
  "/reporting-periods/:reportingPeriodId/supplier-risks",
  authorise(),
  getSupplierRisksByReportingPeriod
);
router.post(
  "/reporting-periods/:reportingPeriodId/supplier-risks",
  authorise(),
  validateRequest(supplierRiskSchema),
  createSupplierRisk
);
router.delete(
  "/reporting-periods/:reportingPeriodId/supplier-risks/:id",
  authorise(),
  deleteSupplierRisk
);

router.get(
  "/reporting-periods/:reportingPeriodId/training",
  authorise(),
  getTrainingByReportingPeriod
);
router.post(
  "/reporting-periods/:reportingPeriodId/training",
  authorise(),
  validateRequest(trainingSchema),
  createTraining
);
router.delete(
  "/reporting-periods/:reportingPeriodId/training/:id",
  authorise(),
  deleteTraining
);

router.get(
  "/reporting-periods/:reportingPeriodId/grievances",
  authorise(),
  getGrievancesByReportingPeriod
);
router.post(
  "/reporting-periods/:reportingPeriodId/grievances",
  authorise(),
  validateRequest(grievanceSchema),
  createGrievance
);
router.delete(
  "/reporting-periods/:reportingPeriodId/grievances/:id",
  authorise(),
  deleteGrievance
);

// General Service Routes
router.post(
  "/supplier-risks",
  authorise(),
  validateRequest(supplierRiskSchema),
  createSupplierRisk
);
router.get(
  "/supplier-risks/:reportingPeriodId",
  authorise(),
  getSupplierRisksByReportingPeriod
);
router.delete("/supplier-risks/:id", authorise(), deleteSupplierRisk);

router.post(
  "/training",
  authorise(),
  validateRequest(trainingSchema),
  createTraining
);
router.get(
  "/training/:reportingPeriodId",
  authorise(),
  getTrainingByReportingPeriod
);
router.delete("/training/:id", authorise(), deleteTraining);

router.post(
  "/grievances",
  authorise(),
  validateRequest(grievanceSchema),
  createGrievance
);
router.get(
  "/grievances/:reportingPeriodId",
  authorise(),
  getGrievancesByReportingPeriod
);
router.delete("/grievances/:id", authorise(), deleteGrievance);

// Analytics Routes
router.get(
  "/dashboard/supplier-risk-summary",
  authorise(),
  getSupplierRiskSummary
);
router.get("/dashboard/training-stats", authorise(), getTrainingStats);
router.get("/dashboard/grievance-summary", authorise(), getGrievanceSummary);

// --- Interview and Statement Routes ---
router.get(
  "/reporting-periods/:reportingPeriodId/interview",
  authorise(),
  getInterviewResponses
);
router.post(
  "/reporting-periods/:reportingPeriodId/interview",
  authorise(),
  submitInterviewResponses
);
router.post(
  "/reporting-periods/:reportingPeriodId/statement",
  authorise(),
  generateStatement
);

// --- Handlers ---
async function getReportingPeriods(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const periods = await msService.getReportingPeriods(clientId);
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetReportingPeriods",
      entity: "MSReportingPeriod",
      details: { count: periods.length },
    });
    res.json(periods);
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
    const reportingPeriodId = req.params.id;
    const period = await msService.getReportingPeriodById(
      reportingPeriodId,
      clientId
    );
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetReportingPeriod",
      entity: "MSReportingPeriod",
      entityId: reportingPeriodId,
    });
    res.json(period && period.get ? period.get({ plain: true }) : period);
  } catch (err) {
    next(err);
  }
}

async function createReportingPeriod(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { name, startDate, endDate } = req.body;
    const newPeriod = await msService.createReportingPeriod(clientId, userId, {
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
      entity: "MSReportingPeriod",
      entityId: newPeriod.id,
      details: { name, startDate, endDate },
    });
    res
      .status(201)
      .json(newPeriod.get ? newPeriod.get({ plain: true }) : newPeriod);
  } catch (err) {
    next(err);
  }
}

async function createSupplierRisk(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { reportingPeriodId, supplierName, riskLevel, description } =
      req.body;
    const risk = await msService.createSupplierRisk(clientId, userId, {
      reportingPeriodId,
      supplierName,
      riskLevel,
      description,
    });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "CreateSupplierRisk",
      entity: "SupplierRisk",
      entityId: risk.id,
      details: { supplierName, riskLevel, reportingPeriodId },
    });
    res.status(201).json(risk.get ? risk.get({ plain: true }) : risk);
  } catch (err) {
    next(err);
  }
}

async function getSupplierRisksByReportingPeriod(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const reportingPeriodId = req.params.reportingPeriodId;
    const risks = await msService.getSupplierRisksByReportingPeriodId(
      clientId,
      reportingPeriodId
    );
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetSupplierRisksByReportingPeriod",
      entity: "SupplierRisk",
      entityId: reportingPeriodId,
      details: { count: risks.length },
    });
    res.json(risks);
  } catch (err) {
    next(err);
  }
}

async function deleteSupplierRisk(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;
    await msService.deleteSupplierRisk(clientId, id);
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "DeleteSupplierRisk",
      entity: "SupplierRisk",
      entityId: id,
    });
    res.status(204).send();
  } catch (err) {
    next(err);
  }
}

async function createTraining(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { reportingPeriodId, trainingType, participants, description } =
      req.body;
    const training = await msService.createTrainingRecord(clientId, userId, {
      reportingPeriodId,
      trainingType,
      participants,
      description,
    });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "CreateTraining",
      entity: "Training",
      entityId: training.id,
      details: { trainingType, participants, reportingPeriodId },
    });
    res
      .status(201)
      .json(training.get ? training.get({ plain: true }) : training);
  } catch (err) {
    next(err);
  }
}

async function getTrainingByReportingPeriod(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const reportingPeriodId = req.params.reportingPeriodId;
    const trainings = await msService.getTrainingRecordsByReportingPeriodId(
      clientId,
      reportingPeriodId
    );
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetTrainingByReportingPeriod",
      entity: "Training",
      entityId: reportingPeriodId,
      details: { count: trainings.length },
    });
    res.json(trainings);
  } catch (err) {
    next(err);
  }
}

async function deleteTraining(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;
    await msService.deleteTrainingRecord(clientId, id);
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "DeleteTraining",
      entity: "Training",
      entityId: id,
    });
    res.status(204).send();
  } catch (err) {
    next(err);
  }
}

async function createGrievance(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { reportingPeriodId, grievanceType, description, status } = req.body;
    const grievance = await msService.createGrievance(clientId, userId, {
      reportingPeriodId,
      grievanceType,
      description,
      status,
    });
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "CreateGrievance",
      entity: "Grievance",
      entityId: grievance.id,
      details: { grievanceType, reportingPeriodId, status },
    });
    res
      .status(201)
      .json(grievance.get ? grievance.get({ plain: true }) : grievance);
  } catch (err) {
    next(err);
  }
}

async function getGrievancesByReportingPeriod(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const reportingPeriodId = req.params.reportingPeriodId;
    const grievances = await msService.getGrievancesByReportingPeriodId(
      clientId,
      reportingPeriodId
    );
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetGrievancesByReportingPeriod",
      entity: "Grievance",
      entityId: reportingPeriodId,
      details: { count: grievances.length },
    });
    res.json(grievances);
  } catch (err) {
    next(err);
  }
}

async function deleteGrievance(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;
    await msService.deleteGrievance(clientId, id);
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "DeleteGrievance",
      entity: "Grievance",
      entityId: id,
    });
    res.status(204).send();
  } catch (err) {
    next(err);
  }
}

// --- Analytics Handlers ---
async function getSupplierRiskSummary(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const summary = await msAnalytics.getSupplierRiskSummary(clientId);
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetSupplierRiskSummary",
      entity: "MSDashboard",
      entityId: "AllPeriods",
      details: { count: Array.isArray(summary) ? summary.length : undefined },
    });
    res.json(summary);
  } catch (err) {
    next(err);
  }
}

async function getTrainingStats(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const stats = await msAnalytics.getTrainingCompletionStats(clientId);
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetTrainingStats",
      entity: "MSDashboard",
      entityId: "AllPeriods",
      details: { count: Array.isArray(stats) ? stats.length : undefined },
    });
    res.json(stats);
  } catch (err) {
    next(err);
  }
}

async function getGrievanceSummary(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const summary = await msAnalytics.getGrievanceSummary(clientId);
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetGrievanceSummary",
      entity: "MSDashboard",
      entityId: "AllPeriods",
      details: { count: Array.isArray(summary) ? summary.length : undefined },
    });
    res.json(summary);
  } catch (err) {
    next(err);
  }
}

// --- Interview and Statement Handlers ---
async function getInterviewResponses(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const reportingPeriodId = req.params.reportingPeriodId;
    const responses = await msService.getInterviewResponses(
      reportingPeriodId,
      clientId
    );
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GetInterviewResponses",
      entity: "MSInterview",
      entityId: reportingPeriodId,
      details: {
        count: Array.isArray(responses) ? responses.length : undefined,
      },
    });
    res.json(responses);
  } catch (err) {
    next(err);
  }
}

async function submitInterviewResponses(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const reportingPeriodId = req.params.reportingPeriodId;
    const data = req.body;
    const result = await msService.submitInterviewResponses(
      clientId,
      userId,
      reportingPeriodId,
      data
    );
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "SubmitInterviewResponses",
      entity: "MSInterview",
      entityId: reportingPeriodId,
      details: { count: Array.isArray(data) ? data.length : 1 },
    });
    res.json(result);
  } catch (err) {
    next(err);
  }
}

async function generateStatement(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const reportingPeriodId = req.params.reportingPeriodId;
    const result = await msService.generateStatement(
      reportingPeriodId,
      clientId
    );
    await auditService.logEvent({
      clientId,
      userId,
      ip,
      device,
      action: "GenerateStatement",
      entity: "MSStatement",
      entityId: reportingPeriodId,
    });
    res.json(result);
  } catch (err) {
    next(err);
  }
}
module.exports = router;

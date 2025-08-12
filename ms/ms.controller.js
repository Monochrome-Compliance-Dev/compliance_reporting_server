const {
  logCreateAudit,
  logReadAudit,
  logUpdateAudit,
  logDeleteAudit,
} = require("../audit/auditHelpers");
const express = require("express");
const router = express.Router();
const msService = require("./ms.service");
const msAnalytics = require("./ms_analytics.service");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const {
  msSupplierRiskSchema,
  msTrainingSchema,
  msGrievanceSchema,
} = require("./ms.validator");

// Reporting Period Routes
router.get("/reporting-periods", authorise(), getReportingPeriods);
router.get("/reporting-periods/:id", authorise(), getReportingPeriodById);
router.post("/reporting-periods", authorise(), createReportingPeriod);

// General Service Routes
router.post(
  "/supplier-risks",
  authorise(),
  validateRequest(msSupplierRiskSchema),
  createSupplierRisk
);
router.put("/supplier-risks/:id", authorise(), updateSupplierRisk);
router.delete("/supplier-risks/:id", authorise(), deleteSupplierRisk);

router.post(
  "/training",
  authorise(),
  validateRequest(msTrainingSchema),
  createTraining
);
router.put("/training/:id", authorise(), updateTraining);
router.delete("/training/:id", authorise(), deleteTraining);

router.post(
  "/grievances",
  authorise(),
  validateRequest(msGrievanceSchema),
  createGrievance
);
router.put("/grievances/:id", authorise(), updateGrievance);
router.delete("/grievances/:id", authorise(), deleteGrievance);

// General GET routes for records
router.get("/training", authorise(), getTraining);
router.get("/grievances", authorise(), getGrievances);
router.get("/supplier-risks", authorise(), getSupplierRisks);

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
    await logReadAudit({
      entity: "MSReportingPeriod",
      clientId,
      userId,
      req,
      result: periods,
      details: { count: periods.length },
      action: "Read",
      ip,
      device,
    });
    res
      .status(200)
      .json({
        status: "success",
        data: periods.map((p) => (p.get ? p.get({ plain: true }) : p)),
      });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
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
      clientId,
      reportingPeriodId
    );
    await logReadAudit({
      entity: "MSReportingPeriod",
      clientId,
      userId,
      req,
      result: period,
      entityId: reportingPeriodId,
      action: "Read",
      ip,
      device,
    });
    if (!period) {
      return res.status(404).json({ status: "error", message: "Not found" });
    }
    res
      .status(200)
      .json({
        status: "success",
        data: period.get ? period.get({ plain: true }) : period,
      });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
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
    await logCreateAudit({
      entity: "MSReportingPeriod",
      clientId,
      userId,
      req,
      entityId: newPeriod.id,
      reqBody: { name, startDate, endDate },
      action: "Create",
      details: { name, startDate, endDate },
      result: newPeriod,
      ip,
      device,
    });
    res
      .status(201)
      .json({
        status: "success",
        data: newPeriod.get ? newPeriod.get({ plain: true }) : newPeriod,
      });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

async function createSupplierRisk(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { supplierName, riskLevel, description } = req.body;
    const risk = await msService.createSupplierRisk(clientId, userId, {
      supplierName,
      riskLevel,
      description,
    });
    await logCreateAudit({
      entity: "MSSupplierRisk",
      clientId,
      userId,
      req,
      entityId: risk.id,
      reqBody: { supplierName, riskLevel, description },
      action: "Create",
      details: { supplierName, riskLevel },
      result: risk,
      ip,
      device,
    });
    res
      .status(201)
      .json({
        status: "success",
        data: risk.get ? risk.get({ plain: true }) : risk,
      });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

async function updateSupplierRisk(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;
    // Fetch and update via service
    const before = await msService.getSupplierRiskById(clientId, id);
    if (!before) {
      logger.logEvent({
        message: "SupplierRisk not found",
        statusCode: 404,
        timestamp: new Date().toISOString(),
      });
      return res.status(404).json({ status: "error", message: "Not found" });
    }
    const { name, risk, country, reviewed } = req.body;
    const beforeData = before.get({ plain: true });
    // Update via service
    const after = await msService.updateSupplierRiskById(clientId, id, {
      name,
      risk,
      country,
      reviewed,
      updatedBy: userId,
    });
    const afterData = after ? after.get({ plain: true }) : null;
    await logUpdateAudit({
      entity: "MSSupplierRisk",
      clientId,
      userId,
      reqBody: { name, risk, country, reviewed, updatedBy: userId },
      req,
      action: "Update",
      details: { name, risk, country, reviewed },
      before: beforeData,
      after: afterData,
      entityId: id,
      ip,
      device,
    });
    res.status(200).json({ status: "success", data: afterData });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

async function deleteSupplierRisk(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;
    // Fetch and delete via service
    const before = await msService.getSupplierRiskById(clientId, id);
    if (!before) {
      logger.logEvent({
        message: "SupplierRisk not found",
        statusCode: 404,
        timestamp: new Date().toISOString(),
      });
      return res.status(404).json({ status: "error", message: "Not found" });
    }
    const beforeData = before.get({ plain: true });
    const deleted = await msService.deleteSupplierRiskById(clientId, id);
    await logDeleteAudit({
      entity: "MSSupplierRisk",
      clientId,
      userId,
      req,
      action: "Delete",
      before: beforeData,
      entityId: id,
      ip,
      device,
    });
    res.status(204).send();
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

async function createTraining(req, res, next) {
  console.log("Creating training record", req.body, req.auth);
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const training = await msService.createTraining(clientId, userId, req.body);
    await logCreateAudit({
      entity: "MSTraining",
      clientId,
      userId,
      req,
      entityId: training.id,
      reqBody: req.body,
      result: training,
      action: "Create",
      ip,
      device,
    });
    res
      .status(201)
      .json({
        status: "success",
        data: training.get ? training.get({ plain: true }) : training,
      });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

async function updateTraining(req, res, next) {
  console.log("updateTraining: ", req.body, req.auth);
  try {
    const id = req.params.id;
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    // Fetch and update via service
    const before = await msService.getTrainingById(clientId, id);
    if (!before) {
      logger.logEvent({
        message: "Training not found",
        statusCode: 404,
        timestamp: new Date().toISOString(),
      });
      return res.status(404).json({ status: "error", message: "Not found" });
    }
    const beforeData = before.get({ plain: true });
    const after = await msService.updateTrainingById(clientId, id, req.body);
    const afterData = after ? after.get({ plain: true }) : null;

    await logUpdateAudit({
      entity: "MSTraining",
      clientId,
      userId,
      reqBody: req.body,
      req,
      action: "Update",
      before: beforeData,
      after: afterData,
      entityId: id,
      ip,
      device,
    });
    res.status(200).json({ status: "success", data: afterData });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

async function deleteTraining(req, res, next) {
  try {
    const id = req.params.id;
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    // Fetch and delete via service
    const before = await msService.getTrainingById(clientId, id);
    if (!before) {
      logger.logEvent({
        message: "Training not found",
        statusCode: 404,
        timestamp: new Date().toISOString(),
      });
      return res.status(404).json({ status: "error", message: "Not found" });
    }
    const beforeData = before.get({ plain: true });
    const deleted = await msService.deleteTrainingById(clientId, id);
    await logDeleteAudit({
      entity: "MSTraining",
      clientId,
      userId,
      req,
      action: "Delete",
      before: beforeData,
      entityId: id,
      ip,
      device,
    });
    res.status(204).send();
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

async function createGrievance(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const { grievanceType, description, status } = req.body;
    const grievanceRecord = await msService.createGrievance(clientId, userId, {
      grievanceType,
      description,
      status,
    });
    await logCreateAudit({
      entity: "MSGrievance",
      clientId,
      userId,
      req,
      entityId: grievanceRecord.id,
      reqBody: { grievanceType, description, status },
      action: "Create",
      details: { grievanceType, status },
      result: grievanceRecord,
      ip,
      device,
    });
    res
      .status(201)
      .json({
        status: "success",
        data: grievanceRecord.get
          ? grievanceRecord.get({ plain: true })
          : grievanceRecord,
      });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

async function updateGrievance(req, res, next) {
  try {
    const id = req.params.id;
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    // Fetch and update via service
    const before = await msService.getGrievanceById(clientId, id);
    if (!before) {
      logger.logEvent({
        message: "Grievance not found",
        statusCode: 404,
        timestamp: new Date().toISOString(),
      });
      return res.status(404).json({ status: "error", message: "Not found" });
    }
    const beforeData = before.get({ plain: true });
    const after = await msService.updateGrievanceById(clientId, id, {
      description: req.body.description,
      status: req.body.status,
      updatedBy: userId,
    });
    const afterData = after ? after.get({ plain: true }) : null;
    await logUpdateAudit({
      entity: "MSGrievance",
      clientId,
      userId,
      reqBody: {
        description: req.body.description,
        status: req.body.status,
        updatedBy: userId,
      },
      req,
      action: "Update",
      details: { description: req.body.description, status: req.body.status },
      before: beforeData,
      after: afterData,
      entityId: id,
      ip,
      device,
    });
    res.status(200).json({ status: "success", data: afterData });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

async function deleteGrievance(req, res, next) {
  try {
    const id = req.params.id;
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    // Fetch and delete via service
    const before = await msService.getGrievanceById(clientId, id);
    if (!before) {
      logger.logEvent({
        message: "Grievance not found",
        statusCode: 404,
        timestamp: new Date().toISOString(),
      });
      return res.status(404).json({ status: "error", message: "Not found" });
    }
    const beforeData = before.get({ plain: true });
    const deleted = await msService.deleteGrievanceById(clientId, id);
    await logDeleteAudit({
      entity: "MSGrievance",
      clientId,
      userId,
      req,
      action: "Delete",
      before: beforeData,
      entityId: id,
      ip,
      device,
    });
    res.status(204).send();
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

async function getTraining(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const records = await msService.getTraining(
      clientId,
      req.query.startDate,
      req.query.endDate
    );
    await logReadAudit({
      entity: "MSTraining",
      clientId,
      userId,
      req,
      result: records,
      action: "Read",
      ip,
      device,
    });
    res
      .status(200)
      .json({
        status: "success",
        data: records.map((r) => (r.get ? r.get({ plain: true }) : r)),
      });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

// New handler: getGrievances
async function getGrievances(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const records = await msService.getGrievances(clientId);
    await logReadAudit({
      entity: "MSGrievance",
      clientId,
      userId,
      req,
      result: records,
      details: { count: records.length },
      action: "Read",
      ip,
      device,
    });
    res
      .status(200)
      .json({
        status: "success",
        data: records.map((r) => (r.get ? r.get({ plain: true }) : r)),
      });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

// New handler: getSupplierRisks
async function getSupplierRisks(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const records = await msService.getSupplierRisks(clientId);
    await logReadAudit({
      entity: "MSSupplierRisk",
      clientId,
      userId,
      req,
      result: records,
      details: { count: records.length },
      action: "Read",
      ip,
      device,
    });
    res
      .status(200)
      .json({
        status: "success",
        data: records.map((r) => (r.get ? r.get({ plain: true }) : r)),
      });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

// --- Analytics Handlers ---
async function getSupplierRiskSummary(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    let { reportingPeriodId } = req.query;
    let startDate, endDate;
    if (reportingPeriodId) {
      [startDate, endDate] = reportingPeriodId.split("::");
    }
    console.log("controller startDate, endDate :", startDate, endDate);
    const summary = await msAnalytics.getSupplierRiskSummary(clientId, {
      startDate,
      endDate,
    });
    await logReadAudit({
      entity: "MSDashboard",
      clientId,
      userId,
      req,
      result: summary,
      entityId: "AllPeriods",
      details: { count: Array.isArray(summary) ? summary.length : undefined },
      action: "Read",
      ip,
      device,
    });
    console.log("controller response: ", summary);
    res.status(200).json({ status: "success", data: summary });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

async function getTrainingStats(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    let { reportingPeriodId } = req.query;
    let startDate, endDate;
    if (reportingPeriodId) {
      [startDate, endDate] = reportingPeriodId.split("::");
    }
    const stats = await msAnalytics.getTrainingCompletionStats(clientId, {
      startDate,
      endDate,
    });
    await logReadAudit({
      entity: "MSDashboard",
      clientId,
      userId,
      req,
      result: stats,
      entityId: "AllPeriods",
      details: { count: Array.isArray(stats) ? stats.length : undefined },
      action: "Read",
      ip,
      device,
    });
    res.status(200).json({ status: "success", data: stats });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}

async function getGrievanceSummary(req, res, next) {
  try {
    const clientId = req.auth.clientId;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    let { reportingPeriodId } = req.query;
    let startDate, endDate;
    if (reportingPeriodId) {
      [startDate, endDate] = reportingPeriodId.split("::");
    }
    const summary = await msAnalytics.getGrievanceSummary(clientId, {
      startDate,
      endDate,
    });
    await logReadAudit({
      entity: "MSDashboard",
      clientId,
      userId,
      req,
      result: summary,
      entityId: "AllPeriods",
      details: { count: Array.isArray(summary) ? summary.length : undefined },
      action: "Read",
      ip,
      device,
    });
    res.status(200).json({ status: "success", data: summary });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
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
      clientId,
      reportingPeriodId
    );
    await logReadAudit({
      entity: "MSInterview",
      clientId,
      userId,
      req,
      result: responses,
      entityId: reportingPeriodId,
      details: {
        count: Array.isArray(responses) ? responses.length : undefined,
      },
      action: "Read",
      ip,
      device,
    });
    res
      .status(200)
      .json({
        status: "success",
        data: responses.map((r) => (r.get ? r.get({ plain: true }) : r)),
      });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
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
    await logCreateAudit({
      entity: "MSInterview",
      clientId,
      userId,
      req,
      entityId: reportingPeriodId,
      reqBody: data,
      details: { count: Array.isArray(data) ? data.length : 1 },
      action: "Create",
      result,
      ip,
      device,
    });
    res
      .status(201)
      .json({
        status: "success",
        data: result.map((r) => (r.get ? r.get({ plain: true }) : r)),
      });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
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
      clientId,
      reportingPeriodId
    );
    await logReadAudit({
      entity: "MSStatement",
      clientId,
      userId,
      req,
      result,
      entityId: reportingPeriodId,
      action: "Read",
      ip,
      device,
    });
    res.status(200).json({ status: "success", data: result });
  } catch (err) {
    logger.logEvent({
      message: err.message,
      stack: err.stack,
      statusCode: 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
}
module.exports = router;

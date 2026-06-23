const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const {
  getRequestMeta,
  badRequest,
  notFound,
  success,
} = require("@/helpers/controllerHelpers");
const runService = require("./run.service");

async function listRuns(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");

    const profileId = req.query.profileId || null;
    const items = await runService.listRuns({ customerId, profileId });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubListRuns",
      entity: "DataHubRun",
      entityId: null,
      details: {
        profileId,
        count: Array.isArray(items) ? items.length : 0,
      },
    });

    return success(res, { items });
  } catch (error) {
    logger?.logEvent?.("error", "Error listing Data Hub runs", {
      action: "DataHubListRuns",
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function createRun(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");

    const { profileId, label, name, description, meta } = req.body || {};
    const run = await runService.createRun({
      customerId,
      profileId,
      label,
      name,
      description,
      meta,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubCreateRun",
      entity: "DataHubRun",
      entityId: run.id,
      details: {
        profileId: run.profileId || null,
        label: run.label || null,
        status: run.status || null,
        currentStep: run.currentStep || null,
      },
    });

    return success(res, run, 201);
  } catch (error) {
    logger?.logEvent?.("error", "Error creating Data Hub run", {
      action: "DataHubCreateRun",
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getRun(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const runId = req.params.runId;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!runId) return badRequest(res, "runId missing");

    const run = await runService.getRun({ customerId, runId });
    if (!run) {
      return notFound(res, "Data Hub run not found");
    }

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubGetRun",
      entity: "DataHubRun",
      entityId: runId,
      details: { exists: true },
    });

    return success(res, run);
  } catch (error) {
    logger?.logEvent?.("error", "Error getting Data Hub run", {
      action: "DataHubGetRun",
      customerId,
      userId,
      runId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function updateRun(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const runId = req.params.runId;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!runId) return badRequest(res, "runId missing");

    const {
      label,
      name,
      description,
      status,
      currentStep,
      detectedCoverage,
      metricsSnapshot,
      meta,
    } = req.body || {};

    const run = await runService.updateRun({
      customerId,
      runId,
      label,
      name,
      description,
      status,
      currentStep,
      detectedCoverage,
      metricsSnapshot,
      meta,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubUpdateRun",
      entity: "DataHubRun",
      entityId: runId,
      details: {
        status: run.status || null,
        currentStep: run.currentStep || null,
      },
    });

    return success(res, run);
  } catch (error) {
    logger?.logEvent?.("error", "Error updating Data Hub run", {
      action: "DataHubUpdateRun",
      customerId,
      userId,
      runId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function deleteRun(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const runId = req.params.runId;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!runId) return badRequest(res, "runId missing");

    const result = await runService.deleteRun({ customerId, runId, userId });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubDeleteRun",
      entity: "DataHubRun",
      entityId: runId,
      details: { ok: result.ok === true },
    });

    return success(res, result);
  } catch (error) {
    logger?.logEvent?.("error", "Error deleting Data Hub run", {
      action: "DataHubDeleteRun",
      customerId,
      userId,
      runId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

module.exports = {
  listRuns,
  createRun,
  getRun,
  updateRun,
  deleteRun,
};

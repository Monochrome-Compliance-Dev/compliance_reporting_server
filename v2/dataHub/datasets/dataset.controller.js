const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const {
  getRequestMeta,
  badRequest,
  notFound,
  success,
} = require("@/helpers/controllerHelpers");
const datasetService = require("./dataset.service");

async function listDatasets(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const { runId } = req.params;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!runId) return badRequest(res, "runId missing");

    const role = req.query.role || null;
    const items = await datasetService.listDatasets({
      customerId,
      runId,
      role,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubListDatasets",
      entity: "DataHubDataset",
      entityId: null,
      details: {
        runId,
        role,
        count: Array.isArray(items) ? items.length : 0,
      },
    });

    return success(res, { items });
  } catch (error) {
    logger?.logEvent?.("error", "Error listing Data Hub datasets", {
      action: "DataHubListDatasets",
      customerId,
      userId,
      runId,
      error: error.message,
    });
    return next(error);
  }
}

async function createDataset(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const { runId } = req.params;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!runId) return badRequest(res, "runId missing");

    const { profileId, sourceType, sourceName, meta } = req.body || {};
    const role = String(req.body?.role || req.query?.role || "").trim();
    const file = req.file;

    if (!file || !file.buffer) return badRequest(res, "File is required");
    if (!role) return badRequest(res, "role is required");

    const dataset = await datasetService.createDataset({
      customerId,
      runId,
      profileId,
      role,
      sourceType,
      sourceName,
      meta,
      fileName: file.originalname || null,
      fileSize: file.size || null,
      mimeType: file.mimetype || null,
      buffer: file.buffer,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubCreateDataset",
      entity: "DataHubDataset",
      entityId: dataset.id,
      details: {
        runId,
        role: dataset.role,
        rowsCount: dataset.rowsCount,
      },
    });

    return success(res, dataset, 201);
  } catch (error) {
    logger?.logEvent?.("error", "Error creating Data Hub dataset", {
      action: "DataHubCreateDataset",
      customerId,
      userId,
      runId,
      error: error.message,
    });
    return next(error);
  }
}

async function getDataset(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const { runId, datasetId } = req.params;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!runId) return badRequest(res, "runId missing");
    if (!datasetId) return badRequest(res, "datasetId missing");

    const dataset = await datasetService.getDataset({
      customerId,
      runId,
      datasetId,
    });

    if (!dataset) {
      return notFound(res, "Data Hub dataset not found");
    }

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubGetDataset",
      entity: "DataHubDataset",
      entityId: datasetId,
      details: { runId },
    });

    return success(res, dataset);
  } catch (error) {
    logger?.logEvent?.("error", "Error getting Data Hub dataset", {
      action: "DataHubGetDataset",
      customerId,
      userId,
      runId,
      datasetId,
      error: error.message,
    });
    return next(error);
  }
}

async function getDatasetSample(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const { runId } = req.params;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!runId) return badRequest(res, "runId missing");

    const sample = await datasetService.getDatasetSample({
      customerId,
      runId,
      datasetId: req.query.datasetId || null,
      role: req.query.role || null,
      limit: req.query.limit,
      offset: req.query.offset,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubGetDatasetSample",
      entity: "DataHubDataset",
      entityId: sample?.dataset?.id || req.query.datasetId || null,
      details: {
        runId,
        role: req.query.role || null,
        returnedRows: Array.isArray(sample?.rows) ? sample.rows.length : 0,
        total: sample?.total || 0,
      },
    });

    return success(res, sample);
  } catch (error) {
    logger?.logEvent?.("error", "Error getting Data Hub dataset sample", {
      action: "DataHubGetDatasetSample",
      customerId,
      userId,
      runId,
      datasetId: req.query.datasetId || null,
      role: req.query.role || null,
      error: error.message,
    });
    return next(error);
  }
}

async function deleteDataset(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const { runId, datasetId } = req.params;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!runId) return badRequest(res, "runId missing");
    if (!datasetId) return badRequest(res, "datasetId missing");

    const result = await datasetService.deleteDataset({
      customerId,
      runId,
      datasetId,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubDeleteDataset",
      entity: "DataHubDataset",
      entityId: datasetId,
      details: {
        runId,
        ok: result.ok === true,
      },
    });

    return success(res, result);
  } catch (error) {
    logger?.logEvent?.("error", "Error deleting Data Hub dataset", {
      action: "DataHubDeleteDataset",
      customerId,
      userId,
      runId,
      datasetId,
      error: error.message,
    });
    return next(error);
  }
}

module.exports = {
  listDatasets,
  createDataset,
  getDataset,
  getDatasetSample,
  deleteDataset,
};

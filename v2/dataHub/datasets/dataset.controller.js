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

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    const profileId = req.query.profileId || req.body?.profileId;
    if (!profileId) return badRequest(res, "profileId missing");

    const datasetType = req.query.datasetType;
    const items = await datasetService.listDatasets({
      customerId,
      profileId,
      datasetType,
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
        profileId,
        datasetType,
        count: Array.isArray(items) ? items.length : 0,
      },
    });

    return success(res, { items });
  } catch (error) {
    logger?.logEvent?.("error", "Error listing Data Hub datasets", {
      action: "DataHubListDatasets",
      customerId,
      userId,
      profileId,
      error: error.message,
    });
    return next(error);
  }
}

async function createDataset(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");

    const { profileId, sourceType, sourceName, meta, datasetType } =
      req.body || {};
    if (!profileId) return badRequest(res, "profileId missing");
    const file = req.file;

    if (!file || !file.buffer) return badRequest(res, "File is required");
    if (!datasetType) return badRequest(res, "datasetType is required");

    const dataset = await datasetService.createDataset({
      customerId,
      profileId,
      datasetType,
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
        profileId,
        datasetType: dataset.datasetType,
        rowsCount: dataset.rowsCount,
      },
    });

    return success(res, dataset, 201);
  } catch (error) {
    logger?.logEvent?.("error", "Error creating Data Hub dataset", {
      action: "DataHubCreateDataset",
      customerId,
      userId,
      profileId,
      error: error.message,
    });
    return next(error);
  }
}

async function getDataset(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const { id } = req.params;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!id) return badRequest(res, "id missing");
    const profileId = req.query.profileId || req.body?.profileId;
    if (!profileId) return badRequest(res, "profileId missing");

    const dataset = await datasetService.getDataset({
      customerId,
      profileId,
      id,
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
      entityId: id,
      details: { profileId },
    });

    return success(res, dataset);
  } catch (error) {
    logger?.logEvent?.("error", "Error getting Data Hub dataset", {
      action: "DataHubGetDataset",
      customerId,
      userId,
      id,
      profileId,
      error: error.message,
    });
    return next(error);
  }
}

async function getDatasetSample(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const { id } = req.params;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!id) return badRequest(res, "id missing");
    const profileId = req.query.profileId || req.body?.profileId;
    if (!profileId) return badRequest(res, "profileId missing");

    const sample = await datasetService.getDatasetSample({
      customerId,
      profileId,
      id,
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
      entityId: sample?.dataset?.id || id,
      details: {
        profileId,
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
      id,
      profileId,
      error: error.message,
    });
    return next(error);
  }
}

async function deleteDataset(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const { id } = req.params;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!id) return badRequest(res, "id missing");
    const profileId = req.query.profileId || req.body?.profileId;
    if (!profileId) return badRequest(res, "profileId missing");

    const result = await datasetService.deleteDataset({
      customerId,
      profileId,
      id,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubDeleteDataset",
      entity: "DataHubDataset",
      entityId: id,
      details: {
        profileId,
        ok: result.ok === true,
      },
    });

    return success(res, result);
  } catch (error) {
    logger?.logEvent?.("error", "Error deleting Data Hub dataset", {
      action: "DataHubDeleteDataset",
      customerId,
      userId,
      id,
      profileId,
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

const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const {
  getRequestMeta,
  badRequest,
  success,
} = require("@/helpers/controllerHelpers");
const mapService = require("./map.service");

async function getDatasetMap(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const { id } = req.params;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!id) return badRequest(res, "id missing");

    const profileId = req.query.profileId || req.body?.profileId;
    if (!profileId) return badRequest(res, "profileId missing");

    const datasetMap = await mapService.getDatasetMap({
      customerId,
      profileId,
      datasetId: id,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubGetDatasetMap",
      entity: "DataHubDatasetMap",
      entityId: datasetMap?.id || null,
      details: {
        profileId,
        datasetId: id,
        mappedCount: datasetMap?.mappedCount || 0,
        recommendedCount: datasetMap?.recommendedCount || 0,
      },
    });

    return success(res, datasetMap);
  } catch (error) {
    logger?.logEvent?.("error", "Error getting Data Hub dataset map", {
      action: "DataHubGetDatasetMap",
      customerId,
      userId,
      profileId: req.query.profileId || req.body?.profileId,
      datasetId: id,
      error: error.message,
    });
    return next(error);
  }
}

async function saveDatasetMap(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const { id } = req.params;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!id) return badRequest(res, "id missing");

    const { profileId, fieldMapping, recommendedCount, mappingStatus, meta } =
      req.body || {};

    if (!profileId) return badRequest(res, "profileId missing");
    if (!fieldMapping || typeof fieldMapping !== "object") {
      return badRequest(res, "fieldMapping is required");
    }

    const datasetMap = await mapService.upsertDatasetMap({
      customerId,
      profileId,
      datasetId: id,
      fieldMapping,
      recommendedCount,
      mappingStatus,
      meta,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubSaveDatasetMap",
      entity: "DataHubDatasetMap",
      entityId: datasetMap.id,
      details: {
        profileId,
        datasetId: id,
        mappedCount: datasetMap.mappedCount,
        recommendedCount: datasetMap.recommendedCount,
        mappingStatus: datasetMap.mappingStatus,
      },
    });

    return success(res, datasetMap);
  } catch (error) {
    logger?.logEvent?.("error", "Error saving Data Hub dataset map", {
      action: "DataHubSaveDatasetMap",
      customerId,
      userId,
      profileId: req.body?.profileId,
      datasetId: id,
      error: error.message,
    });
    return next(error);
  }
}

async function listCompatibleMaps(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");

    const profileId = req.query.profileId;
    const datasetType = req.query.datasetType;

    if (!profileId) return badRequest(res, "profileId missing");
    if (!datasetType) return badRequest(res, "datasetType missing");

    const result = await mapService.listCompatibleMaps({
      customerId,
      profileId,
      datasetType,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubListCompatibleMaps",
      entity: "DataHubDatasetMap",
      entityId: null,
      details: {
        profileId,
        datasetType,
        count: Array.isArray(result?.items) ? result.items.length : 0,
      },
    });

    return success(res, result);
  } catch (error) {
    return next(error);
  }
}

async function importDatasetMap(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const { id } = req.params;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!id) return badRequest(res, "id missing");

    const { sourceDatasetId, profileId } = req.body || {};

    if (!sourceDatasetId) return badRequest(res, "sourceDatasetId missing");
    if (!profileId) return badRequest(res, "profileId missing");

    const result = await mapService.importDatasetMap({
      customerId,
      targetDatasetId: id,
      sourceDatasetId,
      profileId,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubImportDatasetMap",
      entity: "DataHubDatasetMap",
      entityId: id,
      details: {
        profileId,
        sourceDatasetId,
      },
    });

    return success(res, result);
  } catch (error) {
    return next(error);
  }
}

module.exports = {
  getDatasetMap,
  saveDatasetMap,
  listCompatibleMaps,
  importDatasetMap,
};

const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const {
  getRequestMeta,
  badRequest,
  success,
} = require("@/helpers/controllerHelpers");
const publishingService = require("./publishing.service");

async function publishDataset(req, res, next) {
  const { customerId, userId, ip, device } = getRequestMeta(req);
  const { id } = req.params;

  try {
    if (!customerId) return badRequest(res, "Customer ID missing");
    if (!id) return badRequest(res, "id missing");

    const profileId = req.body?.profileId || req.query?.profileId;
    if (!profileId) return badRequest(res, "profileId missing");

    const result = await publishingService.publishDataset({
      customerId,
      profileId,
      datasetId: id,
      userId,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DataHubPublishDataset",
      entity: "DataHubDataset",
      entityId: id,
      details: {
        profileId,
        datasetType: result?.datasetType,
        publishedCount: result?.publishedCount || 0,
        mapId: result?.mapId || null,
      },
    });

    return success(res, result);
  } catch (error) {
    logger?.logEvent?.("error", "Error publishing Data Hub dataset", {
      action: "DataHubPublishDataset",
      customerId,
      userId,
      id,
      profileId: req.body?.profileId || req.query?.profileId,
      error: error.message,
    });
    return next(error);
  }
}

module.exports = {
  publishDataset,
};

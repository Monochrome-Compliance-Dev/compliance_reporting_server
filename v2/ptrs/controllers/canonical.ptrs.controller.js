const auditService = require("@/audit/audit.service");
const ptrsService = require("@/v2/ptrs/services/ptrs.service");
const canonicalService = require("@/v2/ptrs/services/canonical.ptrs.service");

async function requirePtrs(req) {
  const customerId = req.effectiveCustomerId;
  if (!customerId) {
    const error = new Error("Customer ID missing");
    error.statusCode = 400;
    throw error;
  }
  const ptrs = await ptrsService.getPtrs({
    customerId,
    ptrsId: req.params.id,
  });
  if (!ptrs) {
    const error = new Error("Ptrs not found");
    error.statusCode = 404;
    throw error;
  }
  return { customerId, ptrs };
}

async function materializeRevision(req, res, next) {
  try {
    const { customerId, ptrs } = await requirePtrs(req);
    const profileId =
      req.body?.profileId || req.query?.profileId || ptrs.profileId;
    if (!profileId) {
      const error = new Error("profileId is required");
      error.statusCode = 400;
      throw error;
    }
    const result = await canonicalService.materializeCanonicalRevision({
      customerId,
      ptrsId: req.params.id,
      datasetId: req.params.datasetId,
      profileId,
      actorId: req.auth?.id || null,
      requestId: req.id || null,
    });
    await auditService.logEvent({
      customerId,
      userId: req.auth?.id,
      ip: req.ip,
      device: req.headers["user-agent"],
      action:
        result.revision.status === "building"
          ? "PtrsV2CanonicalRevisionInProgress"
          : "PtrsV2CanonicalRevisionMaterialised",
      entity: "PtrsCanonicalRevision",
      entityId: result.revision.id,
      details: {
        ptrsId: req.params.id,
        datasetId: req.params.datasetId,
        profileId,
        rowCount:
          result.revision.rowCount == null
            ? null
            : Number(result.revision.rowCount),
        reused: result.reused,
      },
    });
    return res
      .status(result.revision.status === "building" ? 202 : 200)
      .json({ status: "success", data: result });
  } catch (error) {
    return next(error);
  }
}

async function listRevisionStatus(req, res, next) {
  try {
    const { customerId, ptrs } = await requirePtrs(req);
    const profileId = req.query?.profileId || ptrs.profileId;
    if (!profileId) {
      const error = new Error("profileId is required");
      error.statusCode = 400;
      throw error;
    }
    const statuses = await canonicalService.listCanonicalRevisionStatus({
      customerId,
      ptrsId: req.params.id,
      profileId,
    });
    return res
      .status(200)
      .json({ status: "success", data: { datasets: statuses } });
  } catch (error) {
    return next(error);
  }
}

module.exports = { materializeRevision, listRevisionStatus };

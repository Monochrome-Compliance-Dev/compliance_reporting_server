const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

function toPlain(row) {
  if (!row) return null;
  return row.get ? row.get({ plain: true }) : row;
}

function normaliseRun(row) {
  const plain = toPlain(row);
  if (!plain) return null;

  return {
    ...plain,
    id: plain.id,
    runId: plain.id,
    name: plain.label || plain.name || "Untitled Data Hub Run",
    label: plain.label || plain.name || "Untitled Data Hub Run",
    status: plain.status || "draft",
    currentStep: plain.currentStep || "upload",
    detectedCoverage: plain.detectedCoverage || {},
    metricsSnapshot: plain.metricsSnapshot || {},
  };
}

function rollbackQuietly(t) {
  if (!t || t.finished) return Promise.resolve();
  return t.rollback().catch(() => {});
}

function getDataHubRunModel() {
  if (!db.DataHubRun) {
    throw new Error("DataHubRun model is not registered on db");
  }
  return db.DataHubRun;
}

async function listRuns({ customerId, profileId = null } = {}) {
  if (!customerId) throw new Error("customerId is required");

  const DataHubRun = getDataHubRunModel();
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const where = { customerId };
    if (profileId) where.profileId = profileId;

    const rows = await DataHubRun.findAll({
      where,
      order: [["createdAt", "DESC"]],
      transaction: t,
    });

    await t.commit();
    return rows.map(normaliseRun);
  } catch (err) {
    await rollbackQuietly(t);
    logger?.error?.("Failed to list Data Hub runs", {
      action: "DataHubListRuns",
      customerId,
      profileId,
      error: err.message,
    });
    throw err;
  }
}

async function createRun({
  customerId,
  profileId = null,
  label = null,
  name = null,
  description = null,
  meta = null,
  userId = null,
} = {}) {
  if (!customerId) throw new Error("customerId is required");

  const DataHubRun = getDataHubRunModel();
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const row = await DataHubRun.create(
      {
        customerId,
        profileId: profileId || null,
        label: label || name || null,
        description: description || null,
        status: "draft",
        currentStep: "upload",
        detectedCoverage: {},
        metricsSnapshot: {},
        meta: meta && typeof meta === "object" ? meta : null,
        createdBy: userId || null,
        updatedBy: userId || null,
      },
      { transaction: t },
    );

    await t.commit();
    return normaliseRun(row);
  } catch (err) {
    await rollbackQuietly(t);
    logger?.error?.("Failed to create Data Hub run", {
      action: "DataHubCreateRun",
      customerId,
      profileId,
      label: label || name || null,
      error: err.message,
    });
    throw err;
  }
}

async function getRun({ customerId, runId } = {}) {
  if (!customerId) throw new Error("customerId is required");
  if (!runId) throw new Error("runId is required");

  const DataHubRun = getDataHubRunModel();
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const row = await DataHubRun.findOne({
      where: { id: runId, customerId },
      transaction: t,
    });

    await t.commit();
    return normaliseRun(row);
  } catch (err) {
    await rollbackQuietly(t);
    logger?.error?.("Failed to get Data Hub run", {
      action: "DataHubGetRun",
      customerId,
      runId,
      error: err.message,
    });
    throw err;
  }
}

async function updateRun({
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
  userId = null,
} = {}) {
  if (!customerId) throw new Error("customerId is required");
  if (!runId) throw new Error("runId is required");

  const DataHubRun = getDataHubRunModel();
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const row = await DataHubRun.findOne({
      where: { id: runId, customerId },
      transaction: t,
    });

    if (!row) {
      const err = new Error("Data Hub run not found");
      err.statusCode = 404;
      throw err;
    }

    const patch = {};
    if (label != null || name != null) patch.label = label || name;
    if (description != null) patch.description = description;
    if (status != null) patch.status = status;
    if (currentStep != null) patch.currentStep = currentStep;
    if (detectedCoverage != null) patch.detectedCoverage = detectedCoverage;
    if (metricsSnapshot != null) patch.metricsSnapshot = metricsSnapshot;
    if (meta != null) patch.meta = meta;
    patch.updatedBy = userId || null;

    if (Object.keys(patch).length > 0) {
      await row.update(patch, { transaction: t });
    }

    await t.commit();
    return normaliseRun(row);
  } catch (err) {
    await rollbackQuietly(t);
    logger?.error?.("Failed to update Data Hub run", {
      action: "DataHubUpdateRun",
      customerId,
      runId,
      error: err.message,
    });
    throw err;
  }
}

async function deleteRun({ customerId, runId, userId = null } = {}) {
  if (!customerId) throw new Error("customerId is required");
  if (!runId) throw new Error("runId is required");

  const DataHubRun = getDataHubRunModel();
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const row = await DataHubRun.findOne({
      where: { id: runId, customerId },
      transaction: t,
    });

    if (!row) {
      const err = new Error("Data Hub run not found");
      err.statusCode = 404;
      throw err;
    }

    if (userId) {
      await row.update({ updatedBy: userId }, { transaction: t });
    }
    await row.destroy({ transaction: t });

    await t.commit();
    return { ok: true, runId };
  } catch (err) {
    await rollbackQuietly(t);
    logger?.error?.("Failed to delete Data Hub run", {
      action: "DataHubDeleteRun",
      customerId,
      runId,
      error: err.message,
    });
    throw err;
  }
}

module.exports = {
  normaliseRun,
  listRuns,
  createRun,
  getRun,
  updateRun,
  deleteRun,
};

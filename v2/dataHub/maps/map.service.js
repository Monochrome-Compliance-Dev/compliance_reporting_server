const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

function toPlain(row) {
  if (!row) return null;
  return row.get ? row.get({ plain: true }) : row;
}

function normaliseDatasetMap(row) {
  const plain = toPlain(row);
  if (!plain) return null;

  const fieldMapping =
    plain.fieldMapping && typeof plain.fieldMapping === "object"
      ? plain.fieldMapping
      : {};

  return {
    ...plain,
    id: plain.id,
    customerId: plain.customerId,
    profileId: plain.profileId,
    datasetId: plain.datasetId,
    datasetType: plain.datasetType,
    fieldMapping,
    mappingStatus: plain.mappingStatus || "draft",
    mappedCount: Number(plain.mappedCount || 0),
    recommendedCount: Number(plain.recommendedCount || 0),
    meta: plain.meta || null,
    updatedAt: plain.updatedAt || null,
  };
}

function rollbackQuietly(t) {
  if (!t || t.finished) return Promise.resolve();
  return t.rollback().catch(() => {});
}

function getDataHubDatasetModel() {
  if (!db.DataHubDataset) {
    throw new Error("DataHubDataset model is not registered on db");
  }
  return db.DataHubDataset;
}

function getDataHubDatasetMapModel() {
  if (!db.DataHubDatasetMap) {
    throw new Error("DataHubDatasetMap model is not registered on db");
  }
  return db.DataHubDatasetMap;
}

async function getDatasetForCustomer({
  customerId,
  profileId,
  datasetId,
  transaction,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!datasetId) throw new Error("datasetId is required");

  const DataHubDataset = getDataHubDatasetModel();
  const dataset = await DataHubDataset.findOne({
    where: { id: datasetId, customerId, profileId },
    transaction,
  });

  if (!dataset) {
    const err = new Error("Data Hub dataset not found");
    err.statusCode = 404;
    throw err;
  }

  return dataset;
}

function countMappedFields(fieldMapping) {
  if (!fieldMapping || typeof fieldMapping !== "object") return 0;

  return Object.values(fieldMapping).filter((value) => {
    if (!value) return false;
    if (typeof value === "string") return value.trim().length > 0;
    if (typeof value === "object") {
      return String(value.header || value.sourceHeader || "").trim().length > 0;
    }
    return false;
  }).length;
}

function normaliseFieldMapping(fieldMapping) {
  if (
    !fieldMapping ||
    typeof fieldMapping !== "object" ||
    Array.isArray(fieldMapping)
  ) {
    throw new Error("fieldMapping must be an object");
  }

  return Object.entries(fieldMapping).reduce((acc, [fieldId, source]) => {
    const key = String(fieldId || "").trim();
    if (!key) return acc;

    if (!source) {
      acc[key] = null;
      return acc;
    }

    if (typeof source === "string") {
      acc[key] = source.trim() || null;
      return acc;
    }

    if (typeof source === "object") {
      const header = String(source.header || source.sourceHeader || "").trim();
      acc[key] = header
        ? {
            ...source,
            header,
          }
        : null;
      return acc;
    }

    acc[key] = null;
    return acc;
  }, {});
}

async function getDatasetMap({ customerId, profileId, datasetId } = {}) {
  if (!customerId) throw new Error("customerId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!datasetId) throw new Error("datasetId is required");

  const DataHubDatasetMap = getDataHubDatasetMapModel();
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const dataset = await getDatasetForCustomer({
      customerId,
      profileId,
      datasetId,
      transaction: t,
    });

    const row = await DataHubDatasetMap.findOne({
      where: { customerId, profileId, datasetId },
      transaction: t,
    });

    await t.commit();

    if (row) return normaliseDatasetMap(row);

    const plainDataset = toPlain(dataset);
    return {
      id: null,
      customerId,
      profileId,
      datasetId,
      datasetType: plainDataset.datasetType,
      fieldMapping: {},
      mappingStatus: "draft",
      mappedCount: 0,
      recommendedCount: 0,
      meta: null,
      updatedAt: null,
    };
  } catch (err) {
    await rollbackQuietly(t);
    logger?.error?.("Failed to get Data Hub dataset map", {
      action: "DataHubGetDatasetMap",
      customerId,
      profileId,
      datasetId,
      error: err.message,
    });
    throw err;
  }
}

async function upsertDatasetMap({
  customerId,
  profileId,
  datasetId,
  fieldMapping,
  recommendedCount = 0,
  mappingStatus = "draft",
  meta,
  userId,
} = {}) {
  if (!customerId) throw new Error("customerId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!datasetId) throw new Error("datasetId is required");

  const normalisedFieldMapping = normaliseFieldMapping(fieldMapping);
  const mappedCount = countMappedFields(normalisedFieldMapping);
  const safeRecommendedCount = Math.max(Number(recommendedCount || 0), 0);
  const safeMappingStatus = String(mappingStatus || "draft").trim() || "draft";

  const DataHubDatasetMap = getDataHubDatasetMapModel();
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const dataset = await getDatasetForCustomer({
      customerId,
      profileId,
      datasetId,
      transaction: t,
    });
    const plainDataset = toPlain(dataset);

    let row = await DataHubDatasetMap.findOne({
      where: { customerId, profileId, datasetId },
      transaction: t,
    });

    const payload = {
      customerId,
      profileId,
      datasetId,
      datasetType: plainDataset.datasetType,
      fieldMapping: normalisedFieldMapping,
      mappingStatus: safeMappingStatus,
      mappedCount,
      recommendedCount: safeRecommendedCount,
      meta: meta && typeof meta === "object" ? meta : null,
      updatedBy: userId || null,
    };

    if (row) {
      await row.update(payload, { transaction: t });
    } else {
      row = await DataHubDatasetMap.create(
        {
          ...payload,
          createdBy: userId || null,
        },
        { transaction: t },
      );
    }

    await t.commit();
    return normaliseDatasetMap(row);
  } catch (err) {
    await rollbackQuietly(t);
    logger?.error?.("Failed to save Data Hub dataset map", {
      action: "DataHubSaveDatasetMap",
      customerId,
      profileId,
      datasetId,
      error: err.message,
    });
    throw err;
  }
}

module.exports = {
  normaliseDatasetMap,
  getDatasetMap,
  upsertDatasetMap,
};

const fs = require("fs");
const { getTransformer } = require("./transformers/transformer_loader");
const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

function toPlain(row) {
  if (!row) return null;
  return row.get ? row.get({ plain: true }) : row;
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

function getPublishedModel(transformer) {
  if (!transformer?.modelName) {
    throw new Error("transformer.modelName is required");
  }

  const model = db[transformer.modelName];
  if (!model) {
    throw new Error(`${transformer.modelName} model is not registered on db`);
  }

  return model;
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

async function getMapForDataset({
  customerId,
  profileId,
  datasetId,
  transaction,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!datasetId) throw new Error("datasetId is required");

  const DataHubDatasetMap = getDataHubDatasetMapModel();
  const datasetMap = await DataHubDatasetMap.findOne({
    where: { customerId, profileId, datasetId },
    transaction,
  });

  if (!datasetMap) {
    const err = new Error("Data Hub dataset map not found");
    err.statusCode = 404;
    throw err;
  }

  return datasetMap;
}

function normalisePublishedResult({ dataset, datasetMap, publishedCount }) {
  const plainDataset = toPlain(dataset);
  const plainMap = toPlain(datasetMap);

  return {
    id: plainDataset.id,
    customerId: plainDataset.customerId,
    profileId: plainDataset.profileId,
    datasetId: plainDataset.id,
    datasetType: plainDataset.datasetType,
    sourceName: plainDataset.sourceName || null,
    originalFileName: plainDataset.originalFileName || null,
    mapId: plainMap.id,
    mappingStatus: plainMap.mappingStatus,
    mappedCount: Number(plainMap.mappedCount || 0),
    recommendedCount: Number(plainMap.recommendedCount || 0),
    publishedCount: Number(publishedCount || 0),
    publishedAt: new Date().toISOString(),
  };
}

function hasMappedValue(value) {
  if (!value) return false;
  if (typeof value === "string") return value.trim().length > 0;
  if (typeof value === "object") {
    return String(value.header || value.sourceHeader || "").trim().length > 0;
  }
  return false;
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
    if (!hasMappedValue(source)) return acc;

    const key = String(fieldId || "").trim();
    if (!key) return acc;

    if (typeof source === "string") {
      acc[key] = source.trim();
      return acc;
    }

    acc[key] = String(source.header || source.sourceHeader || "").trim();
    return acc;
  }, {});
}

function parseCsvLine(line) {
  const values = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    const next = line[i + 1];

    if (char === '"' && inQuotes && next === '"') {
      current += '"';
      i += 1;
      continue;
    }

    if (char === '"') {
      inQuotes = !inQuotes;
      continue;
    }

    if (char === "," && !inQuotes) {
      values.push(current);
      current = "";
      continue;
    }

    current += char;
  }

  values.push(current);
  return values;
}

function parseCsvFile(storagePath) {
  if (!storagePath) throw new Error("storagePath is required");
  if (!fs.existsSync(storagePath)) {
    throw new Error(`CSV file not found at ${storagePath}`);
  }

  const content = fs.readFileSync(storagePath, "utf8").replace(/^\uFEFF/, "");
  const lines = content.split(/\r?\n/).filter((line) => line.trim().length > 0);

  if (!lines.length) return { headers: [], rows: [] };

  const headers = parseCsvLine(lines[0]);
  const rows = lines.slice(1).map((line, index) => {
    const values = parseCsvLine(line);
    const rawRecord = headers.reduce((acc, header, colIdx) => {
      acc[header] = values[colIdx] ?? "";
      return acc;
    }, {});

    return {
      sourceRowNumber: index + 2,
      rawRecord,
    };
  });

  return { headers, rows };
}

async function publishDataset({
  customerId,
  profileId,
  datasetId,
  userId,
} = {}) {
  if (!customerId) throw new Error("customerId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!datasetId) throw new Error("datasetId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const dataset = await getDatasetForCustomer({
      customerId,
      profileId,
      datasetId,
      transaction: t,
    });
    const datasetMap = await getMapForDataset({
      customerId,
      profileId,
      datasetId,
      transaction: t,
    });

    const plainDataset = toPlain(dataset);
    const plainMap = toPlain(datasetMap);
    const datasetType = plainDataset.datasetType;
    const transformer = getTransformer(datasetType);
    const PublishedModel = getPublishedModel(transformer);
    const fieldMapping = normaliseFieldMapping(plainMap.fieldMapping);

    if (!plainDataset.storagePath) {
      throw new Error("Dataset storagePath is missing");
    }

    const { rows } = parseCsvFile(plainDataset.storagePath);

    const publishRows = transformer.buildRows({
      customerId,
      profileId,
      datasetId,
      rows,
      fieldMapping,
      userId,
    });

    await PublishedModel.destroy({
      where: { customerId, profileId, datasetId },
      force: true,
      transaction: t,
    });

    if (publishRows.length) {
      await PublishedModel.bulkCreate(publishRows, {
        transaction: t,
        validate: true,
      });
    }

    await datasetMap.update(
      {
        mappingStatus: "published",
        meta: {
          ...(plainMap.meta && typeof plainMap.meta === "object"
            ? plainMap.meta
            : {}),
          publishedAt: new Date().toISOString(),
          publishedBy: userId || null,
          publishedRowCount: publishRows.length,
        },
        updatedBy: userId || null,
      },
      { transaction: t },
    );

    await dataset.update(
      {
        status: "published",
        updatedBy: userId || null,
      },
      { transaction: t },
    );

    await t.commit();

    return normalisePublishedResult({
      dataset,
      datasetMap,
      publishedCount: publishRows.length,
    });
  } catch (err) {
    await rollbackQuietly(t);
    logger?.error?.("Failed to publish Data Hub dataset", {
      action: "DataHubPublishDataset",
      customerId,
      profileId,
      datasetId,
      error: err.message,
    });
    throw err;
  }
}

module.exports = {
  publishDataset,
};

const db = require("@/db/database");
const { buildStableInputHash } = require("@/v2/ptrs/services/ptrs.service");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

function normHeaderKey(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "")
    .trim();
}

function buildMapMetaFromMappings(
  mappings,
  signature = null,
  updatedAtIso = null,
) {
  const m =
    mappings && typeof mappings === "object" && !Array.isArray(mappings)
      ? mappings
      : {};
  const sourceHeaders = Object.keys(m);
  const sourceHeadersNorm = sourceHeaders.map(normHeaderKey).filter(Boolean);

  const targets = Array.from(
    new Set(
      Object.values(m)
        .map((cfg) => {
          if (cfg == null) return null;
          if (typeof cfg === "string") return cfg;
          return cfg?.field || null;
        })
        .filter((v) => v != null && String(v).trim() !== "")
        .map((v) => String(v).trim()),
    ),
  );

  return {
    version: 1,
    sourceHeaders,
    sourceHeadersNorm,
    targets,
    updatedAt: updatedAtIso || null,
    signature: signature || null,
  };
}

function safeParseJsonObject(v) {
  if (v == null) return null;
  if (typeof v === "object" && !Array.isArray(v)) return v;
  if (typeof v === "string") {
    try {
      const parsed = JSON.parse(v);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed;
      }
    } catch (_) {
      return null;
    }
  }
  return null;
}

function extractMapMetaFromExtras(extras) {
  const obj = safeParseJsonObject(extras) || {};
  const meta = obj?.mapMeta;
  if (!meta || typeof meta !== "object") return null;
  if (meta.version !== 1) return null;
  return meta;
}

function safeParseJsonAny(v) {
  if (v == null) return null;
  if (typeof v === "string") {
    try {
      return JSON.parse(v);
    } catch {
      return v;
    }
  }
  return v;
}

function buildMaterialMapSignature({ mappings, joins, customFields }) {
  return buildStableInputHash({
    mappings: safeParseJsonAny(mappings) || null,
    joins: safeParseJsonAny(joins) || null,
    customFields: safeParseJsonAny(customFields) || null,
  });
}

function isMainDatasetRole(role) {
  const r = String(role || "")
    .trim()
    .toLowerCase();
  return r === "main" || r.startsWith("main_");
}

async function getSupportConfigForSnapshot({
  customerId,
  ptrsId,
  transaction = null,
}) {
  return db.PtrsColumnMap.findOne({
    where: { customerId, ptrsId },
    transaction,
    raw: true,
  });
}

async function buildMapInputSnapshot({
  customerId,
  ptrsId,
  profileId = null,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const supportConfig = await getSupportConfigForSnapshot({
    customerId,
    ptrsId,
    transaction,
  });

  if (!profileId) {
    throw new Error("profileId is required for map staleness snapshot");
  }

  const resolvedProfileId = profileId;

  const [fieldMapRows, columnMapUpdatedAt, datasets] = await Promise.all([
    resolvedProfileId && db.PtrsFieldMap
      ? db.PtrsFieldMap.findAll({
          where: {
            customerId,
            ptrsId,
            profileId: resolvedProfileId,
          },
          attributes: ["datasetId", "canonicalField", "updatedAt"],
          raw: true,
          transaction,
        })
      : Promise.resolve([]),
    db.PtrsColumnMap
      ? db.PtrsColumnMap.max("updatedAt", {
          where: { customerId, ptrsId },
          transaction,
        })
      : Promise.resolve(null),
    db.PtrsDataset
      ? db.PtrsDataset.findAll({
          where: { customerId, ptrsId },
          attributes: ["id", "role", "updatedAt", "createdAt"],
          order: [
            ["role", "ASC"],
            ["updatedAt", "DESC"],
          ],
          raw: true,
          transaction,
        })
      : Promise.resolve([]),
  ]);

  const supportConfigSignature = supportConfig
    ? buildMaterialMapSignature({
        mappings: supportConfig.mappings || null,
        joins: supportConfig.joins || null,
        customFields: supportConfig.customFields || null,
      })
    : null;

  const mainDatasets = Array.isArray(datasets)
    ? datasets.filter((d) => isMainDatasetRole(d?.role))
    : [];

  const fieldMapRowsByDatasetId = new Map();
  for (const row of fieldMapRows || []) {
    const datasetId = String(row?.datasetId || "").trim();
    if (!datasetId) continue;
    if (!fieldMapRowsByDatasetId.has(datasetId)) {
      fieldMapRowsByDatasetId.set(datasetId, []);
    }
    fieldMapRowsByDatasetId.get(datasetId).push(row);
  }

  const fieldMapByDataset = mainDatasets.map((dataset) => {
    const datasetId = String(dataset?.id || "").trim();
    const rows = fieldMapRowsByDatasetId.get(datasetId) || [];
    const canonicalFields = Array.from(
      new Set(
        rows
          .map((row) => String(row?.canonicalField || "").trim())
          .filter(Boolean),
      ),
    ).sort((a, b) => a.localeCompare(b));
    const updatedAtValues = rows
      .map((row) => row?.updatedAt)
      .filter(Boolean)
      .map((value) => new Date(value).getTime())
      .filter((value) => Number.isFinite(value));
    const maxUpdatedAt = updatedAtValues.length
      ? new Date(Math.max(...updatedAtValues)).toISOString()
      : null;

    return {
      datasetId,
      role: dataset?.role || null,
      fieldMapCount: rows.length,
      canonicalFields,
      maxUpdatedAt,
    };
  });

  return {
    customerId,
    ptrsId,
    profileId: resolvedProfileId,
    supportConfig: {
      id: supportConfig?.id || null,
      updatedAt: columnMapUpdatedAt || supportConfig?.updatedAt || null,
      signature: supportConfigSignature,
      hasJoins: !!supportConfig?.joins,
      hasCustomFields: !!supportConfig?.customFields,
      hasRowRules: !!supportConfig?.rowRules,
    },
    fieldMap: {
      profileId: resolvedProfileId,
      mainDatasetCount: mainDatasets.length,
      byDataset: fieldMapByDataset,
    },
    datasets: Array.isArray(datasets)
      ? datasets.map((d) => ({
          id: d.id,
          role: d.role,
          updatedAt: d.updatedAt || null,
          createdAt: d.createdAt || null,
        }))
      : [],
  };
}

async function getMapStaleness({
  customerId,
  ptrsId,
  profileId = null,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const isExternalTx = !!transaction;

  try {
    const snapshot = await buildMapInputSnapshot({
      customerId,
      ptrsId,
      profileId,
      transaction: t,
    });

    const inputHash = buildStableInputHash(snapshot);

    const previous = await db.PtrsExecutionRun.findOne({
      where: {
        customerId,
        ptrsId,
        step: "map",
        status: "success",
      },
      order: [
        ["startedAt", "DESC"],
        ["id", "DESC"],
      ],
      raw: true,
      transaction: t,
    });

    const existingMappedRowCount = await db.PtrsMappedRow.count({
      where: { customerId, ptrsId },
      transaction: t,
    });

    const previousHash = previous?.inputHash || null;
    const hasChanged = !previousHash || previousHash !== inputHash;

    const result = {
      step: "map",
      inputHash,
      previousHash,
      hasChanged,
      previousRunId: previous?.id || null,
      existingMappedRowCount: Number(existingMappedRowCount) || 0,
      snapshot,
    };

    if (!isExternalTx && !t.finished) {
      await t.commit();
    }

    return result;
  } catch (err) {
    if (!isExternalTx && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

module.exports = {
  normHeaderKey,
  buildMapMetaFromMappings,
  safeParseJsonObject,
  extractMapMetaFromExtras,
  safeParseJsonAny,
  buildMaterialMapSignature,
  buildMapInputSnapshot,
  getMapStaleness,
};

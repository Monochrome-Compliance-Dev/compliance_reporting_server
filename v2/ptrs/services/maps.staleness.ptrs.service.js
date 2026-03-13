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

  const [supportConfig, fieldMapUpdatedAt, fieldMapCount, datasets] =
    await Promise.all([
      getSupportConfigForSnapshot({
        customerId,
        ptrsId,
        transaction,
      }),
      profileId && db.PtrsFieldMap
        ? db.PtrsFieldMap.max("updatedAt", {
            where: { customerId, ptrsId, profileId },
            transaction,
          })
        : Promise.resolve(null),
      profileId && db.PtrsFieldMap
        ? db.PtrsFieldMap.count({
            where: { customerId, ptrsId, profileId },
            transaction,
          })
        : Promise.resolve(0),
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

  const resolvedProfileId = profileId || supportConfig?.profileId || null;

  const supportConfigSignature = supportConfig
    ? buildMaterialMapSignature({
        mappings: null,
        joins: supportConfig.joins || null,
        customFields: supportConfig.customFields || null,
      })
    : null;

  return {
    customerId,
    ptrsId,
    profileId: resolvedProfileId,
    supportConfig: {
      id: supportConfig?.id || null,
      updatedAt: supportConfig?.updatedAt || null,
      signature: supportConfigSignature,
      hasJoins: !!supportConfig?.joins,
      hasCustomFields: !!supportConfig?.customFields,
      hasRowRules: !!supportConfig?.rowRules,
    },
    fieldMap: {
      profileId: resolvedProfileId,
      count: Number(fieldMapCount) || 0,
      maxUpdatedAt: fieldMapUpdatedAt || null,
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

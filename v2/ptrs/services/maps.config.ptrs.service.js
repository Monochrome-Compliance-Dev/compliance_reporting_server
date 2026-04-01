const db = require("@/db/database");
const { Op } = require("sequelize");
const { safeMeta, slog, toSnake } = require("@/v2/ptrs/services/ptrs.service");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  extractMapMetaFromExtras,
  buildMaterialMapSignature,
  safeParseJsonObject,
  buildMapMetaFromMappings,
} = require("@/v2/ptrs/services/maps.staleness.ptrs.service");
const {
  PTRS_CANONICAL_CONTRACT,
} = require("@/v2/ptrs/contracts/ptrs.canonical.contract");

const REQUIRED_CANONICAL_FIELDS = Object.entries(
  PTRS_CANONICAL_CONTRACT?.fields || {},
)
  .filter(([, cfg]) => cfg?.required === true || cfg?.isRequired === true)
  .map(([key]) => toSnake(key))
  .filter(Boolean);

async function getMap({ customerId, ptrsId }) {
  const supportConfig = await getSupportConfig({ customerId, ptrsId });

  const maybeParse = (v) => {
    if (v == null || typeof v !== "string") return v;
    try {
      return JSON.parse(v);
    } catch {
      return v;
    }
  };

  if (!supportConfig) {
    return {
      customerId,
      ptrsId,
      mappings: {},
      extras: null,
      fallbacks: null,
      defaults: null,
      joins: null,
      rowRules: null,
      profileId: null,
      customFields: null,
    };
  }

  supportConfig.mappings = maybeParse(supportConfig.mappings);
  supportConfig.extras = maybeParse(supportConfig.extras);
  supportConfig.fallbacks = maybeParse(supportConfig.fallbacks);
  supportConfig.defaults = maybeParse(supportConfig.defaults);
  supportConfig.joins = maybeParse(supportConfig.joins);
  supportConfig.rowRules = maybeParse(supportConfig.rowRules);
  supportConfig.customFields = maybeParse(supportConfig.customFields);

  return supportConfig;
}

async function getSupportConfig({ customerId, ptrsId, transaction = null }) {
  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const isExternalTx = !!transaction;
  try {
    const supportConfig = await db.PtrsColumnMap.findOne({
      where: { customerId, ptrsId },
      transaction: t,
      raw: true,
    });
    slog.info(
      "PTRS v2 getSupportConfig: loaded support config",
      safeMeta({
        customerId,
        ptrsId,
        hasConfig: !!supportConfig,
        id: supportConfig?.id || null,
        hasJoins: !!(supportConfig && supportConfig.joins),
        hasCustomFields: !!(supportConfig && supportConfig.customFields),
        hasRowRules: !!(supportConfig && supportConfig.rowRules),
        joinsType:
          supportConfig && supportConfig.joins
            ? typeof supportConfig.joins
            : null,
        customFieldsType:
          supportConfig && supportConfig.customFields
            ? typeof supportConfig.customFields
            : null,
      }),
    );
    if (!isExternalTx && !t.finished) {
      await t.commit();
    }
    return supportConfig || null;
  } catch (err) {
    if (!isExternalTx && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function listCompatibleMaps({ customerId, profileId = null }) {
  if (!customerId) throw new Error("customerId is required");

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const supportConfigs = await db.PtrsColumnMap.findAll({
      where: { customerId },
      attributes: ["ptrsId", "extras"],
      raw: true,
      transaction: t,
    });

    const fieldMaps = await db.PtrsFieldMap.findAll({
      where: {
        customerId,
        ...(profileId ? { profileId } : {}),
      },
      attributes: ["ptrsId", "canonicalField", "updatedAt", "createdAt"],
      raw: true,
      transaction: t,
    });

    const ptrsIds = Array.from(
      new Set(
        [...supportConfigs, ...fieldMaps]
          .map((row) => row?.ptrsId)
          .filter(Boolean),
      ),
    );

    if (!ptrsIds.length) {
      await t.commit();
      return { items: [] };
    }

    const metaByPtrsId = new Map();
    for (const sc of supportConfigs) {
      metaByPtrsId.set(sc.ptrsId, extractMapMetaFromExtras(sc.extras));
    }

    const fieldMapStatsByPtrsId = new Map();
    for (const row of fieldMaps || []) {
      const key = String(row?.ptrsId || "");
      if (!key) continue;

      const stat = fieldMapStatsByPtrsId.get(key) || {
        mappedFieldsCount: 0,
        fieldMapUpdatedAt: null,
        fieldMapCreatedAt: null,
      };

      stat.mappedFieldsCount += 1;

      const updatedAt = row?.updatedAt || null;
      const createdAt = row?.createdAt || null;

      if (
        updatedAt &&
        (!stat.fieldMapUpdatedAt ||
          new Date(updatedAt).getTime() >
            new Date(stat.fieldMapUpdatedAt).getTime())
      ) {
        stat.fieldMapUpdatedAt = updatedAt;
      }

      if (
        createdAt &&
        (!stat.fieldMapCreatedAt ||
          new Date(createdAt).getTime() >
            new Date(stat.fieldMapCreatedAt).getTime())
      ) {
        stat.fieldMapCreatedAt = createdAt;
      }

      fieldMapStatsByPtrsId.set(key, stat);
    }

    const ptrsRows = await db.Ptrs.findAll({
      where: { customerId, id: { [Op.in]: ptrsIds } },
      order: [
        ["updatedAt", "DESC"],
        ["createdAt", "DESC"],
      ],
      raw: true,
      transaction: t,
    });

    const dsRows = await db.PtrsDataset.findAll({
      where: { customerId, ptrsId: { [Op.in]: ptrsIds } },
      attributes: ["ptrsId", "role", "fileName", "createdAt"],
      order: [
        ["ptrsId", "ASC"],
        ["createdAt", "ASC"],
      ],
      raw: true,
      transaction: t,
    });

    const byPtrsId = new Map();
    for (const ds of dsRows || []) {
      const key = String(ds?.ptrsId || "");
      if (!key) continue;
      if (!byPtrsId.has(key)) byPtrsId.set(key, []);
      byPtrsId.get(key).push(ds);
    }

    const isMainRole = (role) => {
      const r = String(role || "")
        .trim()
        .toLowerCase();
      return r === "main" || r.startsWith("main_");
    };

    const pickDisplayFileName = (ptrsId) => {
      const list = byPtrsId.get(String(ptrsId || "")) || [];
      const main = list.find((d) => isMainRole(d?.role));
      const chosen = main || list[0] || null;
      return chosen?.fileName || null;
    };

    const items = (ptrsRows || [])
      .map((r) => {
        const fieldMapStats = fieldMapStatsByPtrsId.get(String(r.id || "")) || {
          mappedFieldsCount: 0,
          fieldMapUpdatedAt: null,
          fieldMapCreatedAt: null,
        };

        return {
          ...r,
          fileName: pickDisplayFileName(r.id),
          mapMeta: metaByPtrsId.get(r.id) || null,
          mappedFieldsCount: fieldMapStats.mappedFieldsCount,
          fieldMapUpdatedAt: fieldMapStats.fieldMapUpdatedAt,
          fieldMapCreatedAt: fieldMapStats.fieldMapCreatedAt,
        };
      })
      .sort((a, b) => {
        const countDiff =
          Number(b?.mappedFieldsCount || 0) - Number(a?.mappedFieldsCount || 0);
        if (countDiff !== 0) return countDiff;

        const aTime = new Date(
          a?.fieldMapUpdatedAt || a?.updatedAt || a?.createdAt || 0,
        ).getTime();
        const bTime = new Date(
          b?.fieldMapUpdatedAt || b?.updatedAt || b?.createdAt || 0,
        ).getTime();

        return bTime - aTime;
      });

    await t.commit();
    return { items };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function getFieldMap({
  customerId,
  ptrsId,
  profileId,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!profileId) throw new Error("profileId is required");

  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const isExternalTx = !!transaction;

  try {
    const rows = await db.PtrsFieldMap.findAll({
      where: { customerId, ptrsId, profileId },
      order: [["canonicalField", "ASC"]],
      raw: true,
      transaction: t,
    });

    if (!isExternalTx && !t.finished) {
      await t.commit();
    }

    return rows || [];
  } catch (err) {
    if (!isExternalTx && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function saveFieldMap({
  customerId,
  ptrsId,
  profileId,
  fieldMap,
  userId,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!Array.isArray(fieldMap)) throw new Error("fieldMap array is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    await db.PtrsFieldMap.destroy({
      where: { customerId, ptrsId, profileId },
      force: true,
      transaction: t,
    });

    const actor = userId || null;

    const payload = fieldMap
      .filter((r) => r && typeof r === "object")
      .map((r) => ({
        customerId,
        ptrsId,
        profileId,
        canonicalField: r.canonicalField,
        sourceRole: r.sourceRole,
        sourceColumn: r.sourceColumn ?? null,
        transformType: r.transformType ?? null,
        transformConfig: r.transformConfig ?? null,
        meta: r.meta ?? null,
        createdBy: actor,
        updatedBy: actor,
      }))
      .filter((r) => r.canonicalField && r.sourceRole);

    const seenCanonicalFields = new Set();
    const duplicateCanonicalFields = new Set();

    for (const row of payload) {
      const key = String(row.canonicalField || "").trim();
      if (!key) continue;
      if (seenCanonicalFields.has(key)) {
        duplicateCanonicalFields.add(key);
        continue;
      }
      seenCanonicalFields.add(key);
    }

    if (duplicateCanonicalFields.size) {
      const err = new Error(
        `Duplicate canonical field mappings are not allowed: ${Array.from(
          duplicateCanonicalFields,
        ).join(", ")}`,
      );
      err.statusCode = 400;
      throw err;
    }

    if (payload.length) {
      await db.PtrsFieldMap.bulkCreate(payload, {
        transaction: t,
        validate: true,
      });
    }

    const rows = await db.PtrsFieldMap.findAll({
      where: { customerId, ptrsId, profileId },
      order: [["canonicalField", "ASC"]],
      raw: true,
      transaction: t,
    });

    await t.commit();
    return rows || [];
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function saveSupportConfig({
  customerId,
  ptrsId,
  mappings,
  extras = null,
  fallbacks = null,
  defaults = null,
  joins,
  rowRules = null,
  profileId = null,
  customFields,
  userId,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const existing = await db.PtrsColumnMap.findOne({
      where: { customerId, ptrsId },
      transaction: t,
    });

    const incomingSignature = buildMaterialMapSignature({
      mappings,
      joins,
      customFields,
    });

    const existingExtrasObj = safeParseJsonObject(existing?.extras) || {};
    const existingMeta = existing
      ? extractMapMetaFromExtras(existingExtrasObj)
      : null;
    const existingSignature = existingMeta?.signature || null;

    if (
      existing &&
      existingSignature &&
      existingSignature === incomingSignature
    ) {
      slog.info(
        "PTRS v2 saveSupportConfig: no material change detected; skipping update",
        {
          action: "PtrsV2SaveSupportConfigNoop",
          customerId,
          ptrsId,
          signature: incomingSignature,
        },
      );

      const plain = existing.get ? existing.get({ plain: true }) : existing;
      await t.commit();
      return plain;
    }

    const resolveField = (incoming, existingValue) =>
      typeof incoming === "undefined" ? existingValue : incoming;

    const nextJoins = resolveField(joins, existing ? existing.joins : null);

    const payload = {
      mappings: resolveField(mappings, existing?.mappings || null),
      extras: resolveField(extras, existing?.extras || null),
      fallbacks: resolveField(fallbacks, existing?.fallbacks || null),
      defaults: resolveField(defaults, existing?.defaults || null),
      joins: nextJoins,
      rowRules: resolveField(rowRules, existing?.rowRules || null),
      profileId: resolveField(profileId, existing?.profileId || null),
      customFields: resolveField(customFields, existing?.customFields || null),
    };

    const incomingExtrasObj = safeParseJsonObject(payload.extras) || {};
    const nowIso = new Date().toISOString();
    const nextExtras = {
      ...existingExtrasObj,
      ...incomingExtrasObj,
    };

    nextExtras.mapMeta = buildMapMetaFromMappings(
      payload.mappings,
      incomingSignature,
      nowIso,
    );

    payload.extras = nextExtras;

    slog.info(
      "PTRS v2 saveSupportConfig: upserting support config",
      safeMeta({
        customerId,
        ptrsId,
        hasJoins: !!payload.joins,
        hasCustomFields: !!payload.customFields,
        hasRowRules: !!payload.rowRules,
        joinsType: payload.joins ? typeof payload.joins : null,
        customFieldsType: payload.customFields
          ? typeof payload.customFields
          : null,
      }),
    );

    if (existing) {
      await existing.update(
        {
          ...payload,
          updatedBy: userId || existing.updatedBy || existing.createdBy || null,
        },
        { transaction: t },
      );

      await t.commit();
      return existing.get({ plain: true });
    }

    const row = await db.PtrsColumnMap.create(
      {
        customerId,
        ptrsId,
        ...payload,
        createdBy: userId || null,
        updatedBy: userId || null,
      },
      { transaction: t },
    );

    await t.commit();
    return row.get({ plain: true });
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

module.exports = {
  REQUIRED_CANONICAL_FIELDS,
  getMap,
  getSupportConfig,
  listCompatibleMaps,
  getFieldMap,
  saveFieldMap,
  saveSupportConfig,
};

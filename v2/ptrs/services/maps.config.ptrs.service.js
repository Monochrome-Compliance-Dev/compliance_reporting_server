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

async function getMap({ customerId, ptrsId, transaction = null }) {
  const supportConfig = await getSupportConfig({
    customerId,
    ptrsId,
    transaction,
  });

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
      attributes: [
        "ptrsId",
        "datasetId",
        "canonicalField",
        "updatedAt",
        "createdAt",
      ],
      raw: true,
      transaction: t,
    });

    const ptrsIds = Array.from(
      new Set(fieldMaps.map((row) => row?.ptrsId).filter(Boolean)),
    );

    if (!ptrsIds.length) {
      await t.commit();
      return { items: [] };
    }

    const metaByPtrsId = new Map();
    for (const sc of supportConfigs) {
      metaByPtrsId.set(sc.ptrsId, extractMapMetaFromExtras(sc.extras));
    }

    const fieldMapStatsByDataset = new Map();

    for (const row of fieldMaps || []) {
      const ptrsKey = String(row?.ptrsId || "");
      const datasetId = row?.datasetId || null;

      if (!ptrsKey || !datasetId) continue;
      const key = `${ptrsKey}:${datasetId}`;

      const stat = fieldMapStatsByDataset.get(key) || {
        ptrsId: ptrsKey,
        datasetId,
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

      fieldMapStatsByDataset.set(key, stat);
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
      attributes: [
        "id",
        "ptrsId",
        "role",
        "purpose",
        "referenceKind",
        "sourceFormat",
        "adapterType",
        "fileName",
        "createdAt",
      ],
      order: [
        ["ptrsId", "ASC"],
        ["createdAt", "ASC"],
      ],
      raw: true,
      transaction: t,
    });

    const ptrsById = new Map((ptrsRows || []).map((row) => [row.id, row]));
    const datasetById = new Map((dsRows || []).map((row) => [row.id, row]));

    const items = Array.from(fieldMapStatsByDataset.values())
      .map((fieldMapStats) => {
        const ptrs = ptrsById.get(fieldMapStats.ptrsId);
        const dataset = datasetById.get(fieldMapStats.datasetId);
        if (!ptrs || !dataset || dataset.purpose !== "transaction") return null;

        return {
          ...ptrs,
          datasetId: dataset.id,
          fileName: dataset.fileName || null,
          dataset: {
            id: dataset.id,
            role: dataset.role,
            purpose: dataset.purpose,
            referenceKind: dataset.referenceKind || null,
            sourceFormat: dataset.sourceFormat,
            adapterType: dataset.adapterType || null,
          },
          mapMeta: metaByPtrsId.get(ptrs.id) || null,
          mappedFieldsCount: fieldMapStats.mappedFieldsCount,
          fieldMapUpdatedAt: fieldMapStats.fieldMapUpdatedAt,
          fieldMapCreatedAt: fieldMapStats.fieldMapCreatedAt,
        };
      })
      .filter(Boolean)
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
      where: {
        customerId,
        ptrsId,
        profileId,
      },
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
  datasetId,
  fieldMap,
  userId,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!datasetId) throw new Error("datasetId is required");
  if (!Array.isArray(fieldMap)) throw new Error("fieldMap array is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    await db.PtrsFieldMap.destroy({
      where: { customerId, ptrsId, profileId, datasetId },
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
        datasetId,
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
      where: { customerId, ptrsId, profileId, datasetId },
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

async function importFieldMap({
  customerId,
  sourcePtrsId,
  sourceDatasetId,
  targetPtrsId,
  targetDatasetId,
  profileId,
  userId,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!sourcePtrsId) throw new Error("sourcePtrsId is required");
  if (!sourceDatasetId) throw new Error("sourceDatasetId is required");
  if (!targetPtrsId) throw new Error("targetPtrsId is required");
  if (!targetDatasetId) throw new Error("targetDatasetId is required");
  if (!profileId) throw new Error("profileId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const sourceSupportConfig = await db.PtrsColumnMap.findOne({
      where: { customerId, ptrsId: sourcePtrsId },
      attributes: ["joins"],
      raw: true,
      transaction: t,
    });
    const sourceDatasets = await db.PtrsDataset.findAll({
      where: { customerId, ptrsId: sourcePtrsId },
      attributes: ["id", "role", "purpose", "referenceKind"],
      raw: true,
      transaction: t,
    });
    const targetDatasets = await db.PtrsDataset.findAll({
      where: { customerId, ptrsId: targetPtrsId },
      attributes: ["id", "role", "purpose", "referenceKind"],
      raw: true,
      transaction: t,
    });

    const sourceDatasetIds = new Set(
      (sourceDatasets || []).map((dataset) => String(dataset.id)),
    );
    if (!sourceDatasetIds.has(String(sourceDatasetId))) {
      const err = new Error(
        "The selected source dataset does not belong to the source PTRS.",
      );
      err.statusCode = 400;
      throw err;
    }

    const targetDataset = (targetDatasets || []).find(
      (dataset) => String(dataset.id) === String(targetDatasetId),
    );
    if (!targetDataset || targetDataset.purpose !== "transaction") {
      const err = new Error(
        "The selected target dataset is not a transaction dataset in the target PTRS.",
      );
      err.statusCode = 400;
      throw err;
    }

    const joins = safeParseJsonObject(sourceSupportConfig?.joins) || {
      conditions: [],
    };
    const reachableSourceDatasetIds = new Set([String(sourceDatasetId)]);
    let changed = true;
    while (changed) {
      changed = false;
      for (const condition of Array.isArray(joins?.conditions)
        ? joins.conditions
        : []) {
        const fromDatasetId = String(condition?.from?.datasetId || "");
        const toDatasetId = String(condition?.to?.datasetId || "");
        if (!fromDatasetId || !toDatasetId) continue;
        if (
          reachableSourceDatasetIds.has(fromDatasetId) &&
          !reachableSourceDatasetIds.has(toDatasetId)
        ) {
          reachableSourceDatasetIds.add(toDatasetId);
          changed = true;
        }
        if (
          reachableSourceDatasetIds.has(toDatasetId) &&
          !reachableSourceDatasetIds.has(fromDatasetId)
        ) {
          reachableSourceDatasetIds.add(fromDatasetId);
          changed = true;
        }
      }
    }

    const sourceRows = await db.PtrsFieldMap.findAll({
      where: {
        customerId,
        ptrsId: sourcePtrsId,
        profileId,
        datasetId: { [Op.in]: Array.from(reachableSourceDatasetIds) },
      },
      order: [["canonicalField", "ASC"]],
      raw: true,
      transaction: t,
    });

    if (!sourceRows.length) {
      const err = new Error(
        "The selected previous PTRS run has no saved field mappings for that dataset.",
      );
      err.statusCode = 404;
      throw err;
    }

    const normaliseRole = (value) =>
      String(value || "")
        .trim()
        .toLowerCase();
    if (sourceRows.some((row) => !normaliseRole(row.sourceRole))) {
      const err = new Error(
        "Cannot import field map because a mapping has no sourceRole.",
      );
      err.statusCode = 400;
      err.code = "FIELD_MAP_SOURCE_ROLE_UNRESOLVED";
      throw err;
    }
    const targetDatasetIdByRole = new Map();
    for (const sourceRole of new Set(
      sourceRows.map((row) => normaliseRole(row.sourceRole)).filter(Boolean),
    )) {
      if (sourceRole === "transaction") {
        targetDatasetIdByRole.set(sourceRole, String(targetDatasetId));
        continue;
      }

      const matches = (targetDatasets || []).filter((dataset) => {
        if (dataset.purpose !== "reference") return false;
        return [dataset.role, dataset.referenceKind]
          .map(normaliseRole)
          .includes(sourceRole);
      });

      if (matches.length !== 1) {
        const err = new Error(
          matches.length
            ? `Cannot import field map because sourceRole "${sourceRole}" matches multiple datasets in the target PTRS.`
            : `Cannot import field map because sourceRole "${sourceRole}" has no dataset in the target PTRS.`,
        );
        err.statusCode = 400;
        err.code = "FIELD_MAP_SOURCE_ROLE_UNRESOLVED";
        throw err;
      }

      targetDatasetIdByRole.set(sourceRole, String(matches[0].id));
    }

    const actor = userId || null;

    const payload = sourceRows.map((row) => {
      const resolvedDatasetId = targetDatasetIdByRole.get(
        normaliseRole(row.sourceRole),
      );
      const existingMeta =
        row?.meta && typeof row.meta === "object" && !Array.isArray(row.meta)
          ? row.meta
          : {};

      const remainingMeta = { ...existingMeta };
      delete remainingMeta.sourceDatasetId;

      return {
        customerId,
        ptrsId: targetPtrsId,
        profileId,
        datasetId: resolvedDatasetId,
        canonicalField: row.canonicalField,
        sourceRole: row.sourceRole,
        sourceColumn: row.sourceColumn ?? null,
        transformType: row.transformType ?? null,
        transformConfig: row.transformConfig ?? null,
        meta: Object.keys(remainingMeta).length ? remainingMeta : null,
        createdBy: actor,
        updatedBy: actor,
      };
    });

    const canonicalFields = payload.map((row) =>
      String(row.canonicalField || "").trim(),
    );
    const duplicateCanonicalFields = canonicalFields.filter(
      (canonicalField, index) =>
        canonicalField && canonicalFields.indexOf(canonicalField) !== index,
    );
    if (duplicateCanonicalFields.length) {
      const err = new Error(
        `Cannot import field map because canonicalField "${duplicateCanonicalFields[0]}" is mapped more than once in the source map.`,
      );
      err.statusCode = 400;
      err.code = "FIELD_MAP_CANONICAL_FIELD_DUPLICATE";
      throw err;
    }

    await db.PtrsFieldMap.destroy({
      where: {
        customerId,
        ptrsId: targetPtrsId,
        profileId,
        canonicalField: { [Op.in]: canonicalFields },
      },
      force: true,
      transaction: t,
    });

    await db.PtrsFieldMap.bulkCreate(payload, {
      transaction: t,
      validate: true,
    });

    const importedRows = await db.PtrsFieldMap.findAll({
      where: {
        customerId,
        ptrsId: targetPtrsId,
        profileId,
        canonicalField: { [Op.in]: canonicalFields },
      },
      order: [["canonicalField", "ASC"]],
      raw: true,
      transaction: t,
    });

    await t.commit();

    return importedRows;
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
  rowRules,
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
    const hasRowRules = rowRules != null;
    if (hasRowRules && !Array.isArray(rowRules)) {
      const error = new Error("rowRules must be an array when provided");
      error.statusCode = 400;
      throw error;
    }
    const rowRulesChanged =
      hasRowRules &&
      JSON.stringify(rowRules) !== JSON.stringify(existing?.rowRules ?? []);

    if (
      existing &&
      existingSignature &&
      existingSignature === incomingSignature &&
      !rowRulesChanged
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
      rowRules: hasRowRules ? rowRules : existing?.rowRules || [],
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
  importFieldMap,
  saveSupportConfig,
};

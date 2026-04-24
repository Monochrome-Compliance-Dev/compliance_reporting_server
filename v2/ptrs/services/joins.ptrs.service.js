const db = require("@/db/database");
const { Op } = require("sequelize");

const { safeMeta, slog } = require("@/v2/ptrs/services/ptrs.service");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

const { createPtrsTrace, hrMsSince } = require("@/helpers/ptrsTrackerLog");

module.exports = {
  getJoins,
  saveJoins,
  listCompatibleJoins,
};

// Postgres JSONB will reject strings containing NUL (\u0000) bytes.
// Also, JSON cannot represent `undefined` values.
function sanitizeForJsonbDeep(value) {
  if (value === undefined) return null;
  if (value === null) return null;

  if (typeof value === "string") {
    return value.includes("\u0000") ? value.replace(/\u0000/g, "") : value;
  }

  if (Array.isArray(value)) {
    return value.map((v) => sanitizeForJsonbDeep(v));
  }

  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = sanitizeForJsonbDeep(v);
    }
    return out;
  }

  return value;
}

function normalizeJoinSide(side, label) {
  if (!side || typeof side !== "object" || Array.isArray(side)) {
    const e = new Error(`Invalid joins payload (${label} must be an object)`);
    e.statusCode = 400;
    throw e;
  }

  const role = String(side.role || "").trim();
  const column = String(side.column || "").trim();
  const datasetId =
    side.datasetId != null && String(side.datasetId).trim() !== ""
      ? String(side.datasetId).trim()
      : null;

  const transform =
    side.transform &&
    typeof side.transform === "object" &&
    !Array.isArray(side.transform)
      ? {
          op:
            side.transform.op != null && String(side.transform.op).trim() !== ""
              ? String(side.transform.op).trim()
              : null,
          ...(side.transform.arg != null &&
          String(side.transform.arg).trim() !== ""
            ? { arg: String(side.transform.arg).trim() }
            : {}),
        }
      : null;

  if (!role) {
    const e = new Error(`Invalid joins payload (${label}.role is required)`);
    e.statusCode = 400;
    throw e;
  }

  if (!column) {
    const e = new Error(`Invalid joins payload (${label}.column is required)`);
    e.statusCode = 400;
    throw e;
  }

  return {
    datasetId,
    role,
    column,
    transform: transform?.op ? transform : null,
  };
}

function normalizeJoinsPayload(joins) {
  if (!joins || typeof joins !== "object" || !Array.isArray(joins.conditions)) {
    const e = new Error("Invalid joins payload (expected { conditions: [] })");
    e.statusCode = 400;
    throw e;
  }

  const conditions = joins.conditions.map((condition, index) => {
    if (
      !condition ||
      typeof condition !== "object" ||
      Array.isArray(condition)
    ) {
      const e = new Error(
        `Invalid joins payload (conditions[${index}] must be an object)`,
      );
      e.statusCode = 400;
      throw e;
    }

    return {
      from: normalizeJoinSide(condition.from, `conditions[${index}].from`),
      to: normalizeJoinSide(condition.to, `conditions[${index}].to`),
    };
  });

  return { conditions };
}

// Remap imported datasetIds to the matching current PTRS dataset.
// Matching is intentionally strict: old datasetId -> old role/fileName -> current role/fileName.
async function remapDatasetIdsForCurrentPtrs({
  customerId,
  ptrsId,
  joins,
  customFields,
  transaction,
}) {
  const conditions = Array.isArray(joins?.conditions) ? joins.conditions : [];
  const fields = Array.isArray(customFields) ? customFields : [];

  const joinSides = conditions.flatMap((condition) => [
    condition.from,
    condition.to,
  ]);

  const collectDatasetIdsDeep = (value, out = []) => {
    if (!value || typeof value !== "object") return out;

    if (!Array.isArray(value) && value.datasetId != null) {
      const datasetId = String(value.datasetId || "").trim();
      if (datasetId) out.push(datasetId);
    }

    if (Array.isArray(value)) {
      for (const item of value) collectDatasetIdsDeep(item, out);
      return out;
    }

    for (const child of Object.values(value)) {
      collectDatasetIdsDeep(child, out);
    }

    return out;
  };

  const importedDatasetIds = Array.from(
    new Set([
      ...joinSides
        .map((side) => String(side?.datasetId || "").trim())
        .filter(Boolean),
      ...collectDatasetIdsDeep(fields),
    ]),
  );

  if (!importedDatasetIds.length) {
    if (conditions.length) {
      const e = new Error(
        "Cannot import joins: datasetId is required on every join side",
      );
      e.statusCode = 400;
      throw e;
    }

    return { joins, customFields };
  }

  const currentDatasets = await db.PtrsDataset.findAll({
    where: { customerId, ptrsId },
    attributes: ["id", "role", "fileName"],
    raw: true,
    transaction,
  });

  const currentDatasetIds = new Set(
    (currentDatasets || []).map((dataset) => String(dataset.id)),
  );

  const sourceDatasetIds = importedDatasetIds.filter(
    (datasetId) => !currentDatasetIds.has(datasetId),
  );

  const sourceDatasets = sourceDatasetIds.length
    ? await db.PtrsDataset.findAll({
        where: { customerId, id: { [Op.in]: sourceDatasetIds } },
        attributes: ["id", "role", "fileName"],
        raw: true,
        transaction,
      })
    : [];

  const sourceById = new Map();
  for (const dataset of sourceDatasets || []) {
    sourceById.set(String(dataset.id), dataset);
  }

  const currentByRoleAndFileName = new Map();
  for (const dataset of currentDatasets || []) {
    const role = String(dataset?.role || "").trim();
    const fileName = String(dataset?.fileName || "").trim();
    if (!role || !fileName) continue;

    const key = `${role}::${fileName}`;
    if (!currentByRoleAndFileName.has(key)) {
      currentByRoleAndFileName.set(key, []);
    }
    currentByRoleAndFileName.get(key).push(dataset);
  }

  const remapDatasetId = (datasetId, label) => {
    const importedDatasetId = String(datasetId || "").trim();
    if (!importedDatasetId) {
      const e = new Error(
        `Cannot import joins: datasetId is required for ${label}`,
      );
      e.statusCode = 400;
      throw e;
    }

    if (currentDatasetIds.has(importedDatasetId)) return importedDatasetId;

    const sourceDataset = sourceById.get(importedDatasetId);
    if (!sourceDataset) {
      const e = new Error(
        `Cannot import joins: source dataset "${importedDatasetId}" was not found for this customer`,
      );
      e.statusCode = 400;
      throw e;
    }

    const role = String(sourceDataset.role || "").trim();
    const fileName = String(sourceDataset.fileName || "").trim();
    const key = `${role}::${fileName}`;
    const matches = currentByRoleAndFileName.get(key) || [];

    if (matches.length === 1) return String(matches[0].id);

    if (!matches.length) {
      const e = new Error(
        `Cannot import joins: no current dataset matches role "${role}" and fileName "${fileName}"`,
      );
      e.statusCode = 400;
      throw e;
    }

    const e = new Error(
      `Cannot import joins: multiple current datasets match role "${role}" and fileName "${fileName}"`,
    );
    e.statusCode = 400;
    throw e;
  };

  const remapCustomFieldDatasetIdsDeep = (value, path = "customFields") => {
    if (!value || typeof value !== "object") return value;

    if (Array.isArray(value)) {
      return value.map((item, index) =>
        remapCustomFieldDatasetIdsDeep(item, `${path}[${index}]`),
      );
    }

    const out = {};
    for (const [key, child] of Object.entries(value)) {
      if (key === "datasetId" && child != null) {
        out[key] = remapDatasetId(child, `${path}.datasetId`);
      } else {
        out[key] = remapCustomFieldDatasetIdsDeep(child, `${path}.${key}`);
      }
    }
    return out;
  };

  return {
    joins: {
      conditions: conditions.map((condition, index) => ({
        from: {
          ...condition.from,
          datasetId: remapDatasetId(
            condition.from?.datasetId,
            `conditions[${index}].from`,
          ),
        },
        to: {
          ...condition.to,
          datasetId: remapDatasetId(
            condition.to?.datasetId,
            `conditions[${index}].to`,
          ),
        },
      })),
    },
    customFields: remapCustomFieldDatasetIdsDeep(fields),
  };
}

async function getJoins({ customerId, ptrsId, transaction = null }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const trace = process.env.PTRS_TRACE
    ? createPtrsTrace({
        customerId,
        ptrsId,
        actorId: null,
        logInfo: (msg, meta) => slog.info(msg, meta),
        meta: safeMeta,
      })
    : null;

  const startNs = process.hrtime.bigint();
  trace?.write("joins_get_begin");

  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const isExternalTx = !!transaction;

  try {
    const row = await db.PtrsColumnMap.findOne({
      where: { customerId, ptrsId },
      attributes: ["joins", "customFields", "profileId"],
      raw: true,
      transaction: t,
    });

    trace?.write("joins_get_db_fetched", {
      hasRow: !!row,
      hasJoins: !!row?.joins,
      hasCustomFields: !!row?.customFields,
    });

    const joinsRaw = row?.joins;

    const joins =
      joinsRaw &&
      typeof joinsRaw === "object" &&
      Array.isArray(joinsRaw.conditions)
        ? normalizeJoinsPayload(joinsRaw)
        : { conditions: [] };

    const customFields = Array.isArray(row?.customFields)
      ? row.customFields
      : [];

    const profileId = row?.profileId || null;

    if (!isExternalTx && !t.finished) {
      await t.commit();
    }

    trace?.write("joins_get_end", {
      durationMs: hrMsSince(startNs),
      joinsCount: Array.isArray(joins?.conditions)
        ? joins.conditions.length
        : 0,
      customFieldsCount: Array.isArray(customFields) ? customFields.length : 0,
    });

    if (trace) await trace.close();

    return { joins, customFields, profileId };
  } catch (err) {
    trace?.write("joins_get_error", {
      message: err?.message || null,
      statusCode: err?.statusCode || null,
      durationMs: hrMsSince(startNs),
    });

    if (trace) await trace.close();

    if (!isExternalTx && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function saveJoins({
  customerId,
  ptrsId,
  joins,
  customFields: customFieldsInput,
  profileId = null,
  userId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  let customFields = customFieldsInput;

  // Joins validation removed; now done via normalizeJoinsPayload below.

  if (!Array.isArray(customFields)) {
    const e = new Error("Invalid customFields payload (expected array)");
    e.statusCode = 400;
    throw e;
  }

  let normalizedJoins = normalizeJoinsPayload(joins);

  const trace = process.env.PTRS_TRACE
    ? createPtrsTrace({
        customerId,
        ptrsId,
        actorId: userId || null,
        logInfo: (msg, meta) => slog.info(msg, meta),
        meta: safeMeta,
      })
    : null;

  const startNs = process.hrtime.bigint();
  trace?.write("joins_save_begin", {
    joinsCount: Array.isArray(normalizedJoins?.conditions)
      ? normalizedJoins.conditions.length
      : 0,
    customFieldsCount: Array.isArray(customFields) ? customFields.length : 0,
    hasProfileId: !!profileId,
  });

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const remapped = await remapDatasetIdsForCurrentPtrs({
      customerId,
      ptrsId,
      joins: normalizedJoins,
      customFields,
      transaction: t,
    });

    normalizedJoins = remapped.joins;
    customFields = remapped.customFields;

    const existing = await db.PtrsColumnMap.findOne({
      where: { customerId, ptrsId },
      transaction: t,
    });

    trace?.write("joins_save_lookup_complete", {
      existing: !!existing,
    });

    const payload = {
      joins: sanitizeForJsonbDeep(normalizedJoins),
      customFields: sanitizeForJsonbDeep(customFields),
      profileId: profileId ?? null,
    };

    if (existing) {
      const updateStartNs = process.hrtime.bigint();
      trace?.write("joins_save_update_begin");

      await existing.update(
        {
          ...payload,
          updatedBy: userId || existing.updatedBy || existing.createdBy || null,
        },
        { transaction: t },
      );

      trace?.write("joins_save_update_end", {
        durationMs: hrMsSince(updateStartNs),
      });

      trace?.write("joins_save_commit_begin");

      await t.commit();

      trace?.write("joins_save_committed", {
        durationMs: hrMsSince(startNs),
      });

      if (trace) await trace.close();

      const plain = existing.get({ plain: true });
      return {
        joins:
          plain?.joins &&
          typeof plain.joins === "object" &&
          Array.isArray(plain.joins.conditions)
            ? { conditions: plain.joins.conditions }
            : { conditions: normalizedJoins.conditions },
        customFields: Array.isArray(plain?.customFields)
          ? plain.customFields
          : customFields,
        profileId: plain?.profileId || profileId || null,
      };
    }

    const createStartNs = process.hrtime.bigint();
    trace?.write("joins_save_create_begin");

    const row = await db.PtrsColumnMap.create(
      {
        customerId,
        ptrsId,
        mappings: null,
        extras: null,
        fallbacks: null,
        defaults: null,
        rowRules: null,
        ...payload,
        createdBy: userId || null,
        updatedBy: userId || null,
      },
      { transaction: t },
    );

    trace?.write("joins_save_create_end", {
      durationMs: hrMsSince(createStartNs),
    });

    trace?.write("joins_save_commit_begin");

    await t.commit();

    trace?.write("joins_save_committed", {
      durationMs: hrMsSince(startNs),
    });

    if (trace) await trace.close();

    const plain = row.get({ plain: true });
    return {
      joins:
        plain?.joins &&
        typeof plain.joins === "object" &&
        Array.isArray(plain.joins.conditions)
          ? { conditions: plain.joins.conditions }
          : { conditions: normalizedJoins.conditions },
      customFields: Array.isArray(plain?.customFields)
        ? plain.customFields
        : customFields,
      profileId: plain?.profileId || profileId || null,
    };
  } catch (err) {
    trace?.write("joins_save_error", {
      message: err?.message || null,
      statusCode: err?.statusCode || null,
      durationMs: hrMsSince(startNs),
    });

    if (trace) await trace.close();

    slog.error(
      "PTRS v2 saveJoins: failed",
      safeMeta({
        action: "PtrsV2SaveJoins",
        customerId,
        ptrsId,
        error: err?.message || null,
        statusCode: err?.statusCode || 500,
      }),
    );

    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function listCompatibleJoins({ customerId, ptrsId, transaction = null }) {
  if (!customerId) throw new Error("customerId is required");

  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const isExternalTx = !!transaction;

  try {
    const rows = await db.PtrsColumnMap.findAll({
      where: {
        customerId,
        ...(ptrsId ? { ptrsId: { [Op.ne]: ptrsId } } : {}),
      },
      attributes: [
        "ptrsId",
        "joins",
        "customFields",
        "profileId",
        "updatedAt",
        "createdAt",
      ],
      order: [
        ["updatedAt", "DESC"],
        ["createdAt", "DESC"],
      ],
      raw: true,
      transaction: t,
    });

    const eligible = (rows || []).filter((row) => {
      const joinsCount = Array.isArray(row?.joins?.conditions)
        ? row.joins.conditions.length
        : 0;
      const customFieldsCount = Array.isArray(row?.customFields)
        ? row.customFields.length
        : 0;
      return joinsCount > 0 || customFieldsCount > 0;
    });

    const ptrsIds = Array.from(
      new Set(
        eligible.map((row) => String(row?.ptrsId || "").trim()).filter(Boolean),
      ),
    );

    let byPtrsId = new Map();

    if (ptrsIds.length) {
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

      for (const ds of dsRows || []) {
        const key = String(ds?.ptrsId || "");
        if (!key) continue;
        if (!byPtrsId.has(key)) byPtrsId.set(key, []);
        byPtrsId.get(key).push(ds);
      }
    }

    const isMainRole = (role) => {
      const r = String(role || "")
        .trim()
        .toLowerCase();
      return r === "main" || r.startsWith("main_");
    };

    const pickDisplayFileName = (candidatePtrsId) => {
      const list = byPtrsId.get(String(candidatePtrsId || "")) || [];
      const main = list.find((d) => isMainRole(d?.role));
      const chosen = main || list[0] || null;
      return chosen?.fileName || null;
    };

    const items = eligible.map((row) => ({
      id: row.ptrsId,
      ptrsId: row.ptrsId,
      fileName: pickDisplayFileName(row.ptrsId),
      joinsCount: Array.isArray(row?.joins?.conditions)
        ? row.joins.conditions.length
        : 0,
      customFieldsCount: Array.isArray(row?.customFields)
        ? row.customFields.length
        : 0,
      profileId: row?.profileId || null,
      updatedAt: row?.updatedAt || null,
      createdAt: row?.createdAt || null,
    }));

    if (!isExternalTx && !t.finished) {
      await t.commit();
    }

    return { items };
  } catch (err) {
    if (!isExternalTx && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

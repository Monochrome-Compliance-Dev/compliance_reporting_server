const db = require("@/db/database");

const { safeMeta, slog } = require("@/v2/ptrs/services/ptrs.service");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

const { createPtrsTrace, hrMsSince } = require("@/helpers/ptrsTrackerLog");

module.exports = {
  getJoins,
  saveJoins,
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
        ? { conditions: joinsRaw.conditions }
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
  customFields,
  profileId = null,
  userId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  if (!joins || typeof joins !== "object" || !Array.isArray(joins.conditions)) {
    const e = new Error("Invalid joins payload (expected { conditions: [] })");
    e.statusCode = 400;
    throw e;
  }

  if (!Array.isArray(customFields)) {
    const e = new Error("Invalid customFields payload (expected array)");
    e.statusCode = 400;
    throw e;
  }

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
    joinsCount: Array.isArray(joins?.conditions) ? joins.conditions.length : 0,
    customFieldsCount: Array.isArray(customFields) ? customFields.length : 0,
    hasProfileId: !!profileId,
  });

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const existing = await db.PtrsColumnMap.findOne({
      where: { customerId, ptrsId },
      transaction: t,
    });

    trace?.write("joins_save_lookup_complete", {
      existing: !!existing,
    });

    const payload = {
      joins: sanitizeForJsonbDeep(joins),
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
            : { conditions: joins.conditions },
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
          : { conditions: joins.conditions },
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

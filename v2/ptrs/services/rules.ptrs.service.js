const db = require("@/db/database");

const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { slog, safeMeta } = require("./ptrs.service");

module.exports = {
  applyRules,
  getRulesPreview,
  applyRulesAndPersist,
  updateRulesOnly,
  getRules,
  getProfileRules,
};

function applyRules(rows, rules = []) {
  const enabled = (rules || []).filter((r) => r && r.enabled !== false);
  const stats = { rulesTried: enabled.length, rowsAffected: 0, actions: 0 };

  if (!Array.isArray(rows) || !rows.length || !enabled.length) {
    return { rows: rows || [], stats };
  }

  for (const row of rows) {
    let touched = false;

    // Use whatever was previously persisted into data._appliedRules
    const existingApplied = Array.isArray(row._appliedRules)
      ? row._appliedRules.slice()
      : [];
    const appliedSet = new Set(existingApplied);

    for (const rule of enabled) {
      const ruleKey = rule.id || rule.label || "rule";

      // 🔒 Idempotency: if this rule has already been applied to this row, skip it
      if (appliedSet.has(ruleKey)) {
        continue;
      }

      const conds = Array.isArray(rule.when) ? rule.when : [];
      const ok = conds.every((c) => _matches(row, c));
      if (!ok) continue;

      const actions = Array.isArray(rule.then) ? rule.then : [];
      if (!actions.length) {
        // No actions, but we still consider the rule "applied" to avoid re-hitting it
        appliedSet.add(ruleKey);
        continue;
      }

      for (const act of actions) {
        _applyAction(row, act);
        stats.actions++;
        touched = true;
      }

      // Mark this rule as applied on this row so future runs can skip it
      appliedSet.add(ruleKey);
    }

    if (appliedSet.size) {
      row._appliedRules = Array.from(appliedSet);
    }

    if (touched) stats.rowsAffected++;
  }

  return { rows, stats };
}

async function getRulesPreview({ customerId, ptrsId, limit = 50 }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const effectiveLimit = Math.min(Number(limit) || 50, 500);

  // 🔐 Ensure RLS context for any import/raw reads
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // 1) Compose mapped rows (main import + joins)
    const { rows: baseRows, headers } = await composeMappedRowsForPtrs({
      customerId,
      ptrsId,
      limit: effectiveLimit,
      transaction: t,
    });

    // 2) Load row-level rules from the column map
    let rowRules = null;
    try {
      const mapRow = await getColumnMap({ customerId, ptrsId });
      rowRules = mapRow && mapRow.rowRules != null ? mapRow.rowRules : null;
      if (typeof rowRules === "string") {
        try {
          rowRules = JSON.parse(rowRules);
        } catch {
          rowRules = null;
        }
      }
    } catch (_) {
      rowRules = null;
    }

    // 3) Apply rules in-memory
    const rulesResult = applyRules(
      baseRows,
      Array.isArray(rowRules) ? rowRules : []
    );

    await t.commit();

    return {
      headers,
      rows: rulesResult.rows || baseRows,
      stats: { rules: rulesResult.stats || null },
    };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {
        // ignore rollback errors
      }
    }
    throw err;
  }
}

function ensureRulesMeta(meta) {
  const nextMeta = meta && typeof meta === "object" ? { ...meta } : {};
  const rules =
    nextMeta.rules && typeof nextMeta.rules === "object"
      ? { ...nextMeta.rules }
      : {};

  const applied = Array.isArray(rules.applied) ? [...rules.applied] : [];

  rules.applied = applied;
  if (typeof rules.exclude !== "boolean") {
    rules.exclude = false;
  }

  nextMeta.rules = rules;
  return nextMeta;
}

async function applyRulesAndPersist({
  customerId,
  ptrsId,
  profileId = null,
  limit = null, // null = process ALL rows for this ptrsId
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  // If limit is provided (for diagnostics), respect it with a sane cap.
  // If null/undefined, we pass null through, which composeMappedRowsForPtrs
  // interprets as "no limit" (full dataset).
  const effectiveLimit =
    limit == null || typeof limit === "undefined"
      ? null
      : Math.min(Number(limit) || 50, 5000);

  const started = Date.now();

  slog.info("PTRS v2 applyRulesAndPersist: starting", {
    action: "PtrsV2RulesApplyStart",
    customerId,
    ptrsId,
    requestedLimit: limit,
    effectiveLimit,
  });

  // 🔐 RLS-safe tenant-scoped transaction for reading import + writing stage
  const t = await beginTransactionWithCustomerContext(customerId);

  // ------------------------------------------------------------
  // SAFETY GUARD — prevent rules apply on enormous datasets
  // ------------------------------------------------------------
  const HARD_ROW_CAP = 500000;

  const totalRows = await db.PtrsImportRaw.count({
    where: { customerId, ptrsId },
    transaction: t,
  });

  slog.info("PTRS v2 applyRulesAndPersist: dataset size check", {
    action: "PtrsV2RulesApplyRowCapCheck",
    customerId,
    ptrsId,
    totalRows,
    hardRowCap: HARD_ROW_CAP,
    effectiveLimit,
  });

  // Only enforce the cap when NO explicit limit is provided.
  // If a limit is passed, caller is intentionally working on a subset.
  if (!effectiveLimit && totalRows > HARD_ROW_CAP) {
    const err = new Error(
      `PTRS v2 rules apply aborted: dataset has ${totalRows} rows which exceeds the safety limit of ${HARD_ROW_CAP}.`
    );
    err.statusCode = 413; // FE-friendly soft-failure
    throw err;
  }
  // ------------------------------------------------------------

  try {
    // 1) Load row-level rules from column map
    let rowRules = null;
    try {
      const mapRow = await getColumnMap({ customerId, ptrsId, transaction: t });
      rowRules = mapRow && mapRow.rowRules != null ? mapRow.rowRules : null;
      if (typeof rowRules === "string") {
        try {
          rowRules = JSON.parse(rowRules);
        } catch {
          rowRules = null;
        }
      }
    } catch (_) {
      rowRules = null;
    }

    // 2) Compose mapped rows (main import + joins)
    const { rows: baseRows } = await composeMappedRowsForPtrs({
      customerId,
      ptrsId,
      // null => no limit, process full dataset; otherwise use capped preview limit
      limit: effectiveLimit,
      transaction: t,
    });

    slog.info("PTRS v2 applyRulesAndPersist: composed base rows", {
      action: "PtrsV2RulesApplyComposed",
      customerId,
      ptrsId,
      baseRowCount: baseRows.length,
    });

    // 3) Apply rules
    const rulesResult = applyRules(
      baseRows,
      Array.isArray(rowRules) ? rowRules : []
    );
    const rows = rulesResult.rows || baseRows;

    slog.info("PTRS v2 applyRulesAndPersist: rules applied", {
      action: "PtrsV2RulesApplyRules",
      customerId,
      ptrsId,
      afterRulesCount: rows.length,
      rulesStats: safeMeta(rulesResult.stats || null),
    });

    // 4) Prepare payload for tbl_ptrs_stage_row (mirrors stagePtrs persist logic)
    const basePayload = rows.map((r) => {
      const rowNoVal = Number(r?.row_no ?? r?.rowNo ?? 0) || 0;
      const dataObj =
        r && typeof r === "object" && Object.keys(r).length
          ? r
          : { _warning: "⚠️ No mapped data for this row" };

      return {
        customerId: String(customerId),
        ptrsId: String(ptrsId),
        rowNo: rowNoVal,
        data: dataObj,
        errors: null,
        standard: null,
        custom: null,
        meta: {
          _stage: "ptrs.v2.rulesApply",
          at: new Date().toISOString(),
          profileId: profileId || null,
          rules: {
            applied: Array.isArray(r._appliedRules) ? r._appliedRules : [],
            exclude: !!r.exclude,
          },
        },
      };
    });

    const isEmptyPlain = (v) =>
      v &&
      typeof v === "object" &&
      !Array.isArray(v) &&
      Object.keys(v).length === 0;

    const insertWarning = (obj) => {
      if (!obj || typeof obj !== "object") return obj;
      for (const key of ["data", "errors", "standard", "custom", "meta"]) {
        if (isEmptyPlain(obj[key])) {
          obj[key] = {
            _warning: "⚠️ Empty JSONB payload — nothing to insert",
          };
        }
        if (typeof obj[key] === "undefined") {
          obj[key] = null;
        }
      }
      return obj;
    };

    const safePayload = basePayload.map(insertWarning);

    slog.info("PTRS v2 applyRulesAndPersist: preparing to insert", {
      action: "PtrsV2RulesApplyPersist",
      customerId,
      ptrsId,
      batchSize: safePayload.length,
      sampleRow: safeMeta(safePayload[0] || {}),
    });

    // 5) Clear previous stage rows for this run, then bulk insert
    await db.PtrsStageRow.destroy({
      where: { customerId, ptrsId },
      transaction: t,
    });

    if (safePayload.length) {
      await db.PtrsStageRow.bulkCreate(safePayload, {
        validate: false,
        returning: false,
        transaction: t,
      });
    }

    const tookMs = Date.now() - started;

    await t.commit();

    slog.info("PTRS v2 applyRulesAndPersist: done", {
      action: "PtrsV2RulesApplyDone",
      customerId,
      ptrsId,
      persisted: safePayload.length,
      tookMs,
    });

    return {
      persisted: safePayload.length,
      tookMs,
      stats: { rules: rulesResult.stats || null },
    };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {
        // ignore rollback errors
      }
    }
    throw err;
  }
}

/** Update only rules-related fields without touching mappings/defaults/joins */
async function updateRulesOnly({
  customerId,
  ptrsId,
  rowRules = [],
  crossRowRules = [],
  userId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  // Helper to parse JSON/TEXT extras safely
  const parseMaybe = (v) => {
    if (v == null) return null;
    if (typeof v === "string") {
      try {
        return JSON.parse(v);
      } catch {
        return null;
      }
    }
    if (typeof v === "object") return v;
    return null;
  };

  // 1) Load existing map via the RLS-safe accessor
  //    This is the same thing controller.getMap/FE mapping step relies on.
  const existing = await getColumnMap({ customerId, ptrsId });

  // 2) Tenant-scoped transaction for the write
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // --- CASE 1: no existing map row at all -> create minimal row ---
    if (!existing) {
      const row = await db.PtrsColumnMap.create(
        {
          customerId,
          ptrsId,
          // Keep mappings empty but valid
          mappings: {},
          extras: {
            __experimentalCrossRowRules: Array.isArray(crossRowRules)
              ? crossRowRules
              : [],
          },
          fallbacks: null,
          defaults: null,
          joins: null,
          rowRules: Array.isArray(rowRules) ? rowRules : [],
          createdBy: userId || null,
          updatedBy: userId || null,
        },
        { transaction: t }
      );

      await t.commit();
      return row.get({ plain: true });
    }

    // --- CASE 2: map exists -> merge rules into the SAME row ---

    const prevExtras = parseMaybe(existing.extras) || {};
    const nextExtras = {
      ...prevExtras,
      __experimentalCrossRowRules: Array.isArray(crossRowRules)
        ? crossRowRules
        : [],
    };

    await db.PtrsColumnMap.update(
      {
        // Only touch rules-related fields
        rowRules: Array.isArray(rowRules) ? rowRules : [],
        extras: nextExtras,
        updatedBy: userId || existing.updatedBy || existing.createdBy || null,
      },
      {
        where: {
          id: existing.id,
          customerId,
          ptrsId,
        },
        transaction: t,
      }
    );

    await t.commit();

    // Return a plain-ish merged view
    return {
      ...existing,
      rowRules: Array.isArray(rowRules) ? rowRules : [],
      extras: nextExtras,
    };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {
        // ignore rollback errors
      }
    }
    throw err;
  }
}

/** Get rules for a ptrs */
async function getRules({ customerId, ptrsId }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    // table name for rules may change
    const rules = await db.PtrsColumnMap.findOne({
      where: { customerId, ptrsId },
      transaction: t,
      raw: true,
    });
    await t.commit();
    slog.info(
      "PTRS v2 getRules: loaded rules",
      safeMeta({
        customerId,
        ptrsId,
        id: rules?.id || null,
        hasRowRules: !!(rules && rules.rowRules),
      })
    );
    return rules || null;
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

/** Get rules for a profile */
async function getProfileRules({ customerId, profileId }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    // table name for rules may change
    const rules = await db.PtrsColumnMap.findOne({
      where: { customerId, id: profileId },
      transaction: t,
      raw: true,
    });
    await t.commit();
    slog.info(
      "PTRS v2 getProfileRules: loaded profile rules",
      safeMeta({
        customerId,
        profileId,
        id: rules?.id || null,
        hasRowRules: !!(rules && rules.rowRules),
      })
    );
    return rules || null;
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

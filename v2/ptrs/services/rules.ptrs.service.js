const db = require("@/db/database");

const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { slog, safeMeta } = require("./ptrs.service");
const { composeMappedRowsForPtrs } = require("./tablesAndMaps.ptrs.service");

module.exports = {
  applyRules,
  getRulesPreview,
  applyRulesAndPersist,
  updateRulesOnly,
  getRules,
  getProfileRules,
  sandboxRulesPreview,
};

function applyRules(rows, rules = []) {
  const enabled = (rules || []).filter((r) => r && r.enabled !== false);
  const stats = { rulesTried: enabled.length, rowsAffected: 0, actions: 0 };

  if (!Array.isArray(rows) || !rows.length || !enabled.length) {
    return { rows: rows || [], stats };
  }

  for (const row of rows) {
    let touched = false;

    const existingApplied = Array.isArray(row._appliedRules)
      ? row._appliedRules.slice()
      : [];
    const appliedSet = new Set(existingApplied);

    for (const rule of enabled) {
      const ruleKey = rule.id || rule.label || "rule";

      if (appliedSet.has(ruleKey)) {
        continue;
      }

      const conds = Array.isArray(rule.when) ? rule.when : [];
      const ok = conds.every((c) => _matches(row, c));
      if (!ok) continue;

      const actions = Array.isArray(rule.then) ? rule.then : [];
      if (!actions.length) {
        appliedSet.add(ruleKey);
        continue;
      }

      for (const act of actions) {
        _applyAction(row, act);
        stats.actions++;
        touched = true;
      }

      appliedSet.add(ruleKey);
    }

    if (appliedSet.size) {
      row._appliedRules = Array.from(appliedSet);
    }

    if (touched) stats.rowsAffected++;
  }

  return { rows, stats };
}

// --- NEW: central helper to load row-level rules from PtrsRuleset ---
async function loadRowRulesForPtrs({ customerId, ptrsId, transaction }) {
  const rulesets = await db.PtrsRuleset.findAll({
    where: { customerId, ptrsId },
    transaction,
    raw: true,
  });

  const rowRules = [];
  for (const rs of rulesets || []) {
    const def = rs.definition;
    if (!def || typeof def !== "object") continue;
    const type = def.type || rs.scope || "row";
    if (type === "crossRow") continue; // BE only applies row-level rules
    rowRules.push(def);
  }

  slog.info(
    "PTRS v2 loadRowRulesForPtrs",
    safeMeta({
      customerId,
      ptrsId,
      rulesetCount: rulesets?.length || 0,
      rowRules: rowRules.length,
    })
  );

  return rowRules;
}

async function getRulesPreview({ customerId, ptrsId, limit = 50 }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const effectiveLimit = Math.min(Number(limit) || 50, 500);

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // 1) Compose mapped rows (main import + joins)
    const { rows: baseRows, headers } = await composeMappedRowsForPtrs({
      customerId,
      ptrsId,
      limit: effectiveLimit,
      transaction: t,
    });

    // 2) Load row rules from tbl_ptrs_ruleset
    const rowRules = await loadRowRulesForPtrs({
      customerId,
      ptrsId,
      transaction: t,
    });

    // 3) Apply rules in-memory
    const rulesResult = applyRules(baseRows, rowRules);

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

function applySandboxFilter(rows, { field, op, value }) {
  if (!field || !op) return rows;
  const val = String(value ?? "").trim();
  if (!val && !["is_null", "not_null"].includes(op)) return rows;

  const toNum = (v) => {
    const n = Number(String(v).replace(/[, ]+/g, ""));
    return Number.isFinite(n) ? n : NaN;
  };

  return rows.filter((row) => {
    const raw = row[field];
    const s = raw == null ? "" : String(raw);

    switch (op) {
      case "eq":
        return s === val;
      case "neq":
        return s !== val;
      case "gt":
        return toNum(raw) > toNum(val);
      case "gte":
        return toNum(raw) >= toNum(val);
      case "lt":
        return toNum(raw) < toNum(val);
      case "lte":
        return toNum(raw) <= toNum(val);
      case "in": {
        const parts = val
          .split(",")
          .map((p) => p.trim())
          .filter(Boolean);
        return parts.includes(s);
      }
      case "nin": {
        const parts = val
          .split(",")
          .map((p) => p.trim())
          .filter(Boolean);
        return !parts.includes(s);
      }
      case "is_null":
        return raw == null || s === "";
      case "not_null":
        return raw != null && s !== "";
      default:
        return true;
    }
  });
}

function applySandboxFilters(rows, filters) {
  if (!Array.isArray(filters) || !filters.length) return rows;
  return filters.reduce((current, f) => applySandboxFilter(current, f), rows);
}

async function sandboxRulesPreview({
  customerId,
  ptrsId,
  filters = [],
  limit = 50,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const effectiveLimit = Math.min(Number(limit) || 50, 500);

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // Compose mapped rows for the full dataset (no limit) so counts match Excel
    const { rows: baseRows, headers } = await composeMappedRowsForPtrs({
      customerId,
      ptrsId,
      limit: null,
      transaction: t,
    });

    const filteredRows = applySandboxFilters(baseRows, filters);
    const limitedRows = filteredRows.slice(0, effectiveLimit);

    slog.info(
      "PTRS v2 sandboxRulesPreview",
      safeMeta({
        customerId,
        ptrsId,
        totalRows: baseRows.length,
        filters: Array.isArray(filters) ? filters.length : 0,
        totalMatching: filteredRows.length,
        returned: limitedRows.length,
      })
    );

    await t.commit();

    return {
      headers,
      rows: limitedRows,
      stats: {
        totalMatching: filteredRows.length,
        returned: limitedRows.length,
      },
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

// function ensureRulesMeta(meta) {
//   const nextMeta = meta && typeof meta === "object" ? { ...meta } : {};
//   const rules =
//     nextMeta.rules && typeof nextMeta.rules === "object"
//       ? { ...nextMeta.rules }
//       : {};

//   const applied = Array.isArray(rules.applied) ? [...rules.applied] : [];

//   rules.applied = applied;
//   if (typeof rules.exclude !== "boolean") {
//     rules.exclude = false;
//   }

//   nextMeta.rules = rules;
//   return nextMeta;
// }

/** Update only rules-related fields, now in tbl_ptrs_ruleset */
async function updateRulesOnly({
  customerId,
  ptrsId,
  profileId = null,
  rowRules = [],
  crossRowRules = [],
  userId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // Blow away any existing rulesets for this run
    await db.PtrsRuleset.destroy({
      where: { customerId, ptrsId },
      transaction: t,
    });

    const payloads = [];

    if (Array.isArray(rowRules)) {
      for (const rule of rowRules) {
        const def = { ...rule, type: rule.type || "row" };
        payloads.push({
          customerId,
          profileId: profileId || null,
          ptrsId,
          scope: "row",
          name: def.label || null,
          description: def.description || null,
          isDefaultForProfile: false,
          definition: def,
          createdBy: userId || null,
          updatedBy: userId || null,
        });
      }
    }

    if (Array.isArray(crossRowRules)) {
      for (const rule of crossRowRules) {
        const def = { ...rule, type: rule.type || "crossRow" };
        payloads.push({
          customerId,
          profileId: profileId || null,
          ptrsId,
          scope: "crossRow",
          name: def.label || null,
          description: def.description || null,
          isDefaultForProfile: false,
          definition: def,
          createdBy: userId || null,
          updatedBy: userId || null,
        });
      }
    }

    if (payloads.length) {
      await db.PtrsRuleset.bulkCreate(payloads, {
        transaction: t,
        validate: false,
        returning: false,
      });
    }

    await t.commit();

    return {
      rowRules: Array.isArray(rowRules) ? rowRules : [],
      crossRowRules: Array.isArray(crossRowRules) ? crossRowRules : [],
    };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

/** Get rules for a ptrs run from tbl_ptrs_ruleset */
async function getRules({ customerId, ptrsId }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const rulesets = await db.PtrsRuleset.findAll({
      where: { customerId, ptrsId },
      transaction: t,
      raw: true,
    });
    await t.commit();

    const rowRules = [];
    const crossRowRules = [];

    for (const rs of rulesets || []) {
      const def = rs.definition;
      if (!def || typeof def !== "object") continue;
      const type = def.type || rs.scope || "row";
      if (type === "crossRow") {
        crossRowRules.push(def);
      } else {
        rowRules.push(def);
      }
    }

    slog.info(
      "PTRS v2 getRules: loaded rulesets",
      safeMeta({
        customerId,
        ptrsId,
        total: rulesets?.length || 0,
        rowRules: rowRules.length,
        crossRowRules: crossRowRules.length,
      })
    );

    return { rowRules, crossRowRules };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

/** Get rulesets for a profile/customer (sources for import) */
async function getProfileRules({ customerId, profileId = null }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const where = { customerId };
    if (profileId) where.profileId = profileId;

    const rulesets = await db.PtrsRuleset.findAll({
      where,
      transaction: t,
      raw: true,
    });

    await t.commit();

    slog.info(
      "PTRS v2 getProfileRules: loaded profile rulesets",
      safeMeta({
        customerId,
        profileId: profileId || null,
        count: rulesets?.length || 0,
      })
    );

    return rulesets || [];
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
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
    // 1) Load row-level rules from tbl_ptrs_ruleset
    const rowRules = await loadRowRulesForPtrs({
      customerId,
      ptrsId,
      transaction: t,
    });

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
    const rulesResult = applyRules(baseRows, rowRules);
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

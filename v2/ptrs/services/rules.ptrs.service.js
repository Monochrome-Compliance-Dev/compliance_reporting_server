// Helper to ensure cross-row rules are not dangerously broad
function validateCrossRowRule(rule) {
  const match = Array.isArray(rule?.target?.match) ? rule.target.match : [];
  const where = Array.isArray(rule?.target?.where) ? rule.target.where : [];

  // Safe if we have an explicit match key
  if (match.length > 0) return;

  // Safe if we have at least one positive constraint
  const hasPositiveWhere = where.some((w) => ["eq", "in"].includes(w?.op));

  if (!hasPositiveWhere) {
    const err = new Error(
      "Cross-row rule target scope is too broad. Add a match key or a positive target condition.",
    );
    err.statusCode = 400;
    throw err;
  }
}
const db = require("@/db/database");

const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { slog, safeMeta } = require("./ptrs.service");
const { loadMappedRowsForPtrs } = require("./tablesAndMaps.ptrs.service");

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

function _matches(row, cond) {
  if (!cond || typeof cond !== "object") return true;

  const { field, op, value } = cond;
  if (!field || !op) return true;

  const raw = row?.[field];
  const s = raw == null ? "" : String(raw);
  const v = value == null ? "" : String(value);

  const toNum = (x) => {
    const n = Number(String(x).replace(/[, ]+/g, ""));
    return Number.isFinite(n) ? n : NaN;
  };

  switch (op) {
    case "eq":
      return s === v;
    case "neq":
      return s !== v;
    case "gt":
      return toNum(raw) > toNum(v);
    case "gte":
      return toNum(raw) >= toNum(v);
    case "lt":
      return toNum(raw) < toNum(v);
    case "lte":
      return toNum(raw) <= toNum(v);
    case "in": {
      const parts = v
        .split(",")
        .map((p) => p.trim())
        .filter(Boolean);
      return parts.includes(s);
    }
    case "nin": {
      const parts = v
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
}

function _toNumLoose(v) {
  if (v == null || v === "") return 0;
  const n = Number(String(v).replace(/[, ]+/g, ""));
  return Number.isFinite(n) ? n : 0;
}

function _round(n, dp = 2) {
  const f = Math.pow(10, dp);
  return Math.round(n * f) / f;
}

function applyCrossRowRules(rows, rules = [], statsRef = null) {
  const enabled = (rules || []).filter((r) => r && r.enabled !== false);

  const stats =
    statsRef && typeof statsRef === "object"
      ? statsRef
      : { rulesTried: enabled.length, rowsAffected: 0, actions: 0 };

  if (!Array.isArray(rows) || !rows.length || !enabled.length) {
    return { rows: rows || [], stats };
  }

  // Build per-field indexes on demand: Map<fieldName, Map<key, row[]>>
  const indexes = new Map();

  const getIndexForField = (field) => {
    const f = String(field || "").trim();
    if (!f) return null;
    if (indexes.has(f)) return indexes.get(f);

    const byKey = new Map();
    for (const r of rows) {
      const k = String(r?.[f] ?? "").trim();
      if (!k) continue;
      if (!byKey.has(k)) byKey.set(k, []);
      byKey.get(k).push(r);
    }

    indexes.set(f, byKey);
    return byKey;
  };

  for (const rule of enabled) {
    const ruleKey = rule.id || rule.label || "crossRowRule";

    const when = Array.isArray(rule.when) ? rule.when : [];
    const match = Array.isArray(rule.target?.match) ? rule.target.match : [];
    const where = Array.isArray(rule.target?.where) ? rule.target.where : [];
    const match0 = match[0] || {};

    const currentField = match0.currentField;
    const targetField = match0.targetField;

    const action = rule.action || {};
    const op = (action.op || "add").toLowerCase();
    const targetAmountField = action.field;
    const currentAmountField = action.valueFieldFromCurrent;
    const dp = typeof action.round === "number" ? action.round : 2;

    // MVP validity checks — if misconfigured, skip
    if (
      !currentField ||
      !targetField ||
      !targetAmountField ||
      !currentAmountField
    ) {
      continue;
    }

    const targetIndex = getIndexForField(targetField);
    if (!targetIndex) continue;

    // Aggregate ET rows per key after filtering by `when`
    // 1. Select ET rows matching `when`
    const filteredCurrents = [];
    for (const row of rows) {
      const ok = when.length ? when.every((c) => _matches(row, c)) : true;
      if (!ok) continue;
      const key = String(row?.[currentField] ?? "").trim();
      if (!key) continue;
      filteredCurrents.push({ key, row });
    }

    // 2. Group by key (currentField)
    const groupedCurrents = new Map();
    for (const { key, row } of filteredCurrents) {
      if (!groupedCurrents.has(key)) {
        groupedCurrents.set(key, { totalDelta: 0, rows: [] });
      }
      const group = groupedCurrents.get(key);
      group.totalDelta += _toNumLoose(row[currentAmountField]);
      group.rows.push(row);
    }

    // 3. For each group, apply to targets
    for (const [key, group] of groupedCurrents.entries()) {
      const targets = targetIndex.get(key) || [];
      if (!targets.length) continue;

      for (const target of targets) {
        if (where.length && !where.every((c) => _matches(target, c))) continue;

        // ------------------------------------------------------------
        // Idempotency guard:
        // Apply cross-row adjustments against a stable baseline so the
        // same rule can be run multiple times without stacking deltas.
        // Baseline is stored per-row/per-field in `_rulesBaseline`.
        // ------------------------------------------------------------
        const baselineStore =
          target && typeof target === "object"
            ? target._rulesBaseline && typeof target._rulesBaseline === "object"
              ? target._rulesBaseline
              : {}
            : {};

        const hasBaseline = Object.prototype.hasOwnProperty.call(
          baselineStore,
          targetAmountField,
        );

        const baseline = hasBaseline
          ? _toNumLoose(baselineStore[targetAmountField])
          : _toNumLoose(target?.[targetAmountField]);

        // Persist baseline for future passes
        baselineStore[targetAmountField] = baseline;
        if (target && typeof target === "object") {
          target._rulesBaseline = baselineStore;
        }

        let next = baseline;
        if (op === "add") next = baseline + group.totalDelta;
        else if (op === "sub") next = baseline - group.totalDelta;
        else if (op === "mul") next = baseline * group.totalDelta;
        else if (op === "div")
          next =
            group.totalDelta === 0 ? baseline : baseline / group.totalDelta;
        else if (op === "assign") next = group.totalDelta;

        target[targetAmountField] = _round(next, dp);

        // Mark applied on target only once
        const tApplied = Array.isArray(target._appliedRules)
          ? target._appliedRules.slice()
          : [];
        if (!tApplied.includes(ruleKey)) tApplied.push(ruleKey);
        target._appliedRules = tApplied;

        stats.actions++;
        stats.rowsAffected++;
      }

      // Optionally also exclude current ET rows
      if (rule.alsoExcludeCurrent) {
        for (const current of group.rows) {
          const cApplied = Array.isArray(current._appliedRules)
            ? current._appliedRules.slice()
            : [];
          if (!cApplied.includes(ruleKey)) cApplied.push(ruleKey);
          current._appliedRules = cApplied;
          current.exclude = true;
          current.exclude_from_metrics = true;
          if (!current.exclude_comment)
            current.exclude_comment = "Excluded by cross-row rule";
        }
      }
    }
  }

  return { rows, stats };
}

// --- Helper: load row-level rules from PtrsRuleset ---
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
    if (type === "crossRow") continue;
    rowRules.push(def);
  }

  slog.info(
    "PTRS v2 loadRowRulesForPtrs",
    safeMeta({
      customerId,
      ptrsId,
      rulesetCount: rulesets?.length || 0,
      rowRules: rowRules.length,
    }),
  );

  return rowRules;
}

// --- NEW: load both row and cross-row rules for a PTRS run ---
async function loadRulesForPtrs({ customerId, ptrsId, transaction }) {
  const rulesets = await db.PtrsRuleset.findAll({
    where: { customerId, ptrsId },
    transaction,
    raw: true,
  });

  const rowRules = [];
  const crossRowRules = [];

  for (const rs of rulesets || []) {
    const def = rs.definition;
    if (!def || typeof def !== "object") continue;
    const type = def.type || rs.scope || "row";
    if (type === "crossRow") crossRowRules.push(def);
    else rowRules.push(def);
  }

  slog.info(
    "PTRS v2 loadRulesForPtrs",
    safeMeta({
      customerId,
      ptrsId,
      rulesetCount: rulesets?.length || 0,
      rowRules: rowRules.length,
      crossRowRules: crossRowRules.length,
    }),
  );

  return { rowRules, crossRowRules };
}

async function getRulesPreview({
  customerId,
  ptrsId,
  limit = 50,
  mode = "sample",
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const started = Date.now();
  const effectiveLimit = Math.min(Number(limit) || 50, 500);

  const t = await beginTransactionWithCustomerContext(customerId);

  // Helper to build numeric SQL for JSONB text values (handles commas / blanks)
  const numExpr = (jsonbExpr) =>
    `NULLIF(regexp_replace(${jsonbExpr}, '[^0-9\\.\\-]', '', 'g'), '')::numeric`;

  // Helper to build a WHERE clause fragment for JSONB "data" fields
  const buildJsonbCond = (prefix, cond) => {
    if (!cond || typeof cond !== "object") return null;
    const field = String(cond.field || "").trim();
    const op = String(cond.op || "").trim();
    if (!field || !op) return null;

    const expr = `${prefix}.data->>'${field}'`;

    switch (op) {
      case "eq":
        return `${expr} = :w_${field}`;
      case "neq":
        return `${expr} <> :w_${field}`;
      case "in":
        return `${expr} = ANY(:w_${field})`;
      case "nin":
        return `NOT (${expr} = ANY(:w_${field}))`;
      case "is_null":
        return `(${expr} IS NULL OR ${expr} = '')`;
      case "not_null":
        return `(${expr} IS NOT NULL AND ${expr} <> '')`;
      case "gt":
        return `${numExpr(expr)} > :w_${field}`;
      case "gte":
        return `${numExpr(expr)} >= :w_${field}`;
      case "lt":
        return `${numExpr(expr)} < :w_${field}`;
      case "lte":
        return `${numExpr(expr)} <= :w_${field}`;
      default:
        return null;
    }
  };

  const bindValueFor = (cond) => {
    const field = String(cond.field || "").trim();
    const op = String(cond.op || "").trim();
    const key = `w_${field}`;

    if (op === "in" || op === "nin") {
      const arr = String(cond.value || "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      return { key, value: arr };
    }

    if (op === "is_null" || op === "not_null") {
      return null;
    }

    return { key, value: cond.value != null ? cond.value : "" };
  };

  try {
    // Load rules (row + cross-row)
    const { rowRules, crossRowRules } = await loadRulesForPtrs({
      customerId,
      ptrsId,
      transaction: t,
    });

    // ------------------------------------------------------------------
    // FULL PREVIEW MODE
    // For cross-row rules, do a server-side aggregate preview across the
    // FULL staged dataset (tbl_ptrs_stage_row) so counts match Apply.
    // ------------------------------------------------------------------
    if (
      mode === "full" &&
      Array.isArray(crossRowRules) &&
      crossRowRules.length
    ) {
      const rule = crossRowRules[0]; // MVP: preview the first cross-row rule

      // Validate up-front (same semantics as apply/save)
      try {
        validateCrossRowRule(rule);
      } catch (e) {
        await t.rollback();
        throw e;
      }

      const when = Array.isArray(rule.when) ? rule.when : [];
      const match = Array.isArray(rule.target?.match) ? rule.target.match : [];
      const where = Array.isArray(rule.target?.where) ? rule.target.where : [];

      const match0 = match[0] || {};
      const currentField = String(match0.currentField || "").trim();
      const targetField = String(match0.targetField || "").trim();

      const action = rule.action || {};
      const op = String(action.op || "add").toLowerCase();
      const targetAmountField = String(action.field || "").trim();
      const currentAmountField = String(
        action.valueFieldFromCurrent || "",
      ).trim();
      const dp = typeof action.round === "number" ? action.round : 2;

      if (
        !currentField ||
        !targetField ||
        !targetAmountField ||
        !currentAmountField
      ) {
        await t.commit();
        return {
          meta: {
            ptrsId,
            mode,
            elapsedMs: Date.now() - started,
            isPartial: false,
          },
          summary: {
            rulesTried: crossRowRules.length,
            rowsAffected: 0,
            actions: 0,
          },
          headers: [],
          rows: [],
          byRule: {},
          examples: [],
          warning:
            "Cross-row rule is missing match fields or action fields; preview cannot be computed.",
        };
      }

      // Build WHERE fragments
      const wherePartsCurr = [];
      const wherePartsTgt = [];
      const replacements = {
        customerId: String(customerId),
        ptrsId: String(ptrsId),
      };

      for (const c of when) {
        const frag = buildJsonbCond("c", c);
        if (frag) {
          wherePartsCurr.push(frag);
          const bind = bindValueFor(c);
          if (bind) replacements[bind.key] = bind.value;
        }
      }

      for (const c of where) {
        const frag = buildJsonbCond("t", c);
        if (frag) {
          wherePartsTgt.push(frag);
          const bind = bindValueFor(c);
          if (bind) replacements[bind.key] = bind.value;
        }
      }

      const currWhereSql = wherePartsCurr.length
        ? `AND (${wherePartsCurr.join(" AND ")})`
        : "";

      const tgtWhereSql = wherePartsTgt.length
        ? `AND (${wherePartsTgt.join(" AND ")})`
        : "";

      const currKeyExpr = `c.data->>'${currentField}'`;
      const tgtKeyExpr = `t.data->>'${targetField}'`;
      const currAmtExpr = numExpr(`c.data->>'${currentAmountField}'`);
      const tgtAmtExpr = numExpr(`t.data->>'${targetAmountField}'`);

      const opSql =
        op === "add"
          ? `(${tgtAmtExpr} + curr.delta)`
          : op === "sub"
            ? `(${tgtAmtExpr} - curr.delta)`
            : op === "mul"
              ? `(${tgtAmtExpr} * curr.delta)`
              : op === "div"
                ? `CASE WHEN curr.delta = 0 THEN ${tgtAmtExpr} ELSE (${tgtAmtExpr} / curr.delta) END`
                : op === "assign"
                  ? `curr.delta`
                  : `(${tgtAmtExpr} + curr.delta)`;

      const sqlCount = `
WITH curr AS (
  SELECT
    ${currKeyExpr} AS k,
    SUM(COALESCE(${currAmtExpr}, 0)) AS delta
  FROM "tbl_ptrs_stage_row" c
  WHERE c."customerId" = :customerId
    AND c."ptrsId" = :ptrsId
    ${currWhereSql}
    AND COALESCE(${currKeyExpr}, '') <> ''
  GROUP BY 1
),
impacted AS (
  SELECT
    t."id",
    t."rowNo",
    t.data->>'document_type' AS document_type,
    ${tgtKeyExpr} AS ref,
    COALESCE(${tgtAmtExpr}, 0) AS base_before,
    curr.delta AS expected_delta,
    ROUND(${opSql}::numeric, ${dp}) AS would_be
  FROM "tbl_ptrs_stage_row" t
  JOIN curr ON curr.k = ${tgtKeyExpr}
  WHERE t."customerId" = :customerId
    AND t."ptrsId" = :ptrsId
    ${tgtWhereSql}
)
SELECT COUNT(*)::int AS count
FROM impacted;
`;

      const sqlExamples = `
WITH curr AS (
  SELECT
    ${currKeyExpr} AS k,
    SUM(COALESCE(${currAmtExpr}, 0)) AS delta
  FROM "tbl_ptrs_stage_row" c
  WHERE c."customerId" = :customerId
    AND c."ptrsId" = :ptrsId
    ${currWhereSql}
    AND COALESCE(${currKeyExpr}, '') <> ''
  GROUP BY 1
),
impacted AS (
  SELECT
    t."rowNo",
    t.data->>'document_type' AS document_type,
    ${tgtKeyExpr} AS ref,
    COALESCE(${tgtAmtExpr}, 0) AS base_before,
    curr.delta AS expected_delta,
    ROUND(${opSql}::numeric, ${dp}) AS would_be,
    t."updatedAt"
  FROM "tbl_ptrs_stage_row" t
  JOIN curr ON curr.k = ${tgtKeyExpr}
  WHERE t."customerId" = :customerId
    AND t."ptrsId" = :ptrsId
    ${tgtWhereSql}
)
SELECT *
FROM impacted
ORDER BY "updatedAt" DESC
LIMIT 20;
`;

      const [{ count } = { count: 0 }] = await db.sequelize.query(sqlCount, {
        transaction: t,
        replacements,
        type: db.sequelize.QueryTypes.SELECT,
      });

      const examples = await db.sequelize.query(sqlExamples, {
        transaction: t,
        replacements,
        type: db.sequelize.QueryTypes.SELECT,
      });

      await t.commit();

      return {
        meta: {
          ptrsId,
          mode,
          elapsedMs: Date.now() - started,
          isPartial: false,
        },
        summary: {
          rulesTried: crossRowRules.length,
          rowsAffected: Number(count || 0),
          actions: Number(count || 0),
        },
        headers: [],
        rows: [],
        byRule: {},
        examples: Array.isArray(examples) ? examples : [],
      };
    }

    // ------------------------------------------------------------------
    // SAMPLE PREVIEW MODE (existing behaviour)
    // ------------------------------------------------------------------

    // 1) Compose mapped rows (main import + joins)
    const { rows: baseRows, headers } = await loadMappedRowsForPtrs({
      customerId,
      ptrsId,
      limit: effectiveLimit,
      transaction: t,
    });

    const rowResult = applyRules(baseRows, rowRules);
    const rulesResult = applyCrossRowRules(
      rowResult.rows || baseRows,
      crossRowRules,
      rowResult.stats,
    );

    await t.commit();

    const stats = rulesResult.stats || {};
    const summary = {
      rulesTried: stats.rulesTried ?? 0,
      rowsAffected: stats.rowsAffected ?? 0,
      actions: stats.actions ?? 0,
    };

    const previewRows = Array.isArray(rulesResult.rows)
      ? rulesResult.rows
      : Array.isArray(rowResult.rows)
        ? rowResult.rows
        : baseRows;

    return {
      meta: {
        ptrsId,
        mode,
        previewLimit: effectiveLimit,
        elapsedMs: Date.now() - started,
        isPartial: true,
      },
      summary,
      headers,
      rows: previewRows,
      byRule: {},
      examples: [],
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
    // Load staged rows from tbl_ptrs_stage_row as the canonical dataset
    const stageRows = await db.PtrsStageRow.findAll({
      where: { customerId, ptrsId },
      transaction: t,
      raw: true,
    });

    // Flatten JSONB data for preview; assume "data" holds the mapped row
    const baseRows = (stageRows || []).map((r) => {
      const data =
        r && typeof r.data === "object" && r.data !== null ? r.data : {};
      return data;
    });

    const filteredRows = applySandboxFilters(baseRows, filters);
    const limitedRows = filteredRows.slice(0, effectiveLimit);

    const headers = baseRows.length > 0 ? Object.keys(baseRows[0]) : [];

    slog.info(
      "PTRS v2 sandboxRulesPreview",
      safeMeta({
        customerId,
        ptrsId,
        stageRows: stageRows.length,
        filters: Array.isArray(filters) ? filters.length : 0,
        totalMatching: filteredRows.length,
        returned: limitedRows.length,
      }),
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
        // Validate cross-row rule for excessive broadness
        validateCrossRowRule(rule);
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
      }),
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
      }),
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
  // If null/undefined, we pass null through, which loadMappedRowsForPtrs
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
      `PTRS v2 rules apply aborted: dataset has ${totalRows} rows which exceeds the safety limit of ${HARD_ROW_CAP}.`,
    );
    err.statusCode = 413; // FE-friendly soft-failure
    throw err;
  }
  // ------------------------------------------------------------

  try {
    // 1) Load row-level rules from tbl_ptrs_ruleset
    // 1) Load rules (row + cross-row) from tbl_ptrs_ruleset
    const { rowRules, crossRowRules } = await loadRulesForPtrs({
      customerId,
      ptrsId,
      transaction: t,
    });

    // 2) Compose mapped rows (main import + joins)
    const { rows: baseRows } = await loadMappedRowsForPtrs({
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
      baseRowCount: Array.isArray(baseRows) ? baseRows.length : 0,
      rowRules: Array.isArray(rowRules) ? rowRules.length : 0,
      crossRowRules: Array.isArray(crossRowRules) ? crossRowRules.length : 0,
    });

    // 3) Apply row rules first, then cross-row adjustments
    const rowResult = applyRules(baseRows, rowRules);
    const rulesResult = applyCrossRowRules(
      rowResult.rows || baseRows,
      crossRowRules,
      rowResult.stats,
    );

    const rows = rulesResult.rows || baseRows;

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
            // Captures baseline values used to keep cross-row rules idempotent.
            // Safe to omit when not present.
            baseline:
              r && typeof r === "object" && r._rulesBaseline
                ? r._rulesBaseline
                : null,
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

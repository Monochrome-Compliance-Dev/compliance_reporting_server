const db = require("@/db/database");

const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { slog, safeMeta } = require("./ptrs.service");
const { loadMappedRowsForPtrs } = require("./maps.ptrs.service");
const {
  applyRowRulesSql,
  buildRowRulesProjectionSql: importedBuildRowRulesProjectionSql,
} = require("./rules.row.sql");
const { applyCrossRowRulesSql } = require("./rules.crossRow.sql");

module.exports = {
  applyRules,
  getRulesPreview,
  applyRulesAndPersist,
  updateRulesOnly,
  getRules,
  getProfileRules,
  sandboxRulesPreview,
};

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

function normaliseSelectedGroupNames(groupName = null) {
  if (Array.isArray(groupName)) {
    return groupName.map((v) => String(v || "").trim()).filter(Boolean);
  }

  const single = String(groupName || "").trim();
  return single ? [single] : [];
}

function ruleMatchesSelectedGroups(rule, selectedGroupNames = []) {
  if (!Array.isArray(selectedGroupNames) || !selectedGroupNames.length) {
    return true;
  }

  const ruleGroupName = String(rule?.groupName || "").trim();
  return selectedGroupNames.includes(ruleGroupName);
}

function collectSubmittedGroupNames({ rowRules = [], crossRowRules = [] }) {
  return Array.from(
    new Set(
      [
        ...(Array.isArray(rowRules) ? rowRules : []),
        ...(Array.isArray(crossRowRules) ? crossRowRules : []),
      ]
        .map((rule) => String(rule?.groupName || "").trim())
        .filter(Boolean),
    ),
  );
}

function buildPreviewRowProjectionSql({ rules = [], sourceExpr = "data" }) {
  if (typeof importedBuildRowRulesProjectionSql === "function") {
    return importedBuildRowRulesProjectionSql({ rules, sourceExpr });
  }

  let expr = sourceExpr;

  const jsonPathFor = (field) => `'{${String(field || "").trim()}}'`;
  const textExprFor = (field, currentExpr) =>
    `COALESCE(${currentExpr}->>'${String(field || "").trim()}', '')`;

  for (const rule of Array.isArray(rules) ? rules : []) {
    const actions = Array.isArray(rule?.then) ? rule.then : [];
    for (const action of actions) {
      if (!action || action.op !== "concat_fields") continue;

      const targetField = String(action.field || "").trim();
      const segments = Array.isArray(action.segments) ? action.segments : [];
      if (!targetField || !segments.length) continue;

      const concatSql = segments
        .map((seg) => {
          if (seg?.kind === "literal") {
            return db.sequelize.escape(String(seg?.value || ""));
          }
          const fieldName = String(seg?.name || "").trim();
          if (!fieldName) {
            return db.sequelize.escape("");
          }
          return textExprFor(fieldName, expr);
        })
        .join(" || ");

      expr = `jsonb_set(${expr}, ${jsonPathFor(targetField)}, to_jsonb(${concatSql}), true)`;
    }
  }

  return expr;
}

// Merge preview headers from both the existing headers and the preview rows
function mergePreviewHeadersFromRows(existingHeaders = [], rows = []) {
  const base = Array.isArray(existingHeaders) ? existingHeaders : [];
  const seen = new Set(base.map((h) => String(h || "")).filter(Boolean));
  const merged = [...base];

  for (const row of Array.isArray(rows) ? rows : []) {
    if (!row || typeof row !== "object") continue;

    for (const key of Object.keys(row)) {
      const nextKey = String(key || "").trim();
      if (!nextKey || seen.has(nextKey)) continue;
      seen.add(nextKey);
      merged.push(nextKey);
    }
  }

  return merged;
}

async function loadStageRowsForRulesPreview({
  customerId,
  ptrsId,
  transaction,
  limit = 20000,
}) {
  const rows = await db.sequelize.query(
    `
      SELECT
        s."rowNo",
        s."data",
        s."meta",
        s."updatedAt"
      FROM "tbl_ptrs_stage_row" s
      WHERE s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
      ORDER BY s."rowNo" ASC
      LIMIT :limit
    `,
    {
      transaction,
      replacements: {
        customerId,
        ptrsId,
        limit: Math.max(Number(limit) || 20000, 1),
      },
      type: db.sequelize.QueryTypes.SELECT,
    },
  );

  return (Array.isArray(rows) ? rows : []).map((r) => {
    const data =
      r && typeof r.data === "object" && r.data !== null ? r.data : {};
    const meta =
      r && typeof r.meta === "object" && r.meta !== null ? r.meta : {};

    return {
      ...data,
      rowNo: Number(r?.rowNo || data?.rowNo || data?.row_no || 0) || null,
      row_no: Number(r?.rowNo || data?.rowNo || data?.row_no || 0) || null,
      _meta: meta,
      updatedAt: r?.updatedAt || null,
    };
  });
}

function previewGetText(row, keys = []) {
  for (const key of Array.isArray(keys) ? keys : []) {
    const value = row?.[key];
    if (value == null) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return "";
}

function previewGetNumber(row, keys = []) {
  for (const key of Array.isArray(keys) ? keys : []) {
    const value = row?.[key];
    if (value == null || value === "") continue;
    const n = Number(String(value).replace(/[, ]+/g, ""));
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function previewApproxEqual(a, b, epsilon = 0.000001) {
  return Math.abs(Number(a || 0) - Number(b || 0)) <= epsilon;
}

function previewRound(value, dp = 2) {
  const factor = 10 ** Number(dp || 2);
  return Math.round(Number(value || 0) * factor) / factor;
}

function previewMatchesCond(row, cond) {
  if (!cond || typeof cond !== "object") return true;

  const field = String(cond.field || "").trim();
  const op = String(cond.op || "").trim();
  if (!field || !op) return true;

  const raw = row?.[field];
  const s = raw == null ? "" : String(raw);
  const v = cond.value == null ? "" : String(cond.value);

  const toNum = (x) => {
    const n = Number(String(x).replace(/[, ]+/g, ""));
    return Number.isFinite(n) ? n : NaN;
  };

  switch (op) {
    case "eq":
      return s === v;
    case "neq":
      return s !== v;
    case "starts_with":
      return s.startsWith(v);
    case "ends_with":
      return s.endsWith(v);
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

function buildBestMatchPairingPreview({
  rows = [],
  rule,
  previewPageSize = 30,
  effectivePage = 1,
}) {
  const when = Array.isArray(rule?.when) ? rule.when : [];
  const match = Array.isArray(rule?.target?.match) ? rule.target.match : [];
  const where = Array.isArray(rule?.target?.where) ? rule.target.where : [];
  const match0 = match[0] || {};
  const currentField = String(match0.currentField || "").trim();
  const targetField = String(match0.targetField || "").trim();
  const action = rule?.action || {};
  const op = String(action.op || "add").toLowerCase();
  const targetAmountField = String(action.field || "").trim();
  const currentAmountField = String(
    action.valueFieldFromCurrent || action.valueField || "",
  ).trim();
  const dp = typeof action.round === "number" ? action.round : 2;

  const excludeUnmatchedCurrent =
    rule?.target?.excludeUnmatchedCurrent === true;
  const unmatchedComment = String(
    rule?.target?.unmatchedComment ||
      action.unmatchedComment ||
      `Excluded by cross-row rule — no matching invoice found for credit pairing (${rule.label || rule.id || "crossRowRule"})`,
  ).trim();
  const zeroInvoiceComment = `Excluded by cross-row rule — zero balance after pairing (${rule.label || rule.id || "crossRowRule"})`;
  const excludeReason = "CROSS_ROW_RULE";

  const currentRows = (Array.isArray(rows) ? rows : [])
    .filter(
      (row) =>
        !(
          row?.exclude === true ||
          row?.exclude_from_metrics === true ||
          row?._meta?.exclusions?.exclude === true ||
          row?._meta?.exclusions?.exclude_from_metrics === true
        ),
    )
    .filter((row) => when.every((cond) => previewMatchesCond(row, cond)))
    .map((row, idx) => ({
      row,
      idx,
      rowNo: Number(row?.row_no || row?.rowNo || idx + 1),
      key: previewGetText(row, [currentField]),
      amount: previewGetNumber(row, [currentAmountField]),
      purchasingDocumentNo: previewGetText(row, [
        "purchasing_document_no",
        "Purchasing Document No",
      ]),
      dueDate: previewGetText(row, ["invoice_due_date", "Net Due Date"]),
    }))
    .filter((entry) => entry.key && Number.isFinite(entry.amount))
    .sort((a, b) => a.rowNo - b.rowNo);

  const targetRows = (Array.isArray(rows) ? rows : [])
    .filter(
      (row) =>
        !(
          row?.exclude === true ||
          row?.exclude_from_metrics === true ||
          row?._meta?.exclusions?.exclude === true ||
          row?._meta?.exclusions?.exclude_from_metrics === true
        ),
    )
    .filter((row) => where.every((cond) => previewMatchesCond(row, cond)))
    .map((row, idx) => ({
      row,
      idx,
      rowNo: Number(row?.row_no || row?.rowNo || idx + 1),
      key: previewGetText(row, [targetField]),
      amount: previewGetNumber(row, [targetAmountField]),
      purchasingDocumentNo: previewGetText(row, [
        "purchasing_document_no",
        "Purchasing Document No",
      ]),
      dueDate: previewGetText(row, ["invoice_due_date", "Net Due Date"]),
      claimed: false,
    }))
    .filter((entry) => entry.key && Number.isFinite(entry.amount))
    .sort((a, b) => a.rowNo - b.rowNo);

  const targetsByKey = new Map();
  for (const entry of targetRows) {
    const bucket = targetsByKey.get(entry.key) || [];
    bucket.push(entry);
    targetsByKey.set(entry.key, bucket);
  }

  const examples = [];
  let unmatchedKeys = 0;

  for (const current of currentRows) {
    const available = (targetsByKey.get(current.key) || []).filter(
      (target) => !target.claimed,
    );

    if (!available.length) {
      unmatchedKeys += 1;
      if (excludeUnmatchedCurrent) {
        examples.push({
          ref: current.key,
          document_type:
            previewGetText(current.row, ["document_type"]) || "Credit",
          payee_entity_name: previewGetText(current.row, [
            "payee_entity_name",
            "Account  Name",
            "account_name",
          ]),
          base_before: current.amount,
          expected_delta: current.amount,
          would_be: null,
          exclude_reason: excludeReason,
          exclude_comment: unmatchedComment,
          updatedAt: current.row?.updatedAt || null,
        });
      }
      continue;
    }

    const currentAbsAmount = Math.abs(current.amount);

    const ranked = available
      .map((target) => ({
        target,
        exactAmountMatch: previewApproxEqual(target.amount, currentAbsAmount)
          ? 1
          : 0,
        samePurchasingDoc:
          current.purchasingDocumentNo &&
          target.purchasingDocumentNo &&
          current.purchasingDocumentNo === target.purchasingDocumentNo
            ? 1
            : 0,
        sameDueDate:
          current.dueDate &&
          target.dueDate &&
          current.dueDate === target.dueDate
            ? 1
            : 0,
      }))
      .sort((a, b) => {
        if (b.exactAmountMatch !== a.exactAmountMatch) {
          return b.exactAmountMatch - a.exactAmountMatch;
        }
        if (b.samePurchasingDoc !== a.samePurchasingDoc) {
          return b.samePurchasingDoc - a.samePurchasingDoc;
        }
        if (b.sameDueDate !== a.sameDueDate) {
          return b.sameDueDate - a.sameDueDate;
        }
        return a.target.rowNo - b.target.rowNo;
      });

    const best = ranked[0]?.target;
    if (!best) {
      unmatchedKeys += 1;
      if (excludeUnmatchedCurrent) {
        examples.push({
          ref: current.key,
          document_type:
            previewGetText(current.row, ["document_type"]) || "Credit",
          payee_entity_name: previewGetText(current.row, [
            "payee_entity_name",
            "Account  Name",
            "account_name",
          ]),
          base_before: current.amount,
          expected_delta: current.amount,
          would_be: null,
          exclude_reason: excludeReason,
          exclude_comment: unmatchedComment,
          updatedAt: current.row?.updatedAt || null,
        });
      }
      continue;
    }

    let wouldBe = best.amount;
    if (op === "add") wouldBe = best.amount + current.amount;
    else if (op === "sub") wouldBe = best.amount - current.amount;
    else if (op === "mul") wouldBe = best.amount * current.amount;
    else if (op === "div") {
      wouldBe =
        current.amount === 0 ? best.amount : best.amount / current.amount;
    } else if (op === "assign") {
      wouldBe = current.amount;
    }
    wouldBe = previewRound(wouldBe, dp);

    best.claimed = true;

    examples.push({
      ref: best.key,
      document_type: previewGetText(best.row, ["document_type"]) || "Invoice",
      payee_entity_name: previewGetText(best.row, [
        "payee_entity_name",
        "Account  Name",
        "account_name",
      ]),
      base_before: best.amount,
      expected_delta: current.amount,
      would_be: wouldBe,
      exclude_reason: previewApproxEqual(wouldBe, 0) ? excludeReason : "",
      exclude_comment: previewApproxEqual(wouldBe, 0) ? zeroInvoiceComment : "",
      updatedAt: best.row?.updatedAt || null,
    });
  }

  examples.sort((a, b) => {
    const at = new Date(a?.updatedAt || 0).getTime();
    const bt = new Date(b?.updatedAt || 0).getTime();
    return bt - at;
  });

  const total = examples.length;
  const offset =
    (Math.max(Number(effectivePage) || 1, 1) - 1) *
    Math.max(Number(previewPageSize) || 30, 1);
  const pagedExamples = examples.slice(
    offset,
    offset + Math.max(Number(previewPageSize) || 30, 1),
  );

  return {
    count: total,
    ambiguousTargetRows: 0,
    ambiguousKeys: 0,
    unmatchedKeys,
    examples: pagedExamples,
    total,
  };
}

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
    case "starts_with":
      return s.startsWith(v);
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

function _appendRuleComment(existing, next) {
  const current = String(existing ?? "").trim();
  const incoming = String(next ?? "").trim();

  if (!incoming) return current;
  if (!current) return incoming;
  return `${current} | ${incoming}`;
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

function _applyAction(row, act) {
  if (!row || !act || typeof act !== "object") return;

  if (act?.op === "concat_fields") {
    const targetField = String(act.field || "").trim();
    const segments = Array.isArray(act.segments)
      ? act.segments
      : Array.isArray(act.fields)
        ? act.fields.map((name) => ({ kind: "field", name }))
        : [];

    if (!targetField || !segments.length) return;

    row[targetField] = segments
      .map((seg) => {
        if (seg?.kind === "literal") return String(seg?.value || "");
        const fieldName = String(seg?.name || "").trim();
        return fieldName ? String(row?.[fieldName] ?? "") : "";
      })
      .join("");
    return;
  }

  const op = String(act.op || "assign").toLowerCase();
  const targetField = String(act.field || "").trim();
  const sourceField = String(
    act.valueField || act.valueFieldFromCurrent || "",
  ).trim();
  const dp = typeof act.round === "number" ? act.round : 2;

  if (!targetField) return;

  if (op === "assign") {
    row[targetField] = sourceField ? (row?.[sourceField] ?? null) : null;
    return;
  }

  const currentVal = _toNumLoose(row?.[targetField]);
  const sourceVal = sourceField ? _toNumLoose(row?.[sourceField]) : 0;

  let next = currentVal;
  if (op === "add") next = currentVal + sourceVal;
  else if (op === "sub") next = currentVal - sourceVal;
  else if (op === "mul") next = currentVal * sourceVal;
  else if (op === "div")
    next = sourceVal === 0 ? currentVal : currentVal / sourceVal;
  else return;

  row[targetField] = _round(next, dp);
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

        if (action.comment || action.note) {
          const nextComment = action.comment || action.note;
          target.exclude_comment = _appendRuleComment(
            target.exclude_comment,
            nextComment,
          );
        }

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
          current.exclude_comment = _appendRuleComment(
            current.exclude_comment,
            "Excluded by cross-row rule",
          );
        }
      }
    }
  }

  return { rows, stats };
}

// --- Helper: load row-level rules from PtrsRuleset ---
async function loadRowRulesForPtrs({
  customerId,
  ptrsId,
  transaction,
  groupName = null,
}) {
  const rulesets = await db.PtrsRuleset.findAll({
    where: { customerId, ptrsId, deletedAt: null },
    transaction,
    raw: true,
  });

  const selectedGroupNames = normaliseSelectedGroupNames(groupName);
  const rowRules = [];
  for (const rs of rulesets || []) {
    const def = rs.definition;
    if (!def || typeof def !== "object") continue;
    const type = def.type || rs.scope || "row";
    if (type === "crossRow") continue;
    if (!ruleMatchesSelectedGroups(def, selectedGroupNames)) continue;
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
async function loadRulesForPtrs({
  customerId,
  ptrsId,
  transaction,
  groupName = null,
}) {
  const rulesets = await db.PtrsRuleset.findAll({
    where: { customerId, ptrsId, deletedAt: null },
    transaction,
    raw: true,
  });

  const selectedGroupNames = normaliseSelectedGroupNames(groupName);
  const rowRules = [];
  const crossRowRules = [];

  for (const rs of rulesets || []) {
    const def = rs.definition;
    if (!def || typeof def !== "object") continue;
    if (!ruleMatchesSelectedGroups(def, selectedGroupNames)) continue;
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

// -----------------------------------------------------------------------------

async function getRulesPreview({
  customerId,
  ptrsId,
  limit = 50,
  page = 1,
  mode = "sample",
  groupName = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const started = Date.now();
  const effectiveLimit = Math.min(Number(limit) || 50, 500);
  const previewPageSize = Math.min(Number(limit) || 30, 30);
  const effectivePage = Math.max(Number(page) || 1, 1);
  const effectiveOffset = (effectivePage - 1) * previewPageSize;

  const t = await beginTransactionWithCustomerContext(customerId);

  // Helper to build numeric SQL for JSONB text values (handles commas / blanks)
  const numExpr = (jsonbExpr) =>
    `NULLIF(regexp_replace(${jsonbExpr}, '[^0-9\\.\\-]', '', 'g'), '')::numeric`;

  // Helper to build a WHERE clause fragment for JSONB "data" fields
  const buildJsonbCond = (prefix, cond, key) => {
    if (!cond || typeof cond !== "object") return null;
    const field = String(cond.field || "").trim();
    const op = String(cond.op || "").trim();
    if (!field || !op) return null;

    const expr = `${prefix}.data->>'${field}'`;

    switch (op) {
      case "eq":
        return `${expr} = :${key}`;
      case "neq":
        return `${expr} <> :${key}`;
      case "starts_with":
        return `${expr} LIKE :${key}`;
      case "ends_with":
        return `${expr} LIKE :${key}`;
      case "in":
        return `${expr} = ANY(:${key})`;
      case "nin":
        return `NOT (${expr} = ANY(:${key}))`;
      case "gt":
        return `${numExpr(expr)} > :${key}`;
      case "gte":
        return `${numExpr(expr)} >= :${key}`;
      case "lt":
        return `${numExpr(expr)} < :${key}`;
      case "lte":
        return `${numExpr(expr)} <= :${key}`;
      default:
        return null;
    }
  };

  const bindValueFor = (cond, key) => {
    const op = String(cond.op || "").trim();

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

    if (op === "starts_with") {
      return { key, value: `${cond.value != null ? cond.value : ""}%` };
    }

    if (op === "ends_with") {
      return { key, value: `%${cond.value != null ? cond.value : ""}` };
    }

    return { key, value: cond.value != null ? cond.value : "" };
  };

  try {
    // Load rules (row + cross-row)
    const { rowRules, crossRowRules } = await loadRulesForPtrs({
      customerId,
      ptrsId,
      transaction: t,
      groupName,
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
      const helperRowRules = Array.isArray(rowRules)
        ? rowRules.filter(
            (r) =>
              r &&
              (r.type || "row") !== "crossRow" &&
              Array.isArray(r.then) &&
              r.then.some((a) => a && a.op === "concat_fields"),
          )
        : [];

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
      const targetSelection = String(rule?.target?.selection || "")
        .trim()
        .toLowerCase();
      const requireTargetMatch = rule?.target?.requireMatch !== false;

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

      if (targetSelection === "best_match_pairing") {
        const fullPreviewRows = await loadStageRowsForRulesPreview({
          customerId,
          ptrsId,
          limit: 20000,
          transaction: t,
        });

        const previewRowsAfterRowRules = fullPreviewRows;

        const pairingPreview = buildBestMatchPairingPreview({
          rows: previewRowsAfterRowRules,
          rule,
          previewPageSize,
          effectivePage,
        });

        await t.commit();

        return {
          meta: {
            ptrsId,
            mode,
            groupName,
            elapsedMs: Date.now() - started,
            isPartial: false,
            helperRowRulesMaterialised: helperRowRules.length,
            previewSource: "staged_rows",
            targetSelection,
            requireTargetMatch,
            previewPage: effectivePage,
            previewPageSize,
          },
          summary: {
            rulesTried: crossRowRules.length,
            rowsAffected: Number(pairingPreview?.count || 0),
            actions: Number(pairingPreview?.count || 0),
            ambiguousTargetRows: Number(
              pairingPreview?.ambiguousTargetRows || 0,
            ),
            ambiguousKeys: Number(pairingPreview?.ambiguousKeys || 0),
            unmatchedKeys: Number(pairingPreview?.unmatchedKeys || 0),
          },
          warning: null,
          headers: [],
          rows: [],
          byRule: {},
          examples: Array.isArray(pairingPreview?.examples)
            ? pairingPreview.examples
            : [],
          examplesPagination: {
            page: effectivePage,
            limit: previewPageSize,
            total: Number(pairingPreview?.total || 0),
            totalPages: Math.max(
              1,
              Math.ceil(Number(pairingPreview?.total || 0) / previewPageSize),
            ),
          },
        };
      }

      // Build WHERE fragments
      const wherePartsCurr = [];
      const wherePartsTgt = [];
      const replacements = {
        customerId: String(customerId),
        ptrsId: String(ptrsId),
        previewPageSize,
        effectiveOffset,
      };
      const helperProjectionSql = buildPreviewRowProjectionSql({
        rules: helperRowRules,
        sourceExpr: 's."data"',
      });

      const baseRowsCteSql = helperProjectionSql
        ? `
base_rows AS (
  SELECT
    s."id",
    s."rowNo",
    ${helperProjectionSql} AS data,
    s."meta",
    s."updatedAt"
  FROM "tbl_ptrs_stage_row" s
  WHERE s."customerId" = :customerId
    AND s."ptrsId" = :ptrsId
    AND s."deletedAt" IS NULL
),`
        : `
base_rows AS (
  SELECT
    s."id",
    s."rowNo",
    s."data",
    s."meta",
    s."updatedAt"
  FROM "tbl_ptrs_stage_row" s
  WHERE s."customerId" = :customerId
    AND s."ptrsId" = :ptrsId
    AND s."deletedAt" IS NULL
),`;

      for (const [idx, c] of when.entries()) {
        const key = `w_curr_${idx}`;
        const frag = buildJsonbCond("c", c, key);
        if (frag) {
          wherePartsCurr.push(frag);
          const bind = bindValueFor(c, key);
          if (bind) replacements[bind.key] = bind.value;
        }
      }

      for (const [idx, c] of where.entries()) {
        const key = `w_tgt_${idx}`;
        const frag = buildJsonbCond("t", c, key);
        if (frag) {
          wherePartsTgt.push(frag);
          const bind = bindValueFor(c, key);
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

      const strictOpSql =
        op === "add"
          ? `(m.base_before + m.delta)`
          : op === "sub"
            ? `(m.base_before - m.delta)`
            : op === "mul"
              ? `(m.base_before * m.delta)`
              : op === "div"
                ? `CASE WHEN m.delta = 0 THEN m.base_before ELSE (m.base_before / m.delta) END`
                : op === "assign"
                  ? `m.delta`
                  : `(m.base_before + m.delta)`;

      const sqlCount =
        targetSelection === "single_per_key"
          ? `
WITH ${baseRowsCteSql}
curr AS (
  SELECT
    ${currKeyExpr} AS k,
    SUM(COALESCE(${currAmtExpr}, 0)) AS delta
  FROM base_rows c
  WHERE 1=1
    ${currWhereSql}
    AND COALESCE(${currKeyExpr}, '') <> ''
  GROUP BY 1
),
eligible_targets AS (
    SELECT
    t."id",
    t."rowNo",
    t.data->>'document_type' AS document_type,
        COALESCE(t.data->>'payee_entity_name', t.data->>'Account  Name', t.data->>'account_name') AS supplier_name,
    ${tgtKeyExpr} AS k,
    COALESCE(${tgtAmtExpr}, 0) AS base_before,
    t."updatedAt"
  FROM base_rows t
  WHERE 1=1
    ${tgtWhereSql}
    AND COALESCE(${tgtKeyExpr}, '') <> ''
),
matched AS (
  SELECT
    et."id",
    et."rowNo",
    et.document_type,
    et.supplier_name,
    et.k,
    et.base_before,
    et."updatedAt",
    curr.delta
  FROM eligible_targets et
  JOIN curr ON curr.k = et.k
),
target_counts AS (
  SELECT
    k,
    COUNT(*)::int AS target_count
  FROM matched
  GROUP BY 1
),
impacted AS (
  SELECT
    m."rowNo",
    m.document_type,
    m.supplier_name,
    m.k AS ref,
    m.base_before,
    m.delta AS expected_delta,
    ROUND(${strictOpSql}::numeric, ${dp}) AS would_be,
    m."updatedAt"
  FROM matched m
  JOIN target_counts tc ON tc.k = m.k
  WHERE tc.target_count = 1
),
ambiguous AS (
  SELECT COUNT(*)::int AS count
  FROM target_counts
  WHERE target_count > 1
),
unmatched AS (
  SELECT COUNT(*)::int AS count
  FROM curr c
  LEFT JOIN target_counts tc ON tc.k = c.k
  WHERE tc.k IS NULL
)
SELECT
  (SELECT COUNT(*)::int FROM impacted) AS count,
  (SELECT COALESCE(SUM(target_count - 1), 0)::int FROM target_counts WHERE target_count > 1) AS ambiguousTargetRows,
  (SELECT COALESCE(COUNT(*), 0)::int FROM target_counts WHERE target_count > 1) AS ambiguousKeys,
  (SELECT count FROM unmatched) AS unmatchedKeys
FROM ambiguous;
`
          : `
WITH ${baseRowsCteSql}
curr AS (
  SELECT
    ${currKeyExpr} AS k,
    SUM(COALESCE(${currAmtExpr}, 0)) AS delta
  FROM base_rows c
  WHERE 1=1
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
  FROM base_rows t
  JOIN curr ON curr.k = ${tgtKeyExpr}
  WHERE 1=1
    ${tgtWhereSql}
)
SELECT
  COUNT(*)::int AS count,
  0::int AS "ambiguousTargetRows",
  0::int AS "ambiguousKeys",
  0::int AS "unmatchedKeys"
FROM impacted;
`;

      const sqlExamples =
        targetSelection === "single_per_key"
          ? `
WITH ${baseRowsCteSql}
curr AS (
  SELECT
    ${currKeyExpr} AS k,
    SUM(COALESCE(${currAmtExpr}, 0)) AS delta
  FROM base_rows c
  WHERE 1=1
    ${currWhereSql}
    AND COALESCE(${currKeyExpr}, '') <> ''
  GROUP BY 1
),
eligible_targets AS (
    SELECT
    t."id",
    t."rowNo",
    t.data->>'document_type' AS document_type,
    t.data->>'payee_entity_name' AS payee_entity_name,
    ${tgtKeyExpr} AS k,
    COALESCE(${tgtAmtExpr}, 0) AS base_before,
    t."updatedAt"
  FROM base_rows t
  WHERE 1=1
    ${tgtWhereSql}
    AND COALESCE(${tgtKeyExpr}, '') <> ''
),
matched AS (
    SELECT
    et."id",
    et."rowNo",
    et.document_type,
    et.payee_entity_name,
    et.k,
    et.base_before,
    et."updatedAt",
    curr.delta
  FROM eligible_targets et
  JOIN curr ON curr.k = et.k
),
target_counts AS (
  SELECT
    k,
    COUNT(*)::int AS target_count
  FROM matched
  GROUP BY 1
),
impacted AS (
  SELECT
    m."rowNo",
    m.document_type,
    m.payee_entity_name,
    m.k AS ref,
    m.base_before,
    m.delta AS expected_delta,
    ROUND(${strictOpSql}::numeric, ${dp}) AS would_be,
    m."updatedAt"
  FROM matched m
  JOIN target_counts tc ON tc.k = m.k
  WHERE tc.target_count = 1
)
SELECT *
FROM impacted
ORDER BY "updatedAt" DESC
LIMIT :previewPageSize OFFSET :effectiveOffset;
`
          : `
WITH ${baseRowsCteSql}
curr AS (
  SELECT
    ${currKeyExpr} AS k,
    SUM(COALESCE(${currAmtExpr}, 0)) AS delta
  FROM base_rows c
  WHERE 1=1
    ${currWhereSql}
    AND COALESCE(${currKeyExpr}, '') <> ''
  GROUP BY 1
),
impacted AS (
    SELECT
    t."rowNo",
    t.data->>'document_type' AS document_type,
    t.data->>'payee_entity_name' AS payee_entity_name,
    ${tgtKeyExpr} AS ref,
    COALESCE(${tgtAmtExpr}, 0) AS base_before,
    curr.delta AS expected_delta,
    ROUND(${opSql}::numeric, ${dp}) AS would_be,
    t."updatedAt"
  FROM base_rows t
  JOIN curr ON curr.k = ${tgtKeyExpr}
  WHERE 1=1
    ${tgtWhereSql}
)
SELECT *
FROM impacted
ORDER BY "updatedAt" DESC
LIMIT :previewPageSize OFFSET :effectiveOffset;
`;

      const [
        countRow = {
          count: 0,
          ambiguousTargetRows: 0,
          ambiguousKeys: 0,
          unmatchedKeys: 0,
        },
      ] = await db.sequelize.query(sqlCount, {
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
          groupName,
          elapsedMs: Date.now() - started,
          isPartial: false,
          helperRowRulesMaterialised: helperRowRules.length,
          targetSelection: targetSelection || "first_match",
          requireTargetMatch,
          previewPage: effectivePage,
          previewPageSize,
        },
        summary: {
          rulesTried: crossRowRules.length,
          rowsAffected: Number(countRow?.count || 0),
          actions: Number(countRow?.count || 0),
          ambiguousTargetRows: Number(countRow?.ambiguousTargetRows || 0),
          ambiguousKeys: Number(countRow?.ambiguousKeys || 0),
          unmatchedKeys: Number(countRow?.unmatchedKeys || 0),
        },
        warning:
          targetSelection === "single_per_key" &&
          requireTargetMatch &&
          Number(countRow?.ambiguousKeys || 0) > 0
            ? "Some matched keys have multiple eligible target rows and are excluded from strict preview results."
            : null,
        headers: [],
        rows: [],
        byRule: {},
        examples: Array.isArray(examples) ? examples : [],
        examplesPagination: {
          page: effectivePage,
          limit: previewPageSize,
          total: Number(countRow?.count || 0),
          totalPages: Math.max(
            1,
            Math.ceil(Number(countRow?.count || 0) / previewPageSize),
          ),
        },
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

    const previewHeaders = mergePreviewHeadersFromRows(headers, previewRows);

    return {
      meta: {
        ptrsId,
        mode,
        groupName,
        previewLimit: effectiveLimit,
        elapsedMs: Date.now() - started,
        isPartial: true,
      },
      summary,
      headers: previewHeaders,
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
      case "starts_with":
        return s.startsWith(val);
      case "ends_with":
        return s.endsWith(val);
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
  const SANDBOX_ROW_CAP = 10000;

  const numExpr = (jsonbExpr) =>
    `NULLIF(regexp_replace(${jsonbExpr}, '[^0-9\\.\\-]', '', 'g'), '')::numeric`;

  const buildJsonbCond = (prefix, cond, key) => {
    if (!cond || typeof cond !== "object") return null;
    const field = String(cond.field || "").trim();
    const op = String(cond.op || "").trim();
    if (!field || !op) return null;

    const expr = `${prefix}."data"->>'${field}'`;

    switch (op) {
      case "eq":
        return `${expr} = :${key}`;
      case "neq":
        return `${expr} <> :${key}`;
      case "starts_with":
        return `${expr} LIKE :${key}`;
      case "ends_with":
        return `${expr} LIKE :${key}`;
      case "in":
        return `${expr} = ANY(:${key})`;
      case "nin":
        return `NOT (${expr} = ANY(:${key}))`;
      case "gt":
        return `${numExpr(expr)} > :${key}`;
      case "gte":
        return `${numExpr(expr)} >= :${key}`;
      case "lt":
        return `${numExpr(expr)} < :${key}`;
      case "lte":
        return `${numExpr(expr)} <= :${key}`;
      case "is_null":
        return `(${expr} IS NULL OR ${expr} = '')`;
      case "not_null":
        return `(${expr} IS NOT NULL AND ${expr} <> '')`;
      default:
        return null;
    }
  };

  const bindValueFor = (cond, key) => {
    const op = String(cond?.op || "").trim();

    if (op === "in" || op === "nin") {
      return {
        key,
        value: String(cond?.value || "")
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean),
      };
    }

    if (op === "is_null" || op === "not_null") {
      return null;
    }

    if (op === "starts_with") {
      return { key, value: `${cond?.value != null ? cond.value : ""}%` };
    }

    if (op === "ends_with") {
      return { key, value: `%${cond?.value != null ? cond.value : ""}` };
    }

    return { key, value: cond?.value != null ? cond.value : "" };
  };

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const validFilters = Array.isArray(filters)
      ? filters.filter((f) => {
          const field = String(f?.field || "").trim();
          const op = String(f?.op || "").trim();
          if (!field || !op) return false;
          if (op === "is_null" || op === "not_null") return true;
          return String(f?.value ?? "").trim() !== "";
        })
      : [];

    const whereParts = [
      `s."customerId" = :customerId`,
      `s."ptrsId" = :ptrsId`,
      `s."deletedAt" IS NULL`,
    ];

    const replacements = {
      customerId: String(customerId),
      ptrsId: String(ptrsId),
      sandboxRowCap: SANDBOX_ROW_CAP,
      effectiveLimit,
    };

    validFilters.forEach((filter, idx) => {
      const key = `sandbox_filter_${idx}`;
      const frag = buildJsonbCond("s", filter, key);
      if (frag) {
        whereParts.push(frag);
        const bind = bindValueFor(filter, key);
        if (bind) {
          replacements[bind.key] = bind.value;
        }
      }
    });

    const whereSql = whereParts.join("\n        AND ");

    const rowsSql = `
      WITH filtered_rows AS (
        SELECT
          s."rowNo",
          s."data"
        FROM "tbl_ptrs_stage_row" s
        WHERE ${whereSql}
        ORDER BY s."rowNo" ASC
        LIMIT :sandboxRowCap
      )
      SELECT
        "rowNo",
        "data"
      FROM filtered_rows
      ORDER BY "rowNo" ASC
      LIMIT :effectiveLimit;
    `;

    const countSql = `
      SELECT COUNT(*)::int AS count
      FROM "tbl_ptrs_stage_row" s
      WHERE ${whereSql};
    `;

    const headersSql = `
      WITH filtered_rows AS (
        SELECT s."data"
        FROM "tbl_ptrs_stage_row" s
        WHERE ${whereSql}
        ORDER BY s."rowNo" ASC
        LIMIT :sandboxRowCap
      )
      SELECT DISTINCT jsonb_object_keys("data") AS header
      FROM filtered_rows
      ORDER BY header;
    `;

    const stageRows = await db.sequelize.query(rowsSql, {
      transaction: t,
      replacements,
      type: db.sequelize.QueryTypes.SELECT,
    });

    const [{ count: totalMatching = 0 } = { count: 0 }] =
      await db.sequelize.query(countSql, {
        transaction: t,
        replacements,
        type: db.sequelize.QueryTypes.SELECT,
      });

    const headerRows = await db.sequelize.query(headersSql, {
      transaction: t,
      replacements,
      type: db.sequelize.QueryTypes.SELECT,
    });

    const limitedRows = (stageRows || []).map((r) => {
      const data =
        r && typeof r.data === "object" && r.data !== null ? r.data : {};
      return data;
    });

    const headers = (headerRows || [])
      .map((r) => String(r?.header || "").trim())
      .filter(Boolean);

    slog.info(
      "PTRS v2 sandboxRulesPreview",
      safeMeta({
        customerId,
        ptrsId,
        sampledStageRows: stageRows.length,
        sandboxRowCap: SANDBOX_ROW_CAP,
        filters: validFilters.length,
        totalMatching: Number(totalMatching || 0),
        returned: limitedRows.length,
        headersCount: headers.length,
      }),
    );

    await t.commit();

    return {
      headers,
      rows: limitedRows,
      stats: {
        totalMatching: Number(totalMatching || 0),
        returned: limitedRows.length,
        sampledStageRows: stageRows.length,
        sandboxRowCap: SANDBOX_ROW_CAP,
        headersCount: headers.length,
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
    const submittedGroupNames = collectSubmittedGroupNames({
      rowRules,
      crossRowRules,
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

    if (submittedGroupNames.length) {
      const existingRulesets = await db.PtrsRuleset.findAll({
        where: { customerId, ptrsId, deletedAt: null },
        attributes: ["id", "definition"],
        transaction: t,
        raw: true,
      });

      const idsToDelete = (existingRulesets || [])
        .filter((rs) => {
          const groupName = String(rs?.definition?.groupName || "").trim();
          return submittedGroupNames.includes(groupName);
        })
        .map((rs) => rs.id)
        .filter(Boolean);

      if (idsToDelete.length) {
        await db.PtrsRuleset.destroy({
          where: { id: idsToDelete },
          transaction: t,
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
      groupNames: collectSubmittedGroupNames({ rowRules, crossRowRules }),
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
      where: { customerId, ptrsId, deletedAt: null },
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
    const where = { customerId, deletedAt: null };
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
  groupName = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  // Current implementation applies rules in memory via loadMappedRowsForPtrs.
  // That is acceptable only for explicitly limited diagnostic runs.
  // Full dataset runs must use a SQL-based implementation instead.
  const effectiveLimit =
    limit == null || typeof limit === "undefined"
      ? null
      : Math.min(Number(limit) || 50, 5000);

  const started = Date.now();

  slog.info("PTRS v2 applyRulesAndPersist: starting", {
    action: "PtrsV2RulesApplyStart",
    customerId,
    ptrsId,
    groupName,
    requestedLimit: limit,
    effectiveLimit,
  });

  // 🔐 RLS-safe tenant-scoped transaction for reading import + writing stage
  const t = await beginTransactionWithCustomerContext(customerId);

  // Load rules early so we can decide whether the run can stay in SQL.
  const { rowRules, crossRowRules } = await loadRulesForPtrs({
    customerId,
    ptrsId,
    transaction: t,
    groupName,
  });

  const totalRows = await db.PtrsImportRaw.count({
    where: { customerId, ptrsId },
    transaction: t,
  });

  slog.info("PTRS v2 applyRulesAndPersist: dataset size check", {
    action: "PtrsV2RulesApplyRowCapCheck",
    customerId,
    ptrsId,
    groupName,
    totalRows,
    effectiveLimit,
    sqlImplementation: true,
    rowRuleCount: Array.isArray(rowRules) ? rowRules.length : 0,
    crossRowRuleCount: Array.isArray(crossRowRules) ? crossRowRules.length : 0,
  });

  try {
    const rowStats = await applyRowRulesSql({
      customerId,
      ptrsId,
      rules: rowRules,
      transaction: t,
      limit: effectiveLimit,
    });

    const crossRowStats = await applyCrossRowRulesSql({
      customerId,
      ptrsId,
      rules: crossRowRules,
      transaction: t,
      limit: effectiveLimit,
    });

    const combinedStats = {
      rulesTried:
        Number(rowStats?.rulesTried ?? 0) +
        Number(crossRowStats?.rulesTried ?? 0),
      rowsAffected:
        Number(rowStats?.rowsAffected ?? 0) +
        Number(crossRowStats?.rowsAffected ?? 0),
      actions:
        Number(rowStats?.actions ?? 0) + Number(crossRowStats?.actions ?? 0),
      currentExcluded:
        Number(rowStats?.currentExcluded ?? 0) +
        Number(crossRowStats?.currentExcluded ?? 0),
    };

    const tookMs = Date.now() - started;
    await t.commit();

    slog.info("PTRS v2 applyRulesAndPersist: SQL path done", {
      action: "PtrsV2RulesApplySqlDone",
      customerId,
      ptrsId,
      tookMs,
      rowStats,
      crossRowStats,
      combinedStats,
    });

    return {
      groupName,
      persisted: combinedStats.rowsAffected,
      tookMs,
      stats: {
        rowRules: rowStats,
        crossRowRules: crossRowStats,
        rules: combinedStats,
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

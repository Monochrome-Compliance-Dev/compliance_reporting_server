const db = require("@/db/database");
const { slog } = require("./ptrs.service");
const {
  buildJsonbTextExpr,
  buildJsonbNumericExpr,
  buildRuleWhereSql,
} = require("./rules.sql.shared");
const {
  appendJsonbTextArray,
  appendJsonbTextArrayAtPath,
  applyExcludeFlags,
  applyMetaBase,
} = require("./exclusions.shared");

function validateCrossRowRuleSqlSupport(rule) {
  const match = Array.isArray(rule?.target?.match) ? rule.target.match : [];
  const where = Array.isArray(rule?.target?.where) ? rule.target.where : [];
  const action = rule?.action || {};

  const match0 = match[0] || {};
  const currentField = String(match0.currentField || "").trim();
  const targetField = String(match0.targetField || "").trim();
  const targetAmountField = String(action.field || "").trim();
  const currentAmountField = String(
    action.valueFieldFromCurrent || action.valueField || "",
  ).trim();
  const op = String(action.op || "add")
    .trim()
    .toLowerCase();

  if (!currentField || !targetField) {
    const err = new Error(
      "Cross-row SQL rule requires currentField and targetField.",
    );
    err.statusCode = 400;
    throw err;
  }

  if (!["add", "sub", "mul", "div", "assign"].includes(op)) {
    const err = new Error(
      `Cross-row SQL rule op '${op}' is not supported yet.`,
    );
    err.statusCode = 400;
    throw err;
  }

  if (!targetAmountField || !currentAmountField) {
    const err = new Error(
      "Cross-row SQL rule requires action.field and action.valueFieldFromCurrent.",
    );
    err.statusCode = 400;
    throw err;
  }

  if (!match.length && !where.length) {
    const err = new Error(
      "Cross-row SQL rule target scope is too broad. Add a match key or a target condition.",
    );
    err.statusCode = 400;
    throw err;
  }
}

function buildAppliedMetaSql(metaExpr, ruleKey) {
  const escapedRuleKey = String(ruleKey || "rule").replace(/'/g, "''");
  return `
    jsonb_set(
      COALESCE(${metaExpr}, '{}'::jsonb),
      '{rules,applied}',
      COALESCE(COALESCE(${metaExpr}, '{}'::jsonb)#>'{rules,applied}', '[]'::jsonb)
        || to_jsonb('${escapedRuleKey}'::text),
      true
    )
  `;
}

function buildNotAppliedSql(alias, ruleKey) {
  const escapedRuleKey = String(ruleKey || "rule").replace(/'/g, "''");
  return `
    AND NOT (
      COALESCE(COALESCE(${alias}."meta", '{}'::jsonb)#>'{rules,applied}', '[]'::jsonb)
      @> jsonb_build_array('${escapedRuleKey}'::text)
    )
  `;
}

function getAppliedRuleKeys(meta) {
  const applied = meta?.rules?.applied;
  return Array.isArray(applied)
    ? applied.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
}

function hasAppliedRule(meta, ruleKey) {
  return getAppliedRuleKeys(meta).includes(String(ruleKey || "").trim());
}

function appendUniqueText(values, nextValue) {
  const current = Array.isArray(values)
    ? values.map((v) => String(v || "").trim()).filter(Boolean)
    : [];
  const incoming = String(nextValue || "").trim();
  if (!incoming) return current;
  return current.includes(incoming) ? current : [...current, incoming];
}

function appendPipeText(existing, nextValue) {
  const current = String(existing || "").trim();
  const incoming = String(nextValue || "").trim();
  if (!incoming) return current;
  if (!current) return incoming;
  return current.includes(incoming) ? current : `${current} | ${incoming}`;
}

function roundTo(value, dp = 2) {
  const factor = 10 ** Number(dp || 2);
  return Math.round(Number(value || 0) * factor) / factor;
}

function approxEqual(a, b, epsilon = 0.000001) {
  return Math.abs(Number(a || 0) - Number(b || 0)) <= epsilon;
}

function getDataText(rowData, keys = []) {
  for (const key of Array.isArray(keys) ? keys : []) {
    const value = rowData?.[key];
    if (value == null) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return "";
}

function getDataNumber(rowData, keys = []) {
  for (const key of Array.isArray(keys) ? keys : []) {
    const value = rowData?.[key];
    if (value == null || value === "") continue;
    const n = Number(String(value).replace(/[, ]+/g, ""));
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function buildRuleAppliedMeta(meta, ruleKey) {
  const nextMeta = meta && typeof meta === "object" ? { ...meta } : {};
  nextMeta.rules =
    nextMeta.rules && typeof nextMeta.rules === "object"
      ? { ...nextMeta.rules }
      : {};
  nextMeta.rules.applied = appendUniqueText(nextMeta.rules.applied, ruleKey);
  return nextMeta;
}

function buildExcludedRowState({ data, meta, ruleKey, reason, comment }) {
  const nextData = data && typeof data === "object" ? { ...data } : {};
  let nextMeta =
    applyMetaBase(meta && typeof meta === "object" ? { ...meta } : {}) || {};
  nextMeta = nextMeta && typeof nextMeta === "object" ? nextMeta : {};
  nextMeta = buildRuleAppliedMeta(nextMeta, ruleKey);

  nextData.exclude = true;
  nextData.exclude_from_metrics = true;
  nextData.exclude_reason = String(reason || "").trim() || "CROSS_ROW_RULE";
  nextData.exclude_reasons = appendUniqueText(
    nextData.exclude_reasons,
    nextData.exclude_reason,
  );
  nextData.exclude_comment = appendPipeText(nextData.exclude_comment, comment);

  nextMeta.exclusions =
    nextMeta?.exclusions && typeof nextMeta.exclusions === "object"
      ? { ...nextMeta.exclusions }
      : {};
  nextMeta.exclusions.exclude = true;
  nextMeta.exclusions.exclude_from_metrics = true;
  nextMeta.exclusions.reason =
    String(nextMeta.exclusions.reason || "").trim() || nextData.exclude_reason;
  nextMeta.exclusions.reasons = appendUniqueText(
    nextMeta.exclusions.reasons,
    nextData.exclude_reason,
  );
  nextMeta.exclusions.comments = appendUniqueText(
    nextMeta.exclusions.comments,
    comment,
  );

  return { data: nextData, meta: nextMeta };
}

function buildUpdatedTargetState({
  data,
  meta,
  ruleKey,
  targetAmountField,
  nextAmount,
  comment,
}) {
  const nextData = data && typeof data === "object" ? { ...data } : {};
  const nextMetaRaw = buildRuleAppliedMeta(meta, ruleKey);
  const nextMeta =
    nextMetaRaw && typeof nextMetaRaw === "object" ? nextMetaRaw : {};

  nextData[targetAmountField] = nextAmount;
  if (comment) {
    nextData.rule_comment = appendUniqueText(nextData.rule_comment, comment);
  }

  if (comment) {
    nextMeta.rules =
      nextMeta?.rules && typeof nextMeta.rules === "object"
        ? { ...nextMeta.rules }
        : {};
    nextMeta.rules.comments = appendUniqueText(
      nextMeta.rules.comments,
      comment,
    );
  }

  return { data: nextData, meta: nextMeta };
}

async function persistStageRowUpdate({ transaction, rowId, data, meta }) {
  await db.sequelize.query(
    `
      UPDATE "tbl_ptrs_stage_row"
      SET
        "data" = CAST(:data AS jsonb),
        "meta" = CAST(:meta AS jsonb),
        "updatedAt" = now()
      WHERE "id" = :rowId
    `,
    {
      transaction,
      replacements: {
        rowId,
        data: JSON.stringify(data || {}),
        meta: JSON.stringify(meta || {}),
      },
    },
  );
}

async function applyBestMatchPairingRuleSql({
  customerId,
  ptrsId,
  rule,
  transaction,
  limit = null,
  stats,
}) {
  const ruleKey = String(rule?.id || rule?.label || "crossRowRule").trim();
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
  const targetComment = String(action.comment || action.note || "").trim();
  const currentExcludeComment = String(
    action.comment ||
      action.note ||
      `Excluded by cross-row rule — ${rule.label || ruleKey}`,
  ).trim();
  const excludeUnmatchedCurrent =
    rule?.target?.excludeUnmatchedCurrent === true;
  const unmatchedCurrentComment = String(
    rule?.target?.unmatchedComment ||
      action.unmatchedComment ||
      `Excluded by cross-row rule — no matching invoice found for credit pairing (${rule.label || ruleKey})`,
  ).trim();
  const zeroInvoiceComment = `Excluded by cross-row rule — zero balance after pairing (${rule.label || ruleKey})`;
  const reason = "CROSS_ROW_RULE";

  const replacements = { customerId, ptrsId };
  if (limit != null) replacements.limit = Number(limit);

  const currWhereBuilt = buildRuleWhereSql(when, replacements, "c");
  const currWhereSql = currWhereBuilt.sql || "";
  const tgtWhereBuilt = buildRuleWhereSql(where, replacements, "t");
  const tgtWhereSql = tgtWhereBuilt.sql || "";

  const currKeyExpr = buildJsonbTextExpr(currentField, "c");
  const tgtKeyExpr = buildJsonbTextExpr(targetField, "t");

  const currLimitSql =
    limit != null
      ? `
      AND c."rowNo" IN (
        SELECT s2."rowNo"
        FROM "tbl_ptrs_stage_row" s2
        WHERE s2."customerId" = :customerId
          AND s2."ptrsId" = :ptrsId
          AND s2."deletedAt" IS NULL
        ORDER BY s2."rowNo" ASC
        LIMIT :limit
      )
    `
      : "";

  const tgtLimitSql =
    limit != null
      ? `
      AND t."rowNo" IN (
        SELECT s2."rowNo"
        FROM "tbl_ptrs_stage_row" s2
        WHERE s2."customerId" = :customerId
          AND s2."ptrsId" = :ptrsId
          AND s2."deletedAt" IS NULL
        ORDER BY s2."rowNo" ASC
        LIMIT :limit
      )
    `
      : "";

  const currentRows = await db.sequelize.query(
    `
      SELECT
        c."id",
        c."rowNo",
        c."data",
        c."meta"
      FROM "tbl_ptrs_stage_row" c
      WHERE c."customerId" = :customerId
        AND c."ptrsId" = :ptrsId
        AND c."deletedAt" IS NULL
        ${currWhereSql}
        ${currLimitSql}
        AND COALESCE(${currKeyExpr}, '') <> ''
      ORDER BY c."rowNo" ASC
    `,
    {
      transaction,
      replacements,
      type: db.sequelize.QueryTypes.SELECT,
    },
  );

  const targetRows = await db.sequelize.query(
    `
      SELECT
        t."id",
        t."rowNo",
        t."data",
        t."meta"
      FROM "tbl_ptrs_stage_row" t
      WHERE t."customerId" = :customerId
        AND t."ptrsId" = :ptrsId
        AND t."deletedAt" IS NULL
        ${tgtWhereSql}
        ${tgtLimitSql}
        AND COALESCE(${tgtKeyExpr}, '') <> ''
      ORDER BY t."rowNo" ASC
    `,
    {
      transaction,
      replacements,
      type: db.sequelize.QueryTypes.SELECT,
    },
  );

  const targetsByKey = new Map();
  for (const row of Array.isArray(targetRows) ? targetRows : []) {
    if (hasAppliedRule(row?.meta, ruleKey)) continue;
    if (row?.data?.exclude === true) continue;

    const key = getDataText(row?.data, [targetField]);
    if (!key) continue;

    const bucket = targetsByKey.get(key) || [];
    bucket.push({ ...row, claimed: false });
    targetsByKey.set(key, bucket);
  }

  const currentCandidates = (
    Array.isArray(currentRows) ? currentRows : []
  ).filter((row) => !hasAppliedRule(row?.meta, ruleKey));

  for (const currentRow of currentCandidates) {
    const currentKey = getDataText(currentRow?.data, [currentField]);
    if (!currentKey) continue;

    const currentAmount = getDataNumber(currentRow?.data, [currentAmountField]);
    if (!Number.isFinite(currentAmount)) continue;

    const currentAbsAmount = Math.abs(currentAmount);
    const currentPurchasingDoc = getDataText(currentRow?.data, [
      "purchasing_document_no",
      "Purchasing Document No",
    ]);
    const currentDueDate = getDataText(currentRow?.data, [
      "invoice_due_date",
      "Net Due Date",
    ]);

    const availableTargets = (targetsByKey.get(currentKey) || []).filter(
      (targetRow) => !targetRow.claimed,
    );
    if (!availableTargets.length) {
      if (excludeUnmatchedCurrent) {
        const unmatchedState = buildExcludedRowState({
          data: currentRow.data,
          meta: currentRow.meta,
          ruleKey,
          reason,
          comment: unmatchedCurrentComment,
        });

        await persistStageRowUpdate({
          transaction,
          rowId: currentRow.id,
          data: unmatchedState.data,
          meta: unmatchedState.meta,
        });

        currentRow.data = unmatchedState.data;
        currentRow.meta = unmatchedState.meta;
        stats.actions += 1;
        stats.rowsAffected += 1;
        stats.currentExcluded += 1;
      }
      continue;
    }

    const rankedTargets = availableTargets
      .map((targetRow) => {
        const targetAmount = getDataNumber(targetRow?.data, [
          targetAmountField,
        ]);
        const targetPurchasingDoc = getDataText(targetRow?.data, [
          "purchasing_document_no",
          "Purchasing Document No",
        ]);
        const targetDueDate = getDataText(targetRow?.data, [
          "invoice_due_date",
          "Net Due Date",
        ]);

        return {
          targetRow,
          exactAmountMatch:
            Number.isFinite(targetAmount) &&
            approxEqual(targetAmount, currentAbsAmount)
              ? 1
              : 0,
          samePurchasingDoc:
            currentPurchasingDoc &&
            targetPurchasingDoc &&
            currentPurchasingDoc === targetPurchasingDoc
              ? 1
              : 0,
          sameDueDate:
            currentDueDate && targetDueDate && currentDueDate === targetDueDate
              ? 1
              : 0,
        };
      })
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
        return (
          Number(a.targetRow?.rowNo || 0) - Number(b.targetRow?.rowNo || 0)
        );
      });

    const best = rankedTargets[0]?.targetRow;
    if (!best) {
      if (excludeUnmatchedCurrent) {
        const unmatchedState = buildExcludedRowState({
          data: currentRow.data,
          meta: currentRow.meta,
          ruleKey,
          reason,
          comment: unmatchedCurrentComment,
        });

        await persistStageRowUpdate({
          transaction,
          rowId: currentRow.id,
          data: unmatchedState.data,
          meta: unmatchedState.meta,
        });

        currentRow.data = unmatchedState.data;
        currentRow.meta = unmatchedState.meta;
        stats.actions += 1;
        stats.rowsAffected += 1;
        stats.currentExcluded += 1;
      }
      continue;
    }

    const currentTargetAmount = getDataNumber(best?.data, [targetAmountField]);
    if (!Number.isFinite(currentTargetAmount)) {
      if (excludeUnmatchedCurrent) {
        const unmatchedState = buildExcludedRowState({
          data: currentRow.data,
          meta: currentRow.meta,
          ruleKey,
          reason,
          comment: unmatchedCurrentComment,
        });

        await persistStageRowUpdate({
          transaction,
          rowId: currentRow.id,
          data: unmatchedState.data,
          meta: unmatchedState.meta,
        });

        currentRow.data = unmatchedState.data;
        currentRow.meta = unmatchedState.meta;
        stats.actions += 1;
        stats.rowsAffected += 1;
        stats.currentExcluded += 1;
      }
      continue;
    }

    let nextAmount = currentTargetAmount;
    if (op === "add") nextAmount = currentTargetAmount + currentAmount;
    else if (op === "sub") nextAmount = currentTargetAmount - currentAmount;
    else if (op === "mul") nextAmount = currentTargetAmount * currentAmount;
    else if (op === "div") {
      nextAmount =
        currentAmount === 0
          ? currentTargetAmount
          : currentTargetAmount / currentAmount;
    } else if (op === "assign") {
      nextAmount = currentAmount;
    }
    nextAmount = roundTo(nextAmount, dp);

    const targetState = buildUpdatedTargetState({
      data: best.data,
      meta: best.meta,
      ruleKey,
      targetAmountField,
      nextAmount,
      comment: targetComment,
    });

    let finalTargetState = targetState;
    if (approxEqual(nextAmount, 0)) {
      finalTargetState = buildExcludedRowState({
        data: targetState.data,
        meta: targetState.meta,
        ruleKey,
        reason,
        comment: zeroInvoiceComment,
      });
    }

    await persistStageRowUpdate({
      transaction,
      rowId: best.id,
      data: finalTargetState.data,
      meta: finalTargetState.meta,
    });

    best.data = finalTargetState.data;
    best.meta = finalTargetState.meta;
    best.claimed = true;

    stats.actions += 1;
    stats.rowsAffected += 1;

    if (rule.alsoExcludeCurrent) {
      const currentState = buildExcludedRowState({
        data: currentRow.data,
        meta: currentRow.meta,
        ruleKey,
        reason,
        comment: currentExcludeComment,
      });

      await persistStageRowUpdate({
        transaction,
        rowId: currentRow.id,
        data: currentState.data,
        meta: currentState.meta,
      });

      currentRow.data = currentState.data;
      currentRow.meta = currentState.meta;
      stats.actions += 1;
      stats.rowsAffected += 1;
      stats.currentExcluded += 1;
    }
  }
}

async function applyCrossRowRulesSql({
  customerId,
  ptrsId,
  rules,
  transaction,
  limit = null,
}) {
  const enabled = Array.isArray(rules)
    ? rules.filter((r) => r && r.enabled !== false)
    : [];

  const stats = {
    rulesTried: enabled.length,
    rowsAffected: 0,
    actions: 0,
    currentExcluded: 0,
  };

  if (!enabled.length) return stats;

  slog.info("PTRS v2 SQL cross-row rules: starting", {
    action: "PtrsV2CrossRowRulesSqlStart",
    customerId,
    ptrsId,
    ruleCount: enabled.length,
    limit,
  });

  for (const rule of enabled) {
    validateCrossRowRuleSqlSupport(rule);

    const ruleKey = String(rule?.id || rule?.label || "crossRowRule").trim();
    const when = Array.isArray(rule?.when) ? rule.when : [];
    const match = Array.isArray(rule?.target?.match) ? rule.target.match : [];
    const where = Array.isArray(rule?.target?.where) ? rule.target.where : [];
    const match0 = match[0] || {};

    const currentField = String(match0.currentField || "").trim();
    const targetField = String(match0.targetField || "").trim();
    const action = rule.action || {};
    const op = String(action.op || "add").toLowerCase();
    const targetSelection = String(rule?.target?.selection || "")
      .trim()
      .toLowerCase();
    const targetAmountField = String(action.field || "").trim();
    const currentAmountField = String(
      action.valueFieldFromCurrent || action.valueField || "",
    ).trim();
    const dp = typeof action.round === "number" ? action.round : 2;
    const comment = String(action.comment || action.note || "").trim();
    const targetCommentSql = comment
      ? `'${comment.replace(/'/g, "''")}'`
      : null;
    const exclusionReasonSql = `'CROSS_ROW_RULE'`;
    const currentCommentText = String(
      action.comment ||
        action.note ||
        `Excluded by cross-row rule — ${rule.label || ruleKey}`,
    ).trim();
    const currentCommentSql = `'${currentCommentText.replace(/'/g, "''")}'`;

    const replacements = { customerId, ptrsId };
    if (limit != null) replacements.limit = Number(limit);

    const currWhereBuilt = buildRuleWhereSql(when, replacements, "c");
    const currWhereSql = currWhereBuilt.sql || "";
    const tgtWhereBuilt = buildRuleWhereSql(where, replacements, "t");
    const tgtWhereSql = tgtWhereBuilt.sql || "";

    const currKeyExpr = buildJsonbTextExpr(currentField, "c");
    const tgtKeyExpr = buildJsonbTextExpr(targetField, "t");
    const currAmtExpr = buildJsonbNumericExpr(currentAmountField, "c");
    const tgtAmtExpr = buildJsonbNumericExpr(targetAmountField, "t");

    const currLimitSql =
      limit != null
        ? `
      AND c."rowNo" IN (
        SELECT s2."rowNo"
        FROM "tbl_ptrs_stage_row" s2
        WHERE s2."customerId" = :customerId
          AND s2."ptrsId" = :ptrsId
          AND s2."deletedAt" IS NULL
        ORDER BY s2."rowNo" ASC
        LIMIT :limit
      )
    `
        : "";

    const tgtLimitSql =
      limit != null
        ? `
      AND t."rowNo" IN (
        SELECT s2."rowNo"
        FROM "tbl_ptrs_stage_row" s2
        WHERE s2."customerId" = :customerId
          AND s2."ptrsId" = :ptrsId
          AND s2."deletedAt" IS NULL
        ORDER BY s2."rowNo" ASC
        LIMIT :limit
      )
    `
        : "";

    if (targetSelection === "best_match_pairing") {
      await applyBestMatchPairingRuleSql({
        customerId,
        ptrsId,
        rule,
        transaction,
        limit,
        stats,
      });
      continue;
    }

    let mathSql = null;
    if (op === "add") mathSql = `COALESCE(${tgtAmtExpr}, 0) + x.delta`;
    else if (op === "sub") mathSql = `COALESCE(${tgtAmtExpr}, 0) - x.delta`;
    else if (op === "mul") mathSql = `COALESCE(${tgtAmtExpr}, 0) * x.delta`;
    else if (op === "div") {
      mathSql = `CASE WHEN x.delta = 0 THEN COALESCE(${tgtAmtExpr}, 0) ELSE COALESCE(${tgtAmtExpr}, 0) / x.delta END`;
    } else if (op === "assign") {
      mathSql = `x.delta`;
    }

    let targetDataSql = `
      jsonb_set(
        COALESCE(t."data", '{}'::jsonb),
        '{${targetAmountField}}',
        to_jsonb(ROUND((${mathSql})::numeric, ${dp})),
        true
      )
    `;

    if (targetCommentSql) {
      targetDataSql = appendJsonbTextArray(
        "rule_comment",
        targetCommentSql,
        targetDataSql,
      );
    }

    let targetMetaSql = buildAppliedMetaSql(`t."meta"`, ruleKey);
    if (targetCommentSql) {
      targetMetaSql = appendJsonbTextArrayAtPath(
        "rules,comments",
        targetCommentSql,
        targetMetaSql,
      );
    }
    const notAppliedTargetSql = buildNotAppliedSql("t", ruleKey);

    const targetSql = `
      WITH curr AS (
        SELECT
          ${currKeyExpr} AS k,
          SUM(COALESCE(${currAmtExpr}, 0)) AS delta
        FROM "tbl_ptrs_stage_row" c
        WHERE c."customerId" = :customerId
          AND c."ptrsId" = :ptrsId
          AND c."deletedAt" IS NULL
          ${currWhereSql}
          ${currLimitSql}
          AND COALESCE(${currKeyExpr}, '') <> ''
        GROUP BY 1
      ),
      impacted AS (
        SELECT DISTINCT ON (curr.k)
          t."id",
          curr.k,
          curr.delta
        FROM "tbl_ptrs_stage_row" t
        JOIN curr ON curr.k = ${tgtKeyExpr}
        WHERE t."customerId" = :customerId
          AND t."ptrsId" = :ptrsId
          AND t."deletedAt" IS NULL
          ${tgtWhereSql}
          ${tgtLimitSql}
          ${notAppliedTargetSql}
        ORDER BY curr.k, t."rowNo" ASC
      )
      UPDATE "tbl_ptrs_stage_row" t
      SET
        "data" = ${targetDataSql},
        "meta" = ${targetMetaSql},
        "updatedAt" = now()
      FROM impacted x
      WHERE t."id" = x."id"
    `;

    const [, targetMeta] = await db.sequelize.query(targetSql, {
      transaction,
      replacements,
    });

    const targetAffected = Number(targetMeta?.rowCount ?? 0) || 0;
    if (targetAffected > 0) {
      stats.actions += targetAffected;
      stats.rowsAffected += targetAffected;
    }

    if (rule.alsoExcludeCurrent) {
      const currentDataBaseSql = applyExcludeFlags(
        `c."data"`,
        exclusionReasonSql,
      );
      const currentDataWithReasonsSql = appendJsonbTextArray(
        "exclude_reasons",
        exclusionReasonSql,
        currentDataBaseSql,
      );
      const currentDataSql = appendJsonbTextArray(
        "exclude_comment",
        currentCommentSql,
        currentDataWithReasonsSql,
      );

      const currentMetaBaseSql = applyMetaBase(`c."meta"`);
      const currentMetaWithReasonSql = `
        jsonb_set(
          ${currentMetaBaseSql},
          '{exclusions,reason}',
          CASE
            WHEN trim(COALESCE(${currentMetaBaseSql}#>>'{exclusions,reason}', '')) <> ''
              THEN to_jsonb(${currentMetaBaseSql}#>>'{exclusions,reason}')
            ELSE to_jsonb((${exclusionReasonSql})::text)
          END,
          true
        )
      `;
      const currentMetaWithReasonsSql = appendJsonbTextArrayAtPath(
        "exclusions,reasons",
        exclusionReasonSql,
        currentMetaWithReasonSql,
      );
      const currentMetaWithCommentsSql = appendJsonbTextArrayAtPath(
        "exclusions,comments",
        currentCommentSql,
        currentMetaWithReasonsSql,
      );
      const currentMetaSql = buildAppliedMetaSql(
        currentMetaWithCommentsSql,
        ruleKey,
      );
      const notAppliedCurrentSql = buildNotAppliedSql("c", ruleKey);

      const currentSql = `
        WITH curr AS (
          SELECT
            ${currKeyExpr} AS k
          FROM "tbl_ptrs_stage_row" c
          WHERE c."customerId" = :customerId
            AND c."ptrsId" = :ptrsId
            AND c."deletedAt" IS NULL
            ${currWhereSql}
            ${currLimitSql}
            AND COALESCE(${currKeyExpr}, '') <> ''
          GROUP BY 1
        ),
        matched_keys AS (
          SELECT DISTINCT curr.k
          FROM curr
          JOIN "tbl_ptrs_stage_row" t
            ON curr.k = ${tgtKeyExpr.replace(/\bt\./g, "t.")}
          WHERE t."customerId" = :customerId
            AND t."ptrsId" = :ptrsId
            AND t."deletedAt" IS NULL
            ${tgtWhereSql}
            ${tgtLimitSql}
        )
        UPDATE "tbl_ptrs_stage_row" c
        SET
          "data" = ${currentDataSql},
          "meta" = ${currentMetaSql},
          "updatedAt" = now()
        FROM matched_keys mk
        WHERE c."customerId" = :customerId
          AND c."ptrsId" = :ptrsId
          AND c."deletedAt" IS NULL
          ${currWhereSql}
          ${currLimitSql}
          AND COALESCE(${currKeyExpr}, '') <> ''
          AND mk.k = ${currKeyExpr}
          ${notAppliedCurrentSql}
      `;

      const [, currentMeta] = await db.sequelize.query(currentSql, {
        transaction,
        replacements,
      });

      const currentAffected = Number(currentMeta?.rowCount ?? 0) || 0;
      if (currentAffected > 0) {
        stats.actions += currentAffected;
        stats.rowsAffected += currentAffected;
        stats.currentExcluded += currentAffected;
      }
    }
  }

  slog.info("PTRS v2 SQL cross-row rules: complete", {
    action: "PtrsV2CrossRowRulesSqlDone",
    customerId,
    ptrsId,
    stats,
  });

  return stats;
}

module.exports = {
  applyCrossRowRulesSql,
};

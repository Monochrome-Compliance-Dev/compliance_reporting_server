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

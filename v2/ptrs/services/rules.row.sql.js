const db = require("@/db/database");
const { slog } = require("./ptrs.service");
const {
  buildJsonbTextExpr,
  buildJsonbNumericExpr,
  buildRuleWhereSql,
  buildConcatSegmentsSql,
} = require("./rules.sql.shared");

async function applyRowRulesSql({
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
  };

  if (!enabled.length) return stats;

  slog.info("PTRS v2 SQL row rules: starting", {
    action: "PtrsV2RowRulesSqlStart",
    customerId,
    ptrsId,
    ruleCount: enabled.length,
    limit,
  });

  for (const rule of enabled) {
    const ruleKey = String(rule?.id || rule?.label || "rule").trim();
    const conds = Array.isArray(rule?.when) ? rule.when : [];
    const actions = Array.isArray(rule?.then) ? rule.then : [];

    if (!actions.length) continue;

    for (const act of actions) {
      const replacements = { customerId, ptrsId };
      const whereBuilt = buildRuleWhereSql(conds, replacements);
      const whereSql = whereBuilt.sql || "";

      const escapedRuleKey = ruleKey.replace(/'/g, "''");
      const notAppliedSql = `
        AND NOT (
          COALESCE(COALESCE(meta, '{}'::jsonb)#>'{rules,applied}', '[]'::jsonb)
          @> jsonb_build_array('${escapedRuleKey}'::text)
        )
      `;

      const limitSql =
        limit != null
          ? `
      AND "rowNo" IN (
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

      if (limit != null) {
        replacements.limit = Number(limit);
      }

      let dataSql = null;
      const op = String(act?.op || "")
        .trim()
        .toLowerCase();
      const targetField = String(act?.field || "").trim();

      if (!targetField) continue;

      if (op === "concat_fields") {
        const concatSql = buildConcatSegmentsSql(
          Array.isArray(act?.segments)
            ? act.segments
            : Array.isArray(act?.fields)
              ? act.fields.map((name) => ({ kind: "field", name }))
              : [],
        );

        if (!concatSql) continue;

        dataSql = `
          jsonb_set(
            COALESCE(data, '{}'::jsonb),
            '{${targetField}}',
            to_jsonb(${concatSql}),
            true
          )
        `;
      } else if (op === "assign") {
        const sourceField = String(
          act?.valueField || act?.valueFieldFromCurrent || "",
        ).trim();
        if (!sourceField) continue;

        dataSql = `
          jsonb_set(
            COALESCE(data, '{}'::jsonb),
            '{${targetField}}',
            to_jsonb(data->>'${sourceField}'),
            true
          )
        `;
      } else if (["add", "sub", "mul", "div"].includes(op)) {
        const sourceField = String(
          act?.valueField || act?.valueFieldFromCurrent || "",
        ).trim();
        if (!sourceField) continue;

        const currentExpr = buildJsonbNumericExpr(targetField);
        const sourceExpr = buildJsonbNumericExpr(sourceField);
        const dp = typeof act?.round === "number" ? act.round : 2;

        let mathSql = null;
        if (op === "add")
          mathSql = `COALESCE(${currentExpr}, 0) + COALESCE(${sourceExpr}, 0)`;
        else if (op === "sub")
          mathSql = `COALESCE(${currentExpr}, 0) - COALESCE(${sourceExpr}, 0)`;
        else if (op === "mul")
          mathSql = `COALESCE(${currentExpr}, 0) * COALESCE(${sourceExpr}, 0)`;
        else if (op === "div") {
          mathSql = `CASE WHEN COALESCE(${sourceExpr}, 0) = 0 THEN COALESCE(${currentExpr}, 0) ELSE COALESCE(${currentExpr}, 0) / COALESCE(${sourceExpr}, 0) END`;
        }

        dataSql = `
          jsonb_set(
            COALESCE(data, '{}'::jsonb),
            '{${targetField}}',
            to_jsonb(ROUND((${mathSql})::numeric, ${dp})),
            true
          )
        `;
      } else {
        continue;
      }

      const metaSql = `
        jsonb_set(
          COALESCE(meta, '{}'::jsonb),
          '{rules,applied}',
          COALESCE(COALESCE(meta, '{}'::jsonb)#>'{rules,applied}', '[]'::jsonb)
            || to_jsonb('${escapedRuleKey}'::text),
          true
        )
      `;

      const sql = `
  UPDATE "tbl_ptrs_stage_row"
  SET
    "data" = ${dataSql},
    "meta" = ${metaSql},
    "updatedAt" = now()
  WHERE "customerId" = :customerId
    AND "ptrsId" = :ptrsId
    AND "deletedAt" IS NULL
    ${whereSql}
    ${notAppliedSql}
    ${limitSql}
`;

      const [, meta] = await db.sequelize.query(sql, {
        transaction,
        replacements,
      });

      const affected = Number(meta?.rowCount ?? 0) || 0;
      if (affected > 0) {
        stats.actions += affected;
        stats.rowsAffected += affected;
      }
    }
  }

  slog.info("PTRS v2 SQL row rules: complete", {
    action: "PtrsV2RowRulesSqlDone",
    customerId,
    ptrsId,
    stats,
  });

  return stats;
}

module.exports = {
  buildJsonbTextExpr,
  buildJsonbNumericExpr,
  buildRuleWhereSql,
  buildConcatSegmentsSql,
  applyRowRulesSql,
};

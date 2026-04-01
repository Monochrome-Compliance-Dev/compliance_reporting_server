const db = require("@/db/database");
const { safeMeta, slog } = require("@/v2/ptrs/services/ptrs.service");

function _sqlIdent(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

function _sqlLiteral(v) {
  if (v == null) return "NULL";
  const s = String(v);
  return `'${s.replace(/'/g, "''")}'`;
}

function _getTableName(model) {
  try {
    const tn =
      model && typeof model.getTableName === "function"
        ? model.getTableName()
        : null;
    if (typeof tn === "string") return tn;
    if (tn && typeof tn === "object" && tn.tableName)
      return tn.schema ? `${tn.schema}.${tn.tableName}` : tn.tableName;
  } catch (_) {}
  return model && model.tableName ? model.tableName : null;
}

function _normSql(exprSql) {
  return `lower(btrim(coalesce(${exprSql}, '')))`;
}

function _isMainRoleSql(role) {
  const r = String(role || "")
    .trim()
    .toLowerCase();
  return r === "main" || r.startsWith("main_");
}

function buildJoinDebugSql({
  customerId,
  ptrsId,
  limit,
  offset,
  normalisedJoins,
}) {
  const importTable = _getTableName(db.PtrsImportRaw) || "PtrsImportRaw";
  const datasetTable = _getTableName(db.PtrsDataset) || "PtrsDataset";

  const byToRole = new Map();
  for (const j of normalisedJoins || []) {
    if (!j || !j.toRole) continue;
    const toRole = String(j.toRole || "")
      .trim()
      .toLowerCase();
    if (!toRole || _isMainRoleSql(toRole)) continue;
    if (!byToRole.has(toRole)) byToRole.set(toRole, []);
    byToRole.get(toRole).push(j);
  }

  const mAlias = "m";
  let selectJson = `${mAlias}.${_sqlIdent("data")}`;
  const joinClauses = [];

  for (const [toRole, joinsForRole] of byToRole.entries()) {
    const alias = `j_${toRole.replace(/[^a-z0-9_]/g, "_")}`;

    const condSqlParts = [];
    for (const j of joinsForRole) {
      const fromRole = String(j.fromRole || "")
        .trim()
        .toLowerCase();
      const fromCol = String(j.fromColumn || "");
      const toCol = String(j.toColumn || "");
      if (!fromRole || !fromCol || !toCol) continue;

      const lhsExpr = _isMainRoleSql(fromRole)
        ? `${mAlias}.${_sqlIdent("data")}->>${_sqlLiteral(fromCol)}`
        : `${mAlias}.${_sqlIdent("data")}->>${_sqlLiteral(
            `${fromRole}__${fromCol}`,
          )}`;

      const rhsExpr = `${alias}.${_sqlIdent("data")}->>${_sqlLiteral(toCol)}`;

      condSqlParts.push(`${_normSql(lhsExpr)} = ${_normSql(rhsExpr)}`);
    }

    const whereMatch = condSqlParts.length
      ? `(${condSqlParts.join(" OR ")})`
      : "FALSE";

    const datasetIdSql = `(SELECT d.${_sqlIdent("id")} FROM ${datasetTable} d WHERE d.${_sqlIdent(
      "customerId",
    )} = ${_sqlLiteral(customerId)} AND d.${_sqlIdent(
      "ptrsId",
    )} = ${_sqlLiteral(ptrsId)} AND lower(btrim(coalesce(d.${_sqlIdent(
      "role",
    )}, ''))) = ${_sqlLiteral(toRole)} LIMIT 1)`;

    const lateral = `
LEFT JOIN LATERAL (
  SELECT s.${_sqlIdent("data")}
  FROM ${importTable} s
  WHERE s.${_sqlIdent("customerId")} = ${_sqlLiteral(customerId)}
    AND s.${_sqlIdent("datasetId")} = ${datasetIdSql}
    AND ${whereMatch}
  ORDER BY s.${_sqlIdent("rowNo")} ASC
  LIMIT 1
) ${alias} ON TRUE`.trim();

    joinClauses.push(lateral);

    const prefixedJson = `
COALESCE((
  SELECT jsonb_object_agg(${_sqlLiteral(`${toRole}__`)} || e.key, e.value)
  FROM jsonb_each(${alias}.${_sqlIdent("data")}) e
), '{}'::jsonb)`.trim();

    selectJson = `(${selectJson} || ${prefixedJson})`;
  }

  return `
SELECT
  ${mAlias}.${_sqlIdent("rowNo")} AS row_no,
  ${selectJson} AS joined_data
FROM ${importTable} ${mAlias}
${joinClauses.join("\n")}
WHERE ${mAlias}.${_sqlIdent("customerId")} = ${_sqlLiteral(customerId)}
  AND ${mAlias}.${_sqlIdent("ptrsId")} = ${_sqlLiteral(ptrsId)}
ORDER BY ${mAlias}.${_sqlIdent("rowNo")} ASC
LIMIT ${Number.isFinite(Number(limit)) ? Math.max(1, Number(limit)) : 50}
OFFSET ${Number.isFinite(Number(offset)) ? Math.max(0, Number(offset)) : 0};
`.trim();
}

function logEarlyComposeJoinDebugSql({
  customerId,
  ptrsId,
  limit,
  offset,
  normalisedJoins,
}) {
  try {
    const debugSql = buildJoinDebugSql({
      customerId,
      ptrsId,
      limit,
      offset,
      normalisedJoins,
    });

    slog.info(
      "PTRS v2 composeMappedRowsForPtrs: generated join SQL (early debug)",
      safeMeta({
        customerId,
        ptrsId,
        joinsCount: normalisedJoins.length,
        sql: debugSql,
      }),
    );
  } catch (e) {
    slog.warn(
      "PTRS v2 composeMappedRowsForPtrs: failed to generate debug join SQL",
      safeMeta({ customerId, ptrsId, error: e.message }),
    );
  }
}

function logComposeJoinProbeOnce({
  logger,
  loggedRef,
  customerId,
  ptrsId,
  message,
  meta,
}) {
  if (loggedRef.logged || !(logger && logger.debug)) return;
  loggedRef.logged = true;
  slog.debug(
    message,
    safeMeta({
      customerId,
      ptrsId,
      ...meta,
    }),
  );
}

module.exports = {
  buildJoinDebugSql,
  logEarlyComposeJoinDebugSql,
  logComposeJoinProbeOnce,
};

function buildJsonbTextExpr(field, alias = null) {
  const f = String(field || "").trim();
  if (!f) return null;
  const prefix = alias ? `${alias}.` : "";
  return `${prefix}data->>'${f}'`;
}

function buildJsonbNumericExpr(field, alias = null) {
  const expr = buildJsonbTextExpr(field, alias);
  if (!expr) return null;
  return `NULLIF(regexp_replace(${expr}, '[^0-9\\.\\-]', '', 'g'), '')::numeric`;
}

function buildRuleWhereSql(conds = [], replacements = {}, alias = null) {
  const clauses = [];

  for (const c of conds || []) {
    if (!c || typeof c !== "object") continue;

    const field = String(c.field || "").trim();
    const op = String(c.op || "").trim();
    if (!field || !op) continue;

    const expr = buildJsonbTextExpr(field, alias);
    const scope = alias ? String(alias).replace(/[^a-zA-Z0-9_]/g, "") : "x";
    const safeField = String(field).replace(/[^a-zA-Z0-9_]/g, "_");
    const key = `r_${scope}_${safeField}_${clauses.length}`;

    switch (op) {
      case "eq":
        clauses.push(`${expr} = :${key}`);
        replacements[key] = c.value ?? "";
        break;

      case "neq":
        clauses.push(`${expr} <> :${key}`);
        replacements[key] = c.value ?? "";
        break;

      case "starts_with":
        clauses.push(`${expr} LIKE :${key}`);
        replacements[key] = `${c.value ?? ""}%`;
        break;

      case "ends_with":
        clauses.push(`${expr} LIKE :${key}`);
        replacements[key] = `%${c.value ?? ""}`;
        break;

      case "is_null":
        clauses.push(`(${expr} IS NULL OR ${expr} = '')`);
        break;

      case "not_null":
        clauses.push(`(${expr} IS NOT NULL AND ${expr} <> '')`);
        break;

      case "gt":
        clauses.push(`${buildJsonbNumericExpr(field, alias)} > :${key}`);
        replacements[key] = c.value ?? 0;
        break;

      case "gte":
        clauses.push(`${buildJsonbNumericExpr(field, alias)} >= :${key}`);
        replacements[key] = c.value ?? 0;
        break;

      case "lt":
        clauses.push(`${buildJsonbNumericExpr(field, alias)} < :${key}`);
        replacements[key] = c.value ?? 0;
        break;

      case "lte":
        clauses.push(`${buildJsonbNumericExpr(field, alias)} <= :${key}`);
        replacements[key] = c.value ?? 0;
        break;

      case "in": {
        const arr = String(c.value || "")
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);
        clauses.push(`${expr} = ANY(:${key})`);
        replacements[key] = arr;
        break;
      }

      case "nin": {
        const arr = String(c.value || "")
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);
        clauses.push(`NOT (${expr} = ANY(:${key}))`);
        replacements[key] = arr;
        break;
      }

      default:
        break;
    }
  }

  if (!clauses.length) return { sql: "", replacements };

  return {
    sql: `AND (${clauses.join(" AND ")})`,
    replacements,
  };
}

function buildConcatSegmentsSql(segments = []) {
  if (!Array.isArray(segments) || !segments.length) return null;

  const parts = segments
    .map((seg) => {
      if (!seg || typeof seg !== "object") return null;

      if (seg.kind === "literal") {
        const val = String(seg.value ?? "").replace(/'/g, "''");
        return `'${val}'`;
      }

      const name = String(seg.name || "").trim();
      if (!name) return null;

      return `COALESCE(data->>'${name}', '')`;
    })
    .filter(Boolean);

  if (!parts.length) return null;

  return parts.join(" || ");
}

module.exports = {
  buildJsonbTextExpr,
  buildJsonbNumericExpr,
  buildRuleWhereSql,
  buildConcatSegmentsSql,
};

const db = require("@/db/database");
const { QueryTypes } = require("sequelize");
const { slog } = require("./ptrs.service");
const {
  parseISODateOnly,
} = require("@/v2/ptrs/services/stage.payment-time.ptrs.service");

function deriveSupplierKey(row) {
  if (!row || typeof row !== "object") return null;
  const candidates = [
    row.supplier,
    row.vendor,
    row.vendor_account,
    row.vendorAccount,
    row.vendor_account_no,
    row.vendorAccountNo,
    row.vendor_id,
    row.vendorId,
    row.payee_vendor_id,
    row.payeeVendorId,
    row.supplier_code,
    row.supplierCode,
  ];
  for (const c of candidates) {
    if (c == null) continue;
    const s = String(c).trim();
    if (s) return s;
  }
  return null;
}

function deriveTermReferenceDate(row) {
  const candidates = [
    row?.invoice_issue_date,
    row?.invoice_receipt_date,
    row?.supply_date,
    row?.payment_date,
  ];

  for (const c of candidates) {
    const p = parseISODateOnly(c);
    if (p) return p.iso;
  }

  return null;
}

function normaliseHeaderToRowField(header) {
  if (!header) return null;
  const s = String(header).trim();
  if (!s) return null;

  return s
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/[^a-zA-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
}

function normaliseHeaderToDbColumn(header) {
  if (!header) return null;
  const s = String(header).trim();
  if (!s) return null;

  const cleaned = s
    .replace(/[^a-zA-Z0-9 ]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");

  const parts = cleaned.split(" ").filter(Boolean);
  if (!parts.length) return null;

  const [first, ...rest] = parts;
  const camel =
    first.charAt(0).toLowerCase() +
    first.slice(1) +
    rest.map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join("");

  return camel;
}

function getCanonicalFieldForMainHeader(mapRow, header) {
  if (!header) return null;
  const h = String(header).trim();
  if (!h) return null;

  const mappings = mapRow && mapRow.mappings ? mapRow.mappings : null;
  if (mappings && typeof mappings === "object") {
    const direct = mappings[h];
    if (direct) {
      if (typeof direct === "string") return direct;
      if (typeof direct === "object") {
        return direct.field || null;
      }
    }

    if (Array.isArray(mappings)) {
      const found = mappings.find(
        (m) =>
          m &&
          (m.from === h || m.source === h || m.column === h || m.header === h),
      );
      if (found) {
        return (
          found.canonicalField ||
          found.canonical ||
          found.to ||
          found.target ||
          null
        );
      }
    }
  }

  return null;
}

function extractTermChangesJoinSpec(mapRow) {
  const joins = mapRow && mapRow.joins ? mapRow.joins : null;
  let parsed = joins;
  if (typeof parsed === "string") {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      parsed = null;
    }
  }

  const conditions =
    parsed && Array.isArray(parsed.conditions) ? parsed.conditions : [];

  const termConds = conditions.filter(
    (c) =>
      c &&
      c.to &&
      c.from &&
      c.to.role === "termschanges" &&
      c.from.role &&
      c.from.role !== "termschanges" &&
      c.to.column &&
      c.from.column,
  );

  const effective = termConds.length
    ? termConds
    : [
        {
          to: { role: "termschanges", column: "Company Code" },
          from: { role: "main", column: "Company Code" },
        },
      ];

  const spec = [];
  for (const c of effective) {
    const mainField = getCanonicalFieldForMainHeader(mapRow, c.from.column);
    const changeColumn = normaliseHeaderToDbColumn(c.to.column);
    if (!mainField || !changeColumn) continue;
    spec.push({ mainField, changeColumn });
  }

  const seen = new Set();
  return spec.filter((s) => {
    const k = `${s.mainField}::${s.changeColumn}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

function getRowValueByField(row, field) {
  if (!row || typeof row !== "object" || !field) return null;
  if (!Object.prototype.hasOwnProperty.call(row, field)) return null;

  const v = row[field];
  if (v == null) return null;
  const s = String(v).trim();
  return s ? s : null;
}

async function loadEffectiveTermChangesForRows({
  customerId,
  profileId,
  rows,
  mapRow,
  transaction,
}) {
  const out = new Map();
  if (!customerId || !profileId) return out;
  if (!Array.isArray(rows) || rows.length === 0) return out;

  const joinSpec = extractTermChangesJoinSpec(mapRow);

  const allowedJoinColumns = new Set([
    "companyCode",
    "supplier",
    "purchOrganization",
    "purchOrg",
    "purchasingOrganization",
  ]);

  const safeJoinSpec = joinSpec.filter(
    (s) =>
      s &&
      s.mainField &&
      s.changeColumn &&
      allowedJoinColumns.has(s.changeColumn),
  );

  const effectiveJoinSpec = safeJoinSpec;

  if (!effectiveJoinSpec.length) {
    slog.error("PTRS v2 term changes: no valid explicit join spec resolved", {
      action: "PtrsV2TermChangesJoinSpecMissing",
      customerId,
      profileId,
      configuredJoinSpec: joinSpec,
    });

    const err = new Error(
      "Effective-dated payment term changes require an explicit resolvable join spec. Fix the term-changes join configuration and main-header mappings.",
    );
    err.statusCode = 400;
    throw err;
  }

  const sample = rows && rows[0] ? rows[0] : null;
  if (sample) {
    const missingMainFields = effectiveJoinSpec
      .map((s) => s.mainField)
      .filter((f) => f && !Object.prototype.hasOwnProperty.call(sample, f));

    if (missingMainFields.length) {
      slog.error(
        "PTRS v2 term changes: join fields not present on staged rows",
        {
          action: "PtrsV2TermChangesJoinFieldsMissing",
          customerId,
          profileId,
          missingMainFields,
          availableFields: Object.keys(sample),
          joinSpec: effectiveJoinSpec,
        },
      );

      const err = new Error(
        `Term changes join requires staged fields missing from row shape: ${missingMainFields.join(
          ", ",
        )}. Fix the column map mappings for those headers.`,
      );
      err.statusCode = 400;
      throw err;
    }
  }

  const input = [];
  const seen = new Set();

  for (const r of rows) {
    if (!r || typeof r !== "object") continue;

    const refDate = deriveTermReferenceDate(r);
    if (!refDate) continue;

    const inputRow = { refDate };
    let missingAnyJoin = false;

    for (const spec of effectiveJoinSpec) {
      const v = getRowValueByField(r, spec.mainField);
      if (!v) {
        missingAnyJoin = true;
        break;
      }
      inputRow[spec.changeColumn] = v;
    }

    if (missingAnyJoin) continue;

    const key =
      effectiveJoinSpec.map((s) => `${inputRow[s.changeColumn]}`).join("::") +
      `::${refDate}`;

    if (seen.has(key)) continue;
    seen.add(key);

    input.push(inputRow);
  }

  if (!input.length) return out;

  const joinCols = effectiveJoinSpec.map((s) => s.changeColumn);
  const recordsetCols = joinCols.map((c) => `"${c}" text`).join(", ");
  const selectCols = joinCols
    .map((c) => `i."${c}" AS "${c}"`)
    .join(",\n      ");
  const wherePreds = joinCols
    .map((c) => `AND c."${c}" = i."${c}"`)
    .join("\n        ");

  const rowsOut = await db.sequelize.query(
    `
    WITH input AS (
      SELECT *
      FROM jsonb_to_recordset(:input::jsonb)
        AS x(${recordsetCols}${recordsetCols ? ", " : ""}"refDate" date)
    )
    SELECT
      ${selectCols}${selectCols ? ",\n      " : ""}i."refDate" AS "refDate",
      c."newRaw"  AS "newRaw",
      c."changedAt" AS "changedAt"
    FROM input i
    LEFT JOIN LATERAL (
      SELECT "newRaw", "changedAt"
      FROM "tbl_ptrs_payment_term_change" c
      WHERE c."customerId" = :customerId
        AND c."profileId" = :profileId
        AND c."deletedAt" IS NULL
        ${wherePreds}
        AND (c."changedAt"::date) <= i."refDate"
      ORDER BY c."changedAt" DESC
      LIMIT 1
    ) c ON true
    `,
    {
      type: QueryTypes.SELECT,
      replacements: { customerId, profileId, input: JSON.stringify(input) },
      transaction,
    },
  );

  for (const r of rowsOut || []) {
    const term = r?.newRaw != null ? String(r.newRaw).trim() : "";
    if (!term) continue;

    const key = joinCols
      .map((c) => (r?.[c] != null ? String(r[c]).trim() : ""))
      .join("::");

    if (!key) continue;

    out.set(key, {
      term,
      changedAt: r?.changedAt || null,
    });
  }

  return out;
}

function applyEffectiveTermChangesToRows(rows, changeMap, mapRow) {
  const stats = {
    considered: 0,
    applied: 0,
    missingKey: 0,
    joinSpec: null,
  };

  if (
    !Array.isArray(rows) ||
    !changeMap ||
    typeof changeMap.get !== "function"
  ) {
    return { rows, stats };
  }

  const joinSpec = extractTermChangesJoinSpec(mapRow);

  const allowedJoinColumns = new Set([
    "companyCode",
    "supplier",
    "purchOrganization",
    "purchOrg",
    "purchasingOrganization",
  ]);

  const safeJoinSpec = joinSpec.filter(
    (s) =>
      s &&
      s.mainField &&
      s.changeColumn &&
      allowedJoinColumns.has(s.changeColumn),
  );

  const effectiveJoinSpec = safeJoinSpec;

  if (!effectiveJoinSpec.length) {
    const err = new Error(
      "Effective-dated payment term changes require an explicit resolvable join spec. Fix the term-changes join configuration and main-header mappings.",
    );
    err.statusCode = 400;
    throw err;
  }
  stats.joinSpec = effectiveJoinSpec;

  for (const r of rows) {
    if (!r || typeof r !== "object") continue;

    stats.considered += 1;

    const keyParts = [];
    let missing = false;

    for (const spec of effectiveJoinSpec) {
      const v = getRowValueByField(r, spec.mainField);
      if (!v) {
        missing = true;
        break;
      }
      keyParts.push(v);
    }

    if (missing || !keyParts.length) {
      stats.missingKey += 1;
      continue;
    }

    const key = keyParts.join("::");
    const hit = changeMap.get(key);
    if (!hit || !hit.term) continue;

    r.contract_po_payment_terms_effective = hit.term;
    r.contract_po_payment_terms_effective_source = "TERM_CHANGES";
    r.contract_po_payment_terms_effective_changed_at = hit.changedAt;

    stats.applied += 1;
  }

  return { rows, stats };
}

async function loadPaymentTermMap({ customerId, profileId, transaction }) {
  if (!customerId || !profileId) return new Map();

  const rows = await db.sequelize.query(
    `
    SELECT "raw", "transformedDays"
    FROM "tbl_ptrs_payment_term_map"
    WHERE "customerId" = :customerId
      AND "profileId" = :profileId
      AND "deletedAt" IS NULL
    `,
    {
      type: QueryTypes.SELECT,
      replacements: { customerId, profileId },
      transaction,
    },
  );

  const map = new Map();
  for (const r of rows || []) {
    const key = r?.raw != null ? String(r.raw).trim() : "";
    const val = Number(r?.transformedDays);
    if (key && Number.isFinite(val)) map.set(key, val);
  }
  return map;
}

function deriveTermCode(row) {
  if (!row || typeof row !== "object") return null;

  const candidates = [
    row.payment_term_days,
    row.paymentTermDays,
    row.invoice_payment_terms_effective,
    row.invoice_payment_terms_raw,
    row.invoice_payment_terms,
    row.payment_term,
    row.paymentTerm,
    row["vendormaster__Payment terms"],
    row.default_payment_term,
    row.defaultPaymentTerm,
    row.contract_po_payment_terms_effective,
    row.contract_po_payment_terms,
  ];

  for (const c of candidates) {
    if (c == null) continue;
    const s = String(c).trim();
    if (s) return s;
  }

  return null;
}

function inferTermDaysFromCode(code) {
  if (code == null) return null;
  const s = String(code).trim();
  if (!s) return null;

  if (/^\d{1,4}$/.test(s)) {
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  }

  return null;
}

async function seedMissingPaymentTermMapRows({
  customerId,
  profileId,
  mappings,
  transaction,
}) {
  if (!customerId || !profileId) return { inserted: 0 };
  if (!mappings || typeof mappings[Symbol.iterator] !== "function") {
    return { inserted: 0 };
  }

  let inserted = 0;

  for (const [term, days] of mappings) {
    const t = term != null ? String(term).trim() : "";
    const d = Number(days);
    if (!t || !Number.isFinite(d)) continue;

    const result = await db.sequelize.query(
      `
      INSERT INTO "tbl_ptrs_payment_term_map" (
        "customerId",
        "profileId",
        "raw",
        "transformedDays",
        "note",
        "createdBy",
        "updatedBy",
        "createdAt",
        "updatedAt"
      )
      SELECT
        :customerId,
        :profileId,
        :raw,
        :transformedDays,
        :note,
        :createdBy,
        :updatedBy,
        NOW(),
        NOW()
      WHERE NOT EXISTS (
        SELECT 1
        FROM "tbl_ptrs_payment_term_map"
        WHERE "customerId" = :customerId
          AND "profileId" = :profileId
          AND "raw" = :raw
          AND "deletedAt" IS NULL
      );
      `,
      {
        type: QueryTypes.INSERT,
        replacements: {
          customerId,
          profileId,
          raw: t,
          transformedDays: d,
          note: "Seeded from PTRS staging",
          createdBy: "system_mvp_seed",
          updatedBy: "system_mvp_seed",
        },
        transaction,
      },
    );

    const rowCount =
      (Array.isArray(result) && result[1] && result[1].rowCount) ||
      (result && result.rowCount) ||
      0;

    if (Number(rowCount) > 0) inserted += 1;
  }

  return { inserted };
}

function applyPaymentTermDaysFromMap(rows, termMap) {
  const stats = {
    lookedUp: 0,
    filled: 0,
    missing: 0,
    unmapped: 0,
    unmappedTermsSample: [],
  };

  if (!Array.isArray(rows) || !termMap || typeof termMap.get !== "function") {
    return { rows, stats };
  }

  for (const r of rows) {
    if (!r || typeof r !== "object") continue;

    const existing = r.payment_term_days;
    const hasExisting = existing !== null && typeof existing !== "undefined";
    if (hasExisting) continue;

    const code = deriveTermCode(r);
    if (!code) {
      stats.missing += 1;
      if (!Array.isArray(r._stageErrors)) r._stageErrors = [];
      r._stageErrors.push({
        code: "PAYMENT_TERM_MISSING",
        message:
          "Payment term code is missing; cannot derive payment_term_days",
        field: "invoice_payment_terms_effective",
        value: null,
      });
      continue;
    }

    stats.lookedUp += 1;

    const mapped = termMap.get(code);
    if (Number.isFinite(mapped)) {
      r.payment_term_days = mapped;
      stats.filled += 1;
      continue;
    }

    stats.unmapped += 1;
    stats.missing += 1;

    if (!Array.isArray(r._stageErrors)) r._stageErrors = [];
    r._stageErrors.push({
      code: "PAYMENT_TERM_UNMAPPED",
      message: "Payment term code is not mapped in tbl_ptrs_payment_term_map",
      field: "invoice_payment_terms_effective",
      value: code,
    });

    if (stats.unmappedTermsSample.length < 10)
      stats.unmappedTermsSample.push(code);
  }

  return { rows, stats };
}

module.exports = {
  deriveSupplierKey,
  deriveTermReferenceDate,
  normaliseHeaderToRowField,
  normaliseHeaderToDbColumn,
  getCanonicalFieldForMainHeader,
  extractTermChangesJoinSpec,
  getRowValueByField,
  loadEffectiveTermChangesForRows,
  applyEffectiveTermChangesToRows,
  loadPaymentTermMap,
  deriveTermCode,
  inferTermDaysFromCode,
  seedMissingPaymentTermMapRows,
  applyPaymentTermDaysFromMap,
};

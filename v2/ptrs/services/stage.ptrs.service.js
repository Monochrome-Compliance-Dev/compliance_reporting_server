const db = require("@/db/database");
const { QueryTypes } = require("sequelize");

const {
  safeMeta,
  slog,
  buildStableInputHash,
  createExecutionRun,
  getLatestExecutionRun,
  updateExecutionRun,
} = require("./ptrs.service");

const { applyRules } = require("./rules.ptrs.service");
const {
  loadMappedRowsForPtrs,
  getColumnMap,
} = require("./tablesAndMaps.ptrs.service");
const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

module.exports = {
  stagePtrs,
  getStagePreview,
};

// --- Payment time (regulator-aligned) helpers ---
function parseISODateOnly(value) {
  if (!value) return null;
  const s = String(value).trim();
  if (!s) return null;

  // Accept YYYY-MM-DD or full ISO; we only care about the date portion.
  const datePart = s.includes("T") ? s.split("T")[0] : s;
  const m = /^\d{4}-\d{2}-\d{2}$/.test(datePart) ? datePart : null;
  if (!m) return null;

  const [y, mo, d] = datePart.split("-").map((x) => Number(x));
  if (!Number.isFinite(y) || !Number.isFinite(mo) || !Number.isFinite(d))
    return null;

  // Use UTC midnight to avoid DST/local-time drift.
  const ms = Date.UTC(y, mo - 1, d);
  if (!Number.isFinite(ms)) return null;
  return { y, mo, d, ms, iso: datePart };
}

function diffDaysUTC(later, earlier) {
  if (!later || !earlier) return null;
  const ms = later.ms - earlier.ms;
  if (!Number.isFinite(ms)) return null;
  // Exact whole days because both are UTC midnight.
  return Math.floor(ms / (24 * 60 * 60 * 1000));
}

function isRctiYes(value) {
  if (value == null) return false;
  const s = String(value).trim().toLowerCase();
  return s === "yes" || s === "y" || s === "true";
}

function computePaymentTimeRegulator(row) {
  // Implements the regulator worked-example logic (Excel DAYS + inclusive +1 when > 0):
  // 1) If RCTI == Yes: DAYS(payment_date, invoice_issue_date)
  // 2) Else if invoice_issue_date blank AND notice_for_payment_issue_date blank:
  //      DAYS(payment_date, supply_date)
  // 3) Else if invoice_issue_date blank:
  //      DAYS(payment_date, notice_for_payment_issue_date)
  // 4) Else:
  //      MIN(DAYS(payment_date, invoice_issue_date), DAYS(payment_date, invoice_receipt_date))
  // 5) Final: IF(Calc<=0, 0, Calc+1)

  if (!row || typeof row !== "object")
    return { days: null, referenceDate: null, referenceKind: null };

  const payment = parseISODateOnly(row.payment_date);
  if (!payment) return { days: null, referenceDate: null, referenceKind: null };

  const issue = parseISODateOnly(row.invoice_issue_date);
  const receipt = parseISODateOnly(row.invoice_receipt_date);
  const notice = parseISODateOnly(row.notice_for_payment_issue_date);
  const supply = parseISODateOnly(row.supply_date);
  const due = parseISODateOnly(row.invoice_due_date);

  const rcti = isRctiYes(row.rcti);

  let calc = null;
  let ref = null;

  if (rcti) {
    // Prefer invoice issue date for RCTI path.
    if (!issue) return { days: null, referenceDate: null, referenceKind: null };
    calc = diffDaysUTC(payment, issue);
    ref = { referenceDate: issue.iso, referenceKind: "invoice_issue" };
  } else if (!issue && !notice) {
    // Both issue and notice missing -> use supply date.
    // MVP fallback: if supply is missing, allow invoice due date as the reference.
    if (supply) {
      calc = diffDaysUTC(payment, supply);
      ref = { referenceDate: supply.iso, referenceKind: "supply" };
    } else if (due) {
      calc = diffDaysUTC(payment, due);
      ref = { referenceDate: due.iso, referenceKind: "invoice_due" };
    } else {
      return { days: null, referenceDate: null, referenceKind: null };
    }
  } else if (!issue) {
    // Issue missing -> use notice for payment terms issue date.
    if (!notice)
      return { days: null, referenceDate: null, referenceKind: null };
    calc = diffDaysUTC(payment, notice);
    ref = { referenceDate: notice.iso, referenceKind: "notice_for_payment" };
  } else {
    // Issue present -> choose MIN of days vs issue and receipt.
    const dIssue = diffDaysUTC(payment, issue);
    const dReceipt = receipt ? diffDaysUTC(payment, receipt) : null;

    // If receipt is missing/invalid, fall back to issue.
    if (dReceipt == null || !Number.isFinite(dReceipt)) {
      calc = dIssue;
      ref = { referenceDate: issue.iso, referenceKind: "invoice_issue" };
    } else {
      // MIN days => reference date closest to payment date.
      if (dIssue == null || !Number.isFinite(dIssue)) {
        calc = dReceipt;
        ref = { referenceDate: receipt.iso, referenceKind: "invoice_receipt" };
      } else if (dIssue <= dReceipt) {
        calc = dIssue;
        ref = { referenceDate: issue.iso, referenceKind: "invoice_issue" };
      } else {
        calc = dReceipt;
        ref = { referenceDate: receipt.iso, referenceKind: "invoice_receipt" };
      }
    }
  }

  if (calc == null || !Number.isFinite(calc)) {
    return { days: null, referenceDate: null, referenceKind: null };
  }

  // Regulator: inclusive counting (+1) for positive values; clamp non-positive to 0.
  const days = calc <= 0 ? 0 : calc + 1;

  return {
    days,
    referenceDate: ref?.referenceDate || null,
    referenceKind: ref?.referenceKind || null,
  };
}

// --- Payment term change (effective-dated) helpers ---
function deriveSupplierKey(row) {
  // Supplier matching is optional. For MVP we key term changes primarily by company code
  // (as per user-configured dataset joins). Keep this helper for future extension.
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
  // The term-change dataset is effective-dated; we need a deterministic reference date.
  // Prefer invoice_issue_date (matches most worked-example expectations). If missing, fall back
  // to invoice_receipt_date, supply_date, then payment_date.
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
function normaliseHeaderToDbColumn(header) {
  // Convert dataset header -> camelCase DB column name (matches tbl_ptrs_payment_term_change columns)
  // e.g. "Company Code" -> "companyCode", "Purch. organization" -> "purchOrganization"
  if (!header) return null;
  const s = String(header).trim();
  if (!s) return null;

  // Remove non-alphanumerics except spaces, then camelCase
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
  // Attempt to map a "main" dataset column header to a canonical staged row field.
  // Falls back to a conservative normalisation.
  if (!header) return null;
  const h = String(header).trim();
  if (!h) return null;

  // 1) If mappings exist and are an object keyed by source header.
  const mappings = mapRow && mapRow.mappings ? mapRow.mappings : null;
  if (mappings && typeof mappings === "object") {
    const direct = mappings[h];
    if (direct) {
      // Column map is authoritative: we ONLY use its configured target field.
      // No guessing, no fallbacks.
      if (typeof direct === "string") return direct;
      if (typeof direct === "object") {
        return direct.field || null;
      }
    }

    // Some implementations store mappings as arrays; do a best-effort scan.
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

  // No fallbacks. If the join field isn't mapped, the user must fix the map.
  return null;
}

function extractTermChangesJoinSpec(mapRow) {
  // Reads mapRow.joins and returns an array of join fields for term changes.
  // Output: [{ mainField: "company_code", changeColumn: "companyCode" }, ...]
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
      c.from.role === "main" &&
      c.to.column &&
      c.from.column,
  );

  // Default to Company Code if nothing configured
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

    // No guessing: if a join is configured but not mapped, we surface it by skipping it here
    // and letting the caller validate against staged row keys.
    if (mainField && changeColumn) spec.push({ mainField, changeColumn });
  }

  // De-dupe
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

  // STRICT: only the exact canonical key is allowed.
  // If the column map says the join uses `supplier`, then the staged row must contain `supplier`.
  if (!Object.prototype.hasOwnProperty.call(row, field)) return null;

  const v = row[field];
  if (v == null) return null;
  const s = String(v).trim();
  return s ? s : null;
}

// Updated effective-dated payment term change logic to use user-configured join conditions
async function loadEffectiveTermChangesForRows({
  customerId,
  profileId,
  rows,
  mapRow,
  transaction,
}) {
  // Returns a Map keyed by the configured join field values (from mapRow.joins)
  // => { term, changedAt }
  const out = new Map();
  if (!customerId || !profileId) return out;
  if (!Array.isArray(rows) || rows.length === 0) return out;

  const joinSpec = extractTermChangesJoinSpec(mapRow);

  // Whitelist of join columns that may exist on tbl_ptrs_payment_term_change.
  // (Add more here when you add columns to the model/table.)
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

  // Default if nothing configured/allowed
  const effectiveJoinSpec = safeJoinSpec.length
    ? safeJoinSpec
    : [{ mainField: "company_code", changeColumn: "companyCode" }];

  // If the user configured term-change joins, the join fields MUST exist on staged rows.
  // No guessing or fallbacks — fail loudly (and let the caller decide how to handle it).
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

  // Build a small distinct input set so Postgres can do the heavy lifting once.
  // We ONLY include rows that have all join fields + a reference date.
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

  // Build SQL fragments safely from whitelisted join columns.
  const joinCols = effectiveJoinSpec.map((s) => s.changeColumn);

  // jsonb_to_recordset column list
  const recordsetCols = joinCols.map((c) => `"${c}" text`).join(", ");

  // SELECT list
  const selectCols = joinCols
    .map((c) => `i."${c}" AS "${c}"`)
    .join(",\n      ");

  // WHERE predicates for lateral join
  const wherePreds = joinCols
    .map((c) => `AND c."${c}" = i."${c}"`)
    .join("\n        ");

  // Debug: log the generated SQL fragments + join spec so we can verify dynamic joins
  // without inlining bound params (avoid leaking potentially sensitive values).
  const _debugTermChangeSql = `
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
  `;

  slog.info("PTRS v2 term changes: joinSpec + SQL", {
    action: "PtrsV2TermChangesSQL",
    customerId,
    profileId,
    joinSpec: effectiveJoinSpec,
    joinCols,
    sql: _debugTermChangeSql,
    replacementsPreview: {
      customerId,
      profileId,
      inputRows: input.length,
      sampleInput: input[0] || null,
    },
  });

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

// Updated to accept mapRow and use dynamic join keys
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

  const effectiveJoinSpec = safeJoinSpec.length
    ? safeJoinSpec
    : [{ mainField: "company_code", changeColumn: "companyCode" }];

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

    // IMPORTANT: do NOT overwrite invoice term fields by default.
    // This sets an effective contract/PO term that can be used as a fallback.
    r.contract_po_payment_terms_effective = hit.term;
    r.contract_po_payment_terms_effective_source = "TERM_CHANGES";
    r.contract_po_payment_terms_effective_changed_at = hit.changedAt;

    stats.applied += 1;
  }

  return { rows, stats };
}

// --- Payment term mapping helpers ---
async function loadPaymentTermMap({ customerId, profileId, transaction }) {
  if (!customerId || !profileId) return new Map();

  // Use raw SQL so we're not coupled to a Sequelize model name/attribute mapping.
  // NOTE: quoted identifiers because Postgres will fold unquoted names to lower-case.
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
    // If the user mapped numeric days directly, prefer that
    row.payment_term_days,
    row.paymentTermDays,

    // Invoice-derived fields first (preferred)
    row.invoice_payment_terms_effective,
    row.invoice_payment_terms_raw,
    row.payment_term,
    row.paymentTerm,

    // System/user default fallback (e.g. "30")
    row.default_payment_term,
    row.defaultPaymentTerm,

    // Contract/PO effective term as a fallback (do not override invoice terms)
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
  // MVP: if the mapped term is already a number (e.g. "30"), treat it as explicit days.
  // Otherwise, do not guess.
  if (code == null) return null;
  const s = String(code).trim();
  if (!s) return null;

  // Accept plain integer strings.
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

  // We avoid ON CONFLICT because your table may not have the required unique constraint.
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

    // Sequelize returns different shapes depending on dialect/versions; be defensive.
    // If the INSERT actually happened, Postgres will report rowCount=1.
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

    // Only fill if not already present.
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

    // No guessing in staging. If it's not mapped, make it visible.
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

/**
 * Stage data for a ptrs. Reuses previewTransform pipeline to project/optionally filter, then
 * (when persist=true) writes rows into tbl_ptrs_stage_row and updates ptrs status.
 * Returns { sample, affectedCount, persistedCount? }.
 * RLS-aware: runs in beginTransactionWithCustomerContext and passes transaction to all DB calls.
 */
async function stagePtrs({
  customerId,
  ptrsId,
  steps = [],
  persist = false,
  limit = null,
  userId,
  profileId = null,
}) {
  let executionRun = null;
  let inputHash = null;

  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  // Persist runs must be attributable to a profile.
  if (persist && !profileId) {
    const e = new Error("profileId is required when persist=true");
    e.statusCode = 400;
    throw e;
  }

  const started = Date.now();
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // --- Execution run tracking (only for persist runs) ---

    if (persist) {
      // Hash inputs that materially affect staging.
      // We intentionally hash metadata/config, not full row data.
      const [
        mapRow,
        rawCount,
        rawMaxUpdatedAt,
        datasets,
        fieldMapUpdatedAt,
        fieldMapCount,
        paymentTermMapUpdatedAt,
        paymentTermMapCount,
        paymentTermChangeUpdatedAt,
        paymentTermChangeCount,
      ] = await Promise.all([
        getColumnMap({ customerId, ptrsId, transaction: t }),
        db.PtrsImportRaw.count({
          where: { customerId, ptrsId },
          transaction: t,
        }),
        db.PtrsImportRaw.max("updatedAt", {
          where: { customerId, ptrsId },
          transaction: t,
        }),
        // v2 datasets (tbl_ptrs_dataset)
        db.PtrsDataset
          ? db.PtrsDataset.findAll({
              where: { customerId, ptrsId },
              attributes: ["id", "role", "updatedAt"],
              order: [
                ["role", "ASC"],
                ["updatedAt", "DESC"],
              ],
              transaction: t,
              raw: true,
            })
          : Promise.resolve([]),
        db.PtrsFieldMap
          ? db.PtrsFieldMap.max("updatedAt", {
              where: { customerId, ptrsId, profileId },
              transaction: t,
            })
          : Promise.resolve(null),
        db.PtrsFieldMap
          ? db.PtrsFieldMap.count({
              where: { customerId, ptrsId, profileId },
              transaction: t,
            })
          : Promise.resolve(0),
        // Payment term map affects staged canonical fields
        (async () => {
          const rows = await db.sequelize.query(
            `
            SELECT MAX("updatedAt") AS "maxUpdatedAt"
            FROM "tbl_ptrs_payment_term_map"
            WHERE "customerId" = :customerId
              AND "profileId" = :profileId
              AND "deletedAt" IS NULL
            `,
            {
              type: QueryTypes.SELECT,
              replacements: { customerId, profileId },
              transaction: t,
            },
          );
          return rows && rows[0] ? rows[0].maxUpdatedAt || null : null;
        })(),
        (async () => {
          const rows = await db.sequelize.query(
            `
            SELECT COUNT(1)::int AS "count"
            FROM "tbl_ptrs_payment_term_map"
            WHERE "customerId" = :customerId
              AND "profileId" = :profileId
              AND "deletedAt" IS NULL
            `,
            {
              type: QueryTypes.SELECT,
              replacements: { customerId, profileId },
              transaction: t,
            },
          );
          return rows && rows[0] ? Number(rows[0].count) || 0 : 0;
        })(),

        // Effective-dated term changes affect staging outcomes
        (async () => {
          const rows = await db.sequelize.query(
            `
            SELECT MAX("updatedAt") AS "maxUpdatedAt"
            FROM "tbl_ptrs_payment_term_change"
            WHERE "customerId" = :customerId
              AND "profileId" = :profileId
              AND "deletedAt" IS NULL
            `,
            {
              type: QueryTypes.SELECT,
              replacements: { customerId, profileId },
              transaction: t,
            },
          );
          return rows && rows[0] ? rows[0].maxUpdatedAt || null : null;
        })(),
        (async () => {
          const rows = await db.sequelize.query(
            `
            SELECT COUNT(1)::int AS "count"
            FROM "tbl_ptrs_payment_term_change"
            WHERE "customerId" = :customerId
              AND "profileId" = :profileId
              AND "deletedAt" IS NULL
            `,
            {
              type: QueryTypes.SELECT,
              replacements: { customerId, profileId },
              transaction: t,
            },
          );
          return rows && rows[0] ? Number(rows[0].count) || 0 : 0;
        })(),
      ]);

      inputHash = buildStableInputHash({
        ptrsId,
        customerId,
        profileId: profileId || null,
        map: mapRow
          ? {
              id: mapRow.id || null,
              updatedAt: mapRow.updatedAt || null,
              mappings: mapRow.mappings || null,
              joins: mapRow.joins || null,
              customFields: mapRow.customFields || null,
              rowRules: mapRow.rowRules || null,
            }
          : null,
        fieldMap: {
          profileId: profileId || null,
          count: Number(fieldMapCount) || 0,
          maxUpdatedAt: fieldMapUpdatedAt || null,
        },
        importRaw: {
          rowCount: rawCount || 0,
          maxUpdatedAt: rawMaxUpdatedAt || null,
        },
        datasets: Array.isArray(datasets)
          ? datasets.map((d) => ({
              id: d.id,
              role: d.role,
              updatedAt: d.updatedAt || null,
            }))
          : [],
        paymentTermMap: {
          profileId: profileId || null,
          count: Number(paymentTermMapCount) || 0,
          maxUpdatedAt: paymentTermMapUpdatedAt || null,
        },
        paymentTermChanges: {
          profileId: profileId || null,
          count: Number(paymentTermChangeCount) || 0,
          maxUpdatedAt: paymentTermChangeUpdatedAt || null,
        },
      });

      const previous = await getLatestExecutionRun({
        customerId,
        ptrsId,
        step: "stage",
        transaction: t,
      });

      if (
        previous &&
        previous.status === "success" &&
        previous.inputHash === inputHash
      ) {
        const existingStageCount = await db.PtrsStageRow.count({
          where: { customerId, ptrsId },
          transaction: t,
        });

        slog.info(
          "PTRS v2 stagePtrs: inputs unchanged; skipping persist staging",
          {
            action: "PtrsV2StagePtrsSkipped",
            customerId,
            ptrsId,
            profileId: profileId || null,
            inputHash,
            previousRunId: previous.id || null,
            existingStageCount,
          },
        );

        await t.commit();
        return {
          skipped: true,
          reason: "INPUT_UNCHANGED",
          inputHash,
          previousRunId: previous.id || null,
          persistedCount: existingStageCount,
          rowsIn: null,
          rowsOut: null,
          tookMs: Date.now() - started,
          sample: null,
          stats: null,
        };
      }

      slog.info("PTRS v2 stagePtrs: execution input hash", {
        action: "PtrsV2StagePtrsInputHash",
        customerId,
        ptrsId,
        profileId: profileId || null,
        inputHash,
        previousHash: previous?.inputHash || null,
        rawCount: rawCount || 0,
        datasetsCount: Array.isArray(datasets) ? datasets.length : 0,
        paymentTermMapCount: Number(paymentTermMapCount) || 0,
        paymentTermMapUpdatedAt: paymentTermMapUpdatedAt || null,
        paymentTermChangeCount: Number(paymentTermChangeCount) || 0,
        paymentTermChangeUpdatedAt: paymentTermChangeUpdatedAt || null,
      });

      executionRun = await createExecutionRun({
        customerId,
        ptrsId,
        profileId,
        step: "stage",
        inputHash,
        status: "running",
        startedAt: new Date(),
        createdBy: userId || null,
        transaction: t,
      });
    }

    // 1) Compose mapped rows for this ptrs (import + joins + column map)
    const { rows: baseRows } = await loadMappedRowsForPtrs({
      customerId,
      ptrsId,
      limit: null,
      transaction: t,
    });

    // Canonical fields are already materialised on mapped rows before staging.
    // Staging must not re-project/guess fields.
    const rows = baseRows;

    slog.info("PTRS v2 stagePtrs: loaded mapped rows", {
      action: "PtrsV2StagePtrsLoadedMappedRows",
      customerId,
      ptrsId,
      rowsCount: Array.isArray(rows) ? rows.length : 0,
      sampleRowKeys: rows && rows[0] ? Object.keys(rows[0]) : null,
    });

    // Load the column map once; used by rules + term-change joins
    let mapRow = null;
    try {
      mapRow = await getColumnMap({ customerId, ptrsId, transaction: t });
    } catch (_) {
      mapRow = null;
    }

    // 2) Apply row-level rules (if any) independently of preview
    let stagedRows = rows;
    let rulesStats = null;

    try {
      let rowRules = mapRow && mapRow.rowRules != null ? mapRow.rowRules : null;
      if (typeof rowRules === "string") {
        try {
          rowRules = JSON.parse(rowRules);
        } catch {
          rowRules = null;
        }
      }

      const rulesResult = applyRules(
        stagedRows,
        Array.isArray(rowRules) ? rowRules : [],
      );
      stagedRows = rulesResult.rows || stagedRows;
      rulesStats = rulesResult.stats || null;
    } catch (err) {
      slog.warn("PTRS v2 stagePtrs: failed to apply row rules", {
        action: "PtrsV2StagePtrsApplyRules",
        customerId,
        ptrsId,
        error: err.message,
      });
    }

    // 2a) Apply effective-dated payment term changes (SQL-heavy) as a fallback term source
    // This does NOT overwrite invoice term fields; it only populates contract/PO effective term fields.
    let paymentTermChangeStats = null;
    try {
      if (profileId) {
        const changeMap = await loadEffectiveTermChangesForRows({
          customerId,
          profileId,
          rows: stagedRows,
          mapRow,
          transaction: t,
        });

        const changeResult = applyEffectiveTermChangesToRows(
          stagedRows,
          changeMap,
          mapRow,
        );
        stagedRows = changeResult.rows || stagedRows;
        paymentTermChangeStats = changeResult.stats || null;

        if (paymentTermChangeStats?.applied) {
          slog.info(
            "PTRS v2 stagePtrs: applied effective-dated payment term changes",
            {
              action: "PtrsV2StagePtrsPaymentTermChangesApplied",
              customerId,
              ptrsId,
              profileId,
              ...paymentTermChangeStats,
              joinSpec: paymentTermChangeStats?.joinSpec || null,
            },
          );
        }
        if (!paymentTermChangeStats?.applied) {
          slog.info(
            "PTRS v2 stagePtrs: no effective-dated payment term changes applied",
            {
              action: "PtrsV2StagePtrsPaymentTermChangesNoneApplied",
              customerId,
              ptrsId,
              profileId,
              ...paymentTermChangeStats,
              joinSpec: paymentTermChangeStats?.joinSpec || null,
              note: "No matches found using company_code join key (and optional supplier) against tbl_ptrs_payment_term_change",
            },
          );
        }
      }
    } catch (err) {
      slog.warn("PTRS v2 stagePtrs: failed to apply payment term changes", {
        action: "PtrsV2StagePtrsPaymentTermChangesFailed",
        customerId,
        ptrsId,
        profileId: profileId || null,
        error: err?.message,
      });
    }

    // 2b) Derive payment_term_days from tbl_ptrs_payment_term_map (profile-scoped)
    // This keeps metrics simple: they just read `payment_term_days` from staged JSON.
    let paymentTermStats = null;
    try {
      if (profileId) {
        const termMap = await loadPaymentTermMap({
          customerId,
          profileId,
          transaction: t,
        });

        const termResult = applyPaymentTermDaysFromMap(stagedRows, termMap);
        stagedRows = termResult.rows;
        paymentTermStats = termResult.stats;

        if (
          paymentTermStats?.missing ||
          paymentTermStats?.filled ||
          paymentTermStats?.unmapped
        ) {
          slog.info("PTRS v2 stagePtrs: payment term mapping stats", {
            action: "PtrsV2StagePtrsPaymentTermMap",
            customerId,
            ptrsId,
            profileId,
            ...paymentTermStats,
          });
        }
      } else {
        slog.warn(
          "PTRS v2 stagePtrs: profileId not provided; skipping payment term mapping",
          {
            action: "PtrsV2StagePtrsPaymentTermMapSkipped",
            customerId,
            ptrsId,
          },
        );
      }
    } catch (err) {
      slog.warn("PTRS v2 stagePtrs: failed to apply payment term mapping", {
        action: "PtrsV2StagePtrsPaymentTermMapFailed",
        customerId,
        ptrsId,
        profileId: profileId || null,
        error: err?.message,
      });
    }

    // 2c) Derive payment_time_days according to regulator worked-example logic
    // We do this in staging so Metrics can remain dumb and deterministic.
    try {
      for (const r of stagedRows) {
        if (!r || typeof r !== "object") continue;

        const res = computePaymentTimeRegulator(r);
        if (res?.days == null) {
          // Leave existing value (if any), but surface as a stage error if missing.
          if (r.payment_time_days == null) {
            if (!Array.isArray(r._stageErrors)) r._stageErrors = [];
            r._stageErrors.push({
              code: "PAYMENT_TIME_UNDERIVED",
              message:
                "Payment time could not be derived using regulator rules (missing required date fields)",
              field: "payment_time_days",
              value: null,
            });
          }
          continue;
        }

        r.payment_time_days = res.days;
        if (res.referenceDate)
          r.payment_time_reference_date = res.referenceDate;
        if (res.referenceKind)
          r.payment_time_reference_kind = res.referenceKind;
      }
    } catch (err) {
      slog.warn("PTRS v2 stagePtrs: failed to derive payment_time_days", {
        action: "PtrsV2StagePtrsPaymentTimeDerivationFailed",
        customerId,
        ptrsId,
        error: err?.message,
      });
    }

    // 3) Persist into tbl_ptrs_stage_row if requested
    let persistedCount = null;
    if (persist) {
      const basePayload = stagedRows.map((r) => {
        const rowNoVal = Number(r?.row_no ?? r?.rowNo ?? 0) || 0;

        // Persist the full resolved row into JSONB `data`.
        // NOTE: tbl_ptrs_stage_row only has: customerId, ptrsId, rowNo, data, errors, meta (+ timestamps)
        const dataObjBase =
          r && typeof r === "object" && Object.keys(r).length
            ? r
            : { _warning: "⚠️ No mapped data for this row" };

        // If the rules engine excluded the row, persist canonical exclusion fields.
        const shouldExclude = !!r?.exclude;
        const excludeComment =
          Array.isArray(r?._warnings) && r._warnings.length
            ? String(r._warnings[0])
            : shouldExclude
              ? "Excluded by rule"
              : null;

        const dataObj = shouldExclude
          ? {
              ...dataObjBase,
              exclude_from_metrics: true,
              exclude_comment: excludeComment,
              exclude_set_at: new Date().toISOString(),
              exclude_set_by: userId || null,
            }
          : dataObjBase;

        return {
          customerId: String(customerId),
          ptrsId: String(ptrsId),
          rowNo: rowNoVal,
          data: dataObj,
          errors:
            Array.isArray(r?._stageErrors) && r._stageErrors.length
              ? r._stageErrors
              : null,
          meta: {
            _stage: "ptrs.v2.stagePtrs",
            at: new Date().toISOString(),
            rules: {
              applied: Array.isArray(r?._appliedRules) ? r._appliedRules : [],
              exclude: !!r?.exclude,
              exclude_comment: dataObj?.exclude_comment || null,
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
        for (const key of ["data", "errors", "meta"]) {
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

      slog.info("PTRS v2 stagePtrs: preparing to insert", {
        action: "PtrsV2StagePtrsBatch",
        customerId,
        ptrsId,
        batchSize: safePayload.length,
        sampleRow: safeMeta(safePayload[0] || {}),
      });

      const offenders = safePayload
        .filter((p) => {
          const hasWarn = Boolean(
            p?.data?._warning || p?.errors?._warning || p?.meta?._warning,
          );
          const hasEmpty =
            isEmptyPlain(p?.data) ||
            isEmptyPlain(p?.errors) ||
            isEmptyPlain(p?.meta);
          return hasWarn || hasEmpty;
        })
        .slice(0, 3)
        .map((p) => ({
          rowNo: p.rowNo,
          dataKeys: p.data ? Object.keys(p.data) : null,
          hasWarning: Boolean(
            p?.data?._warning || p?.errors?._warning || p?.meta?._warning,
          ),
        }));

      if (offenders.length) {
        slog.warn("PTRS v2 stagePtrs: warning/empty JSONB rows detected", {
          action: "PtrsV2StagePtrsWarningRows",
          ptrsId,
          customerId,
          offenderCount: offenders.length,
          sample: safeMeta(offenders),
        });
      }

      // IMPORTANT: staging must be idempotent.
      // On persist runs, we soft-delete any existing *active* stage rows for this ptrs
      // before inserting the new snapshot. We do this with raw SQL so we are not
      // dependent on Sequelize's destroy/paranoid behaviour (and to avoid silent no-ops).
      try {
        const [_, meta] = await db.sequelize.query(
          `
          UPDATE "tbl_ptrs_stage_row"
          SET "deletedAt" = NOW(), "updatedAt" = NOW()
          WHERE "customerId" = :customerId
            AND "ptrsId" = :ptrsId
            AND "deletedAt" IS NULL
          `,
          {
            type: QueryTypes.UPDATE,
            replacements: { customerId, ptrsId },
            transaction: t,
          },
        );

        // `meta` shape varies by Sequelize version; log defensively.
        const clearedCount =
          (meta && typeof meta.rowCount === "number" && meta.rowCount) ||
          (meta &&
            typeof meta.affectedRows === "number" &&
            meta.affectedRows) ||
          0;

        slog.info("PTRS v2 stagePtrs: cleared active stage rows", {
          action: "PtrsV2StagePtrsClearActive",
          customerId,
          ptrsId,
          clearedCount,
        });
      } catch (e) {
        // If this fails, do NOT proceed to insert (otherwise we will duplicate rows).
        slog.error("PTRS v2 stagePtrs: failed to clear active stage rows", {
          action: "PtrsV2StagePtrsClearActiveFailed",
          customerId,
          ptrsId,
          error: e?.message,
        });
        throw e;
      }

      if (safePayload.length) {
        try {
          await db.PtrsStageRow.bulkCreate(safePayload, {
            validate: false,
            returning: false,
            transaction: t,
          });
        } catch (e) {
          // If RLS blocks inserts, or the model/table are out of sync, we want this to be unmistakable.
          slog.error("PTRS v2 stagePtrs: bulkCreate failed", {
            action: "PtrsV2StagePtrsBulkCreateFailed",
            customerId,
            ptrsId,
            error: e?.message,
          });
          throw e;
        }
      }

      // With paranoid enabled, this count will automatically exclude soft-deleted rows.
      persistedCount = await db.PtrsStageRow.count({
        where: { customerId, ptrsId },
        transaction: t,
      });

      slog.info("PTRS v2 stagePtrs: persistence check", {
        action: "PtrsV2StagePtrsPersistedCount",
        customerId,
        ptrsId,
        attempted: safePayload.length,
        persistedCount,
      });
    }

    const tookMs = Date.now() - started;
    // Ensure persistedCount is calculated before commit
    if (!persist) {
      persistedCount = null;
    }

    if (executionRun?.id) {
      try {
        await updateExecutionRun({
          customerId,
          executionRunId: executionRun.id,
          status: "success",
          finishedAt: new Date(),
          rowsIn: stagedRows.length,
          rowsOut: stagedRows.length,
          stats: {
            rules: rulesStats,
            paymentTerms: paymentTermStats,
            paymentTermChanges: paymentTermChangeStats,
          },
          errorMessage: null,
          updatedBy: userId || null,
          transaction: t,
        });
      } catch (e) {
        slog.warn(
          "PTRS v2 stagePtrs: failed to update execution run (non-fatal)",
          {
            action: "PtrsV2StagePtrsUpdateExecutionRunFailed",
            customerId,
            ptrsId,
            executionRunId: executionRun.id,
            error: e?.message,
          },
        );
      }
    }

    await t.commit();

    return {
      rowsIn: stagedRows.length,
      rowsOut: stagedRows.length,
      persistedCount,
      tookMs,
      sample: stagedRows[0] || null,
      stats: {
        rules: rulesStats,
        paymentTerms: paymentTermStats,
        paymentTermChanges: paymentTermChangeStats,
      },
    };
  } catch (err) {
    if (executionRun?.id) {
      try {
        await updateExecutionRun({
          customerId,
          executionRunId: executionRun.id,
          status: "failed",
          finishedAt: new Date(),
          errorMessage: err?.message || "Stage failed",
          updatedBy: userId || null,
          transaction: t,
        });
      } catch (e) {
        // best-effort only
      }
    }

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

/**
 * Returns a preview of staged data using the persisted staging table
 * (tbl_ptrs_stage_row). We:
 *  - read a limited page of rows for preview
 *  - get a full count for this ptrsId
 * so the FE can show "20 of 208,811 rows".
 */
async function getStagePreview({
  customerId,
  ptrsId,
  limit = 50,
  profileId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  let canon = [];
  try {
    const where = { customerId, ptrsId };

    // Pull a preview page and a full count in parallel
    const [rowsRaw, totalRows] = await Promise.all([
      db.PtrsStageRow.findAll({
        where,
        order: [["rowNo", "ASC"]],
        limit,
        transaction: t,
      }),
      db.PtrsStageRow.count({ where, transaction: t }),
    ]);

    const rows = rowsRaw.map((r) =>
      typeof r.toJSON === "function" ? r.toJSON() : r,
    );

    // Derive headers from all rows' JSONB payloads (data/standard/custom)
    const headerSet = new Set();

    const materialiseObj = (value) => {
      if (!value) return null;
      if (typeof value === "string") {
        try {
          const parsed = JSON.parse(value);
          return parsed && typeof parsed === "object" ? parsed : null;
        } catch {
          return null;
        }
      }
      if (typeof value === "object") return value;
      return null;
    };

    // If a field map exists for this profile, prefer its canonical fields as the primary header list.
    if (profileId && db.PtrsFieldMap) {
      try {
        const fm = await db.PtrsFieldMap.findAll({
          where: { customerId, ptrsId, profileId },
          attributes: ["canonicalField"],
          order: [["canonicalField", "ASC"]],
          transaction: t,
          raw: true,
        });

        canon = Array.isArray(fm)
          ? fm.map((r) => r.canonicalField).filter(Boolean)
          : [];

        if (canon.length) {
          canon.forEach((k) => headerSet.add(k));
        }
      } catch (_) {
        // ignore
      }
    }

    for (const row of rows) {
      if (!row) continue;
      const buckets = [row.data];
      for (const bucket of buckets) {
        const obj = materialiseObj(bucket);
        if (!obj) continue;
        Object.keys(obj).forEach((k) => headerSet.add(k));
      }
    }

    // Remove the post-row field map header block (now handled above)

    // Order headers: canonical fields first (if present), then discovered fields
    let headers;
    if (canon.length) {
      const canonSet = new Set(canon);
      headers = [
        ...canon.filter((h) => headerSet.has(h)),
        ...Array.from(headerSet).filter((h) => !canonSet.has(h)),
      ];
    } else {
      headers = Array.from(headerSet);
    }

    await t.commit();

    return {
      headers,
      rows,
      totalRows,
      stats: null,
    };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch {
        // ignore rollback errors
      }
    }
    throw err;
  }
}

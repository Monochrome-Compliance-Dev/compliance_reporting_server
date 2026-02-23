const db = require("@/db/database");

const { logger } = require("@/helpers/logger");
const {
  safeMeta,
  slog,
  normalizeJoinKeyValue,
  toSnake,
  buildStableInputHash,
} = require("@/v2/ptrs/services/ptrs.service");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  pickFromRowLoose,
  getDatasetSample,
} = require("@/v2/ptrs/services/data.ptrs.service");

const { normalizeAmountLike } = require("@/helpers/amountNormaliser");

const { Op } = require("sequelize");

const { createPtrsTrace, hrMsSince } = require("@/helpers/ptrsTrackerLog");

// --- Map compatibility helpers ---
function normHeaderKey(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "")
    .trim();
}

function buildMapMetaFromMappings(
  mappings,
  signature = null,
  updatedAtIso = null,
) {
  const m =
    mappings && typeof mappings === "object" && !Array.isArray(mappings)
      ? mappings
      : {};
  const sourceHeaders = Object.keys(m);
  const sourceHeadersNorm = sourceHeaders.map(normHeaderKey).filter(Boolean);

  const targets = Array.from(
    new Set(
      Object.values(m)
        .map((cfg) => {
          if (cfg == null) return null;
          if (typeof cfg === "string") return cfg;
          return cfg?.field || null;
        })
        .filter((v) => v != null && String(v).trim() !== "")
        .map((v) => String(v).trim()),
    ),
  );

  return {
    version: 1,
    sourceHeaders,
    sourceHeadersNorm,
    targets,
    // Only set updatedAt when the caller is actually persisting a material change
    updatedAt: updatedAtIso || null,
    signature: signature || null,
  };
}

function safeParseJsonObject(v) {
  if (v == null) return null;
  if (typeof v === "object" && !Array.isArray(v)) return v;
  if (typeof v === "string") {
    try {
      const parsed = JSON.parse(v);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed))
        return parsed;
    } catch (_) {
      return null;
    }
  }
  return null;
}

function extractMapMetaFromExtras(extras) {
  const obj = safeParseJsonObject(extras) || {};
  const meta = obj?.mapMeta;
  if (!meta || typeof meta !== "object") return null;
  // No shims: only accept the current version.
  if (meta.version !== 1) return null;
  return meta;
}

function safeParseJsonAny(v) {
  if (v == null) return null;
  if (typeof v === "string") {
    try {
      return JSON.parse(v);
    } catch {
      return v;
    }
  }
  return v;
}

function buildMaterialMapSignature({ mappings, joins, customFields }) {
  // Only include things that actually affect mapped rows content/shape.
  return buildStableInputHash({
    mappings: safeParseJsonAny(mappings) || null,
    joins: safeParseJsonAny(joins) || null,
    customFields: safeParseJsonAny(customFields) || null,
  });
}

module.exports = {
  getMap,
  getColumnMap,
  getImportSample,
  saveColumnMap,
  buildMappedDatasetForPtrs,
  composeMappedRowsForPtrs,
  loadMappedRowsForPtrs,
  // getUnifiedSample,
  getFieldMap,
  saveFieldMap,
  listCompatibleMaps,
  getMainDatasetHeaderInfo,
};
/**
 * Cheap header + example extraction for the MAIN dataset.
 *
 * This is intentionally lightweight and should NOT touch PtrsImportRaw.
 * It prefers PtrsDataset.meta.headers and only falls back to a small dataset sample.
 */
async function getMainDatasetHeaderInfo({
  customerId,
  ptrsId,
  limit = 5,
  offset = 0,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const isExternalTx = !!transaction;

  const isMainRole = (role) => {
    const r = String(role || "").toLowerCase();
    return r === "main" || r.startsWith("main_");
  };

  try {
    const dsRows = await db.PtrsDataset.findAll({
      where: { customerId, ptrsId },
      attributes: ["id", "meta", "role"],
      raw: true,
      transaction: t,
    });

    const datasets = Array.isArray(dsRows) ? dsRows : [];
    const main =
      datasets.find((d) => isMainRole(d?.role)) || datasets[0] || null;

    const datasetId = main?.id || null;
    const meta = main?.meta && typeof main.meta === "object" ? main.meta : {};

    // Prefer precomputed headers from dataset metadata
    let headers = Array.isArray(meta.headers) ? meta.headers : [];

    // Build examples by sampling a few rows from the dataset sample endpoint.
    // This is the same lightweight mechanism used in the joins step.
    let examplesByHeader = {};

    try {
      const sample = datasetId
        ? await getDatasetSample({
            customerId,
            datasetId,
            limit,
            offset,
          })
        : null;

      if (sample) {
        if (!headers.length && Array.isArray(sample.headers)) {
          headers = sample.headers;
        }

        const rows = Array.isArray(sample.rows) ? sample.rows : [];
        for (const row of rows) {
          for (const [k, v] of Object.entries(row || {})) {
            if (examplesByHeader[k] != null) continue;
            if (v == null) continue;
            const s = String(v).trim();
            if (!s) continue;
            examplesByHeader[k] = v;
          }
        }
      }
    } catch (_) {
      // ignore sample failures; headers/examples are best-effort
    }

    if (!isExternalTx && !t.finished) {
      await t.commit();
    }

    return {
      datasetId,
      headers: Array.isArray(headers) ? headers.map((h) => String(h)) : [],
      examplesByHeader,
    };
  } catch (err) {
    if (!isExternalTx && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

const {
  PTRS_CANONICAL_CONTRACT,
} = require("@/v2/ptrs/contracts/ptrs.canonical.contract");

const CANONICAL_FIELDS = Object.keys(PTRS_CANONICAL_CONTRACT?.fields || {});

function toIsoDateOnlyUtc(d) {
  if (!(d instanceof Date)) return null;
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function parseDateFlexible(value) {
  if (value == null) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;

  const s = String(value).trim();
  if (!s) return null;

  // dd/mm/yyyy
  const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(s);
  if (m) {
    const dd = Number(m[1]);
    const mm = Number(m[2]);
    const yyyy = Number(m[3]);
    if (!Number.isFinite(dd) || !Number.isFinite(mm) || !Number.isFinite(yyyy))
      return null;
    const d = new Date(Date.UTC(yyyy, mm - 1, dd));
    if (
      d.getUTCFullYear() !== yyyy ||
      d.getUTCMonth() !== mm - 1 ||
      d.getUTCDate() !== dd
    )
      return null;
    return d;
  }

  // ISO-ish
  const iso = new Date(s);
  if (!Number.isNaN(iso.getTime())) return iso;

  return null;
}

function diffDaysClamped(startDate, endDate) {
  if (!(startDate instanceof Date) || !(endDate instanceof Date)) return null;
  const ms = endDate.getTime() - startDate.getTime();
  const days = ms / (1000 * 60 * 60 * 24);
  if (!Number.isFinite(days)) return null;
  // Regulator examples treat negatives as 0 in practice
  return Math.max(0, Math.round(days));
}

function computePaymentTimeReference(data) {
  // Regulator logic (worked example):
  // - If invoice dates exist: use the shorter-of issue/receipt (i.e., choose the later date -> smaller diff to payment)
  // - Else fallback to notice_for_payment_issue_date
  // - Else fallback to supply_date
  const issue = parseDateFlexible(data?.invoice_issue_date);
  const receipt = parseDateFlexible(data?.invoice_receipt_date);

  if (issue && receipt) {
    const chosen = issue.getTime() >= receipt.getTime() ? issue : receipt;
    return {
      date: chosen,
      kind: chosen === issue ? "invoice_issue" : "invoice_receipt",
    };
  }

  if (issue) return { date: issue, kind: "invoice_issue" };
  if (receipt) return { date: receipt, kind: "invoice_receipt" };

  const notice = parseDateFlexible(data?.notice_for_payment_issue_date);
  if (notice) return { date: notice, kind: "notice" };

  const supply = parseDateFlexible(data?.supply_date);
  if (supply) return { date: supply, kind: "supply" };

  return { date: null, kind: null };
}

function ensureCanonicalRowShape(row) {
  const out = { ...(row || {}) };

  // Ensure all canonical keys exist (null if missing)
  for (const k of CANONICAL_FIELDS) {
    if (!Object.prototype.hasOwnProperty.call(out, k)) out[k] = null;
  }

  // Ensure invoice_reference_number exists by the end of pipeline. If missing, generate deterministic surrogate.
  if (!out.invoice_reference_number) {
    const parts = [
      out.payee_entity_abn || "",
      out.payer_entity_abn || "",
      out.payment_date || "",
      out.payment_amount != null ? String(out.payment_amount) : "",
      out.supply_date || "",
      out.notice_for_payment_issue_date || "",
      out.invoice_issue_date || "",
      out.invoice_receipt_date || "",
      out.row_no != null ? String(out.row_no) : "",
    ].map((x) => String(x).trim());

    // Lightweight deterministic hash without importing crypto here
    const base = parts.join("|");
    let h = 0;
    for (let i = 0; i < base.length; i += 1) {
      h = (h * 31 + base.charCodeAt(i)) >>> 0;
    }
    out.invoice_reference_number = `sys:${h.toString(16)}`;
  }

  // MVP assumption: all records are trade credit agreements unless explicitly mapped otherwise.
  // This makes the trade credit population deterministic and unblocks metrics without field guessing.
  if (out.trade_credit_payment !== true && out.trade_credit_payment !== false) {
    out.trade_credit_payment = true;
  }

  // Unless explicitly flagged, rows are not excluded from trade credit totals.
  if (
    out.excluded_trade_credit_payment !== true &&
    out.excluded_trade_credit_payment !== false
  ) {
    out.excluded_trade_credit_payment = false;
  }

  // Surface raw/effective payment term code into the canonical `payment_term` for UI + downstream.
  // Deterministic projection only (no synonyms / guessing across fields).
  // Priority:
  //  1) existing canonical payment_term
  //  2) effective term resolved earlier in the pipeline (invoice_payment_terms_effective)
  //  3) mapped/raw invoice terms (invoice_payment_terms)
  if (out.payment_term == null || String(out.payment_term).trim() === "") {
    const eff = out.invoice_payment_terms_effective;
    const inv = out.invoice_payment_terms;
    if (eff != null && String(eff).trim() !== "")
      out.payment_term = String(eff).trim();
    else if (inv != null && String(inv).trim() !== "")
      out.payment_term = String(inv).trim();
  }

  // Normalise canonical amount fields while preserving original sign.
  // Metrics uses Math.abs() where needed, but rules may rely on the sign (e.g. discounts).
  if (out.payment_amount != null && out.payment_amount !== "") {
    const norm = normalizeAmountLike(out.payment_amount);
    out.payment_amount = norm == null ? null : norm;
  }

  // Optional: keep invoice_amount consistent when present.
  if (out.invoice_amount != null && out.invoice_amount !== "") {
    const norm = normalizeAmountLike(out.invoice_amount);
    out.invoice_amount = norm == null ? null : norm;
  }

  // Normalise payment_term_days when provided (e.g. "0010" -> 10).
  if (out.payment_term_days != null && out.payment_term_days !== "") {
    const n = Number(String(out.payment_term_days).trim());
    out.payment_term_days = Number.isFinite(n) ? Math.round(n) : null;
  }

  // Derive payment_time_reference_* and payment_time_days
  const ref = computePaymentTimeReference(out);
  if (ref.date) {
    out.payment_time_reference_date =
      out.payment_time_reference_date || toIsoDateOnlyUtc(ref.date);
    out.payment_time_reference_kind =
      out.payment_time_reference_kind || ref.kind;

    const paymentDate = parseDateFlexible(out.payment_date);
    const days = diffDaysClamped(ref.date, paymentDate);
    if (days != null)
      out.payment_time_days =
        out.payment_time_days != null ? out.payment_time_days : days;
  }

  // Normalise date fields to ISO yyyy-mm-dd where possible (keeps UI & metrics consistent)
  const dateKeys = [
    "supply_date",
    "payment_date",
    "notice_for_payment_issue_date",
    "invoice_issue_date",
    "invoice_receipt_date",
    "invoice_due_date",
    "payment_time_reference_date",
  ];

  for (const k of dateKeys) {
    const d = parseDateFlexible(out[k]);
    if (d) out[k] = toIsoDateOnlyUtc(d);
  }

  return out;
}

// Postgres JSONB will reject strings containing NUL (\u0000) bytes.
// Also, JSON cannot represent `undefined` values.
function sanitizeForJsonbDeep(value) {
  if (value === undefined) return null;
  if (value === null) return null;

  if (typeof value === "string") {
    // Remove any NUL bytes which Postgres rejects.
    return value.includes("\u0000") ? value.replace(/\u0000/g, "") : value;
  }

  if (Array.isArray(value)) {
    return value.map((v) => sanitizeForJsonbDeep(v));
  }

  if (typeof value === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = sanitizeForJsonbDeep(v);
    }
    return out;
  }

  return value;
}

async function loadMappedRowsForPtrs({
  customerId,
  ptrsId,
  limit = 50,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const findOpts = {
    where: { customerId, ptrsId },
    order: [["rowNo", "ASC"]],
    attributes: ["rowNo", "data"],
    raw: true,
    transaction,
  };

  const numericLimit = Number(limit);
  if (Number.isFinite(numericLimit) && numericLimit > 0) {
    findOpts.limit = numericLimit;
  }

  const rows = await db.PtrsMappedRow.findAll(findOpts);

  if (logger && logger.info) {
    slog.info(
      "PTRS v2 loadMappedRowsForPtrs: loaded mapped rows",
      safeMeta({
        customerId,
        ptrsId,
        requestedLimit: limit,
        rowsCount: Array.isArray(rows) ? rows.length : 0,
      }),
    );
  }

  const composed = rows.map((r) => {
    let base = r.data || {};
    // If data was accidentally stored as a JSON string, try to parse it defensively
    if (typeof base === "string") {
      try {
        const parsed = JSON.parse(base);
        if (parsed && typeof parsed === "object") {
          base = parsed;
        }
      } catch (_) {
        // leave base as-is if parsing fails
      }
    }
    // ensure row_no is present for downstream logic
    const withRowNo = { ...base, row_no: r.rowNo };
    return ensureCanonicalRowShape(withRowNo);
  });

  // Simple header inference from the mapped rows
  const headers = Array.from(
    new Set(composed.flatMap((row) => Object.keys(row))),
  );

  if (logger && logger.debug && composed.length) {
    slog.debug(
      "PTRS v2 loadMappedRowsForPtrs: sample composed row",
      safeMeta({
        customerId,
        ptrsId,
        sampleRowKeys: Object.keys(composed[0] || {}),
        headersCount: headers.length,
      }),
    );
  }

  return { rows: composed, headers };
}

/** Controller-friendly wrapper: getMap (normalises JSON-ish fields) */
async function getMap({ customerId, ptrsId }) {
  const map = await getColumnMap({ customerId, ptrsId });
  if (!map) return null;
  const maybeParse = (v) => {
    if (v == null || typeof v !== "string") return v;
    try {
      return JSON.parse(v);
    } catch {
      return v;
    }
  };
  map.extras = maybeParse(map.extras);
  map.fallbacks = maybeParse(map.fallbacks);
  map.defaults = maybeParse(map.defaults);
  map.rowRules = maybeParse(map.rowRules);
  return map;
}

/** Get column map for a ptrs */
async function getColumnMap({ customerId, ptrsId, transaction = null }) {
  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const isExternalTx = !!transaction;
  try {
    const map = await db.PtrsColumnMap.findOne({
      where: { customerId, ptrsId },
      transaction: t,
      raw: true,
    });
    slog.info(
      "PTRS v2 getColumnMap: loaded map",
      safeMeta({
        customerId,
        ptrsId,
        hasMap: !!map,
        id: map?.id || null,
        hasMappings: !!(map && map.mappings),
        hasJoins: !!(map && map.joins),
        hasRowRules: !!(map && map.rowRules),
        mappingsKeys: map?.mappings ? Object.keys(map.mappings) : [],
        // 🔍 new bits
        hasCustomFields: !!(map && map.customFields),
        customFieldsType:
          map && map.customFields ? typeof map.customFields : null,
        hasJoinsField: !!(map && map.joins),
        joinsType: map && map.joins ? typeof map.joins : null,
      }),
    );
    if (!isExternalTx && !t.finished) {
      await t.commit();
    }
    return map || null;
  } catch (err) {
    if (!isExternalTx && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

/**
 * List PTRS runs that have a saved column map, including mapMeta extracted from map.extras.
 * Used by the FE to show compatible maps without N+1 getMap calls.
 */
async function listCompatibleMaps({ customerId }) {
  if (!customerId) throw new Error("customerId is required");

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const maps = await db.PtrsColumnMap.findAll({
      where: { customerId },
      attributes: ["ptrsId", "extras"],
      raw: true,
      transaction: t,
    });

    const ptrsIds = maps.map((m) => m.ptrsId).filter(Boolean);
    if (!ptrsIds.length) {
      await t.commit();
      return [];
    }

    const metaByPtrsId = new Map();
    for (const m of maps) {
      metaByPtrsId.set(m.ptrsId, extractMapMetaFromExtras(m.extras));
    }

    const ptrsRows = await db.Ptrs.findAll({
      where: { customerId, id: { [Op.in]: ptrsIds } },
      order: [
        ["updatedAt", "DESC"],
        ["createdAt", "DESC"],
      ],
      raw: true,
      transaction: t,
    });

    const items = (ptrsRows || []).map((r) => ({
      ...r,
      mapMeta: metaByPtrsId.get(r.id) || null,
    }));

    await t.commit();
    return items;
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

/**
 * Return profile-scoped canonical field mappings for a ptrs run.
 */
async function getFieldMap({
  customerId,
  ptrsId,
  profileId,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!profileId) throw new Error("profileId is required");

  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const isExternalTx = !!transaction;

  try {
    const rows = await db.PtrsFieldMap.findAll({
      where: { customerId, ptrsId, profileId },
      order: [["canonicalField", "ASC"]],
      raw: true,
      transaction: t,
    });

    if (!isExternalTx && !t.finished) {
      await t.commit();
    }

    return rows || [];
  } catch (err) {
    if (!isExternalTx && !t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

/**
 * Replace all profile-scoped canonical field mappings for a ptrs run.
 * We do a simple "replace" (delete then bulk insert) to avoid half-updated sets.
 */
async function saveFieldMap({
  customerId,
  ptrsId,
  profileId,
  fieldMap,
  userId,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!Array.isArray(fieldMap)) throw new Error("fieldMap array is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    await db.PtrsFieldMap.destroy({
      where: { customerId, ptrsId, profileId },
      transaction: t,
    });

    const actor = userId || null;

    const payload = fieldMap
      .filter((r) => r && typeof r === "object")
      .map((r) => ({
        customerId,
        ptrsId,
        profileId,
        canonicalField: r.canonicalField,
        sourceRole: r.sourceRole,
        sourceColumn: r.sourceColumn ?? null,
        transformType: r.transformType ?? null,
        transformConfig: r.transformConfig ?? null,
        meta: r.meta ?? null,
        createdBy: actor,
        updatedBy: actor,
      }))
      .filter((r) => r.canonicalField && r.sourceRole);

    if (payload.length) {
      await db.PtrsFieldMap.bulkCreate(payload, {
        transaction: t,
        validate: true,
      });
    }

    const rows = await db.PtrsFieldMap.findAll({
      where: { customerId, ptrsId, profileId },
      order: [["canonicalField", "ASC"]],
      raw: true,
      transaction: t,
    });

    await t.commit();
    return rows || [];
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function getImportSample({
  customerId,
  ptrsId,
  datasetId = null,
  limit = 10,
  offset = 0,
}) {
  if (logger && logger.info) {
    slog.info("PTRS v2 getImportSample: begin", {
      action: "PtrsV2GetImportSample",
      customerId,
      ptrsId,
      limit,
      offset,
    });
  }

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    // rows
    const rows = await db.PtrsImportRaw.findAll({
      where: datasetId
        ? { customerId, ptrsId, datasetId }
        : { customerId, ptrsId },
      order: [["rowNo", "ASC"]],
      limit,
      offset,
      attributes: ["rowNo", "data"],
      raw: true,
      transaction: t,
    });

    // total
    const total = await db.PtrsImportRaw.count({
      where: datasetId
        ? { customerId, ptrsId, datasetId }
        : { customerId, ptrsId },
      transaction: t,
    });

    if (logger && logger.debug) {
      slog.debug("PTRS v2 getImportSample: raw import snapshot", {
        action: "PtrsV2GetImportSample",
        customerId,
        ptrsId,
        fetchedRows: Array.isArray(rows) ? rows.length : 0,
        total,
      });
    }

    // headers: scan up to 500 earliest rows to reduce noise
    const headerScan = await db.PtrsImportRaw.findAll({
      where: datasetId
        ? { customerId, ptrsId, datasetId }
        : { customerId, ptrsId },
      order: [["rowNo", "ASC"]],
      limit: 500,
      attributes: ["data"],
      raw: true,
      transaction: t,
    });
    const headerSet = new Set();
    for (const r of headerScan) {
      const d = r.data || {};
      for (const k of Object.keys(d)) headerSet.add(k);
      if (headerSet.size > 2000) break; // sanity cap
    }
    const headers = Array.from(headerSet.values())
      .filter((h) => h != null && String(h).trim() !== "")
      .map((h) => String(h));

    if (logger && logger.debug) {
      slog.debug("PTRS v2 getImportSample: inferred main headers", {
        action: "PtrsV2GetImportSample",
        customerId,
        ptrsId,
        headersCount: Array.isArray(headers) ? headers.length : 0,
        sampleHeader:
          Array.isArray(headers) && headers.length ? headers[0] : null,
      });
    }

    // Build examples for main dataset from the scanned rows
    const exampleByHeaderMain = {};
    for (const r of headerScan) {
      const d = r.data || {};
      for (const k of Object.keys(d)) {
        if (exampleByHeaderMain[k] == null) {
          const v = d[k];
          if (v != null && String(v).trim() !== "") {
            exampleByHeaderMain[k] = v;
          }
        }
      }
    }

    // Accumulate per-header metadata: sources and examples
    const headerMeta = {};
    for (const h of headers) {
      headerMeta[h] = headerMeta[h] || { sources: new Set(), examples: {} };
      headerMeta[h].sources.add("main");
      if (exampleByHeaderMain[h] != null)
        headerMeta[h].examples.main = exampleByHeaderMain[h];
    }

    // --- Merge supporting dataset headers and collect examples ---
    try {
      const dsRows = await db.PtrsDataset.findAll({
        where: { customerId, ptrsId },
        attributes: ["id", "meta", "role"],
        raw: true,
        transaction: t,
      });
      if (logger && logger.info) {
        slog.info("PTRS v2 getImportSample: supporting datasets found", {
          action: "PtrsV2GetImportSampleMergeHeaders",
          customerId,
          ptrsId,
          datasetCount: Array.isArray(dsRows) ? dsRows.length : 0,
        });
      }
      if (Array.isArray(dsRows) && dsRows.length) {
        const addHeaders = (arr, role) => {
          for (const h of arr || []) {
            if (h == null) continue;
            const s = String(h).trim();
            if (!s) continue;
            headerSet.add(s);
            headerMeta[s] = headerMeta[s] || {
              sources: new Set(),
              examples: {},
            };
            if (role) headerMeta[s].sources.add(role);
          }
        };

        for (const ds of dsRows) {
          const role = ds.role || "dataset";
          // Prefer meta.headers
          const meta = ds.meta || {};
          let dsHeaders = Array.isArray(meta.headers) ? meta.headers : null;
          let sampleRows = null;
          try {
            // Always try to fetch a tiny sample to capture example values
            const sample = await getDatasetSample({
              customerId,
              datasetId: ds.id,
              limit: 5,
              offset: 0,
            });
            dsHeaders =
              dsHeaders && dsHeaders.length ? dsHeaders : sample.headers;
            sampleRows = Array.isArray(sample.rows) ? sample.rows : [];
          } catch (_) {
            // ignore
          }
          addHeaders(dsHeaders, role);
          // examples: first non-empty per header from this dataset
          if (sampleRows && sampleRows.length) {
            for (const row of sampleRows) {
              for (const [k, v] of Object.entries(row)) {
                if (v != null && String(v).trim() !== "") {
                  headerMeta[k] = headerMeta[k] || {
                    sources: new Set(),
                    examples: {},
                  };
                  if (headerMeta[k].examples[role] == null) {
                    headerMeta[k].examples[role] = v;
                  }
                }
              }
            }
          }
        }
        if (logger && logger.info) {
          slog.info("PTRS v2 getImportSample: merged supporting headers", {
            action: "PtrsV2GetImportSampleMergeHeaders",
            customerId,
            ptrsId,
            unifiedHeaderCount: headerSet ? headerSet.size : 0,
          });
        }
      }
    } catch (e) {
      if (logger && logger.warn) {
        slog.warn(
          "PTRS v2 getImportSample: failed merging supporting dataset headers",
          {
            action: "PtrsV2GetSampleMergeHeaders",
            customerId,
            ptrsId,
            error: e.message,
          },
        );
      }
    }

    // ---   // --- Finalise unified headers and headerMeta into plain structures ---
    const unifiedHeaders = Array.from(headerSet.values());
    const finalizedHeaderMeta = {};
    for (const key of unifiedHeaders) {
      const meta = headerMeta[key] || { sources: new Set(), examples: {} };
      const sources = Array.from(meta.sources || []);
      let example = null;
      if (meta.examples) {
        if (meta.examples.main != null) example = meta.examples.main;
        else {
          const firstRole = Object.keys(meta.examples)[0];
          if (firstRole) example = meta.examples[firstRole];
        }
      }
      finalizedHeaderMeta[key] = {
        sources,
        examples: meta.examples || {},
        example,
      };
    }

    if (logger && logger.info) {
      slog.info("PTRS v2 getImportSample: done", {
        action: "PtrsV2GetImportSample",
        customerId,
        ptrsId,
        rowsReturned: Array.isArray(rows) ? rows.length : 0,
        total,
        unifiedHeadersCount: Array.isArray(unifiedHeaders)
          ? unifiedHeaders.length
          : 0,
        headerMetaKeys: finalizedHeaderMeta
          ? Object.keys(finalizedHeaderMeta).length
          : 0,
        exampleForFirstHeader:
          Array.isArray(unifiedHeaders) && unifiedHeaders.length
            ? (finalizedHeaderMeta[unifiedHeaders[0]]?.example ?? null)
            : null,
      });
    }

    await t.commit();

    return {
      rows,
      total,
      headers: unifiedHeaders,
      headerMeta: finalizedHeaderMeta,
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

/** Upsert column map for a ptrs — now RLS-safe */
async function saveColumnMap({
  customerId,
  ptrsId,
  mappings,
  extras = null,
  fallbacks = null,
  defaults = null,
  // IMPORTANT: no default. `undefined` means "no change"; `null` means "clear".
  joins,
  rowRules = null,
  profileId = null,
  // IMPORTANT: no default. `undefined` means "no change"; `null` means "clear".
  customFields,
  userId,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  // 🔐 RLS-safe tenant-scoped transaction
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const existing = await db.PtrsColumnMap.findOne({
      where: { customerId, ptrsId },
      transaction: t,
    });

    const incomingSignature = buildMaterialMapSignature({
      mappings,
      joins,
      customFields,
    });

    const existingExtrasObj = safeParseJsonObject(existing?.extras) || {};
    const existingMeta = existing
      ? extractMapMetaFromExtras(existingExtrasObj)
      : null;
    const existingSignature = existingMeta?.signature || null;

    if (
      existing &&
      existingSignature &&
      existingSignature === incomingSignature
    ) {
      slog.info(
        "PTRS v2 saveColumnMap: no material change detected; skipping update",
        {
          action: "PtrsV2SaveColumnMapNoop",
          customerId,
          ptrsId,
          signature: incomingSignature,
        },
      );

      const plain = existing.get ? existing.get({ plain: true }) : existing;
      await t.commit();
      return plain;
    }

    const resolveField = (incoming, existingValue) =>
      typeof incoming === "undefined" ? existingValue : incoming;

    // Joins can be provided in either legacy array form or the new object form
    // (e.g. { conditions: [...] }). Treat `undefined` as "no change".
    const nextJoins = resolveField(joins, existing ? existing.joins : null);

    const payload = {
      mappings: resolveField(mappings, existing?.mappings || null),
      extras: resolveField(extras, existing?.extras || null),
      fallbacks: resolveField(fallbacks, existing?.fallbacks || null),
      defaults: resolveField(defaults, existing?.defaults || null),
      joins: nextJoins,
      rowRules: resolveField(rowRules, existing?.rowRules || null),
      profileId: resolveField(profileId, existing?.profileId || null),
      customFields: resolveField(customFields, existing?.customFields || null),
    };

    // --- Persist compatibility metadata on every save (authoritative server-side) ---
    // Store in extras.mapMeta so the UI can list compatible maps without N+1 getMap calls.
    const incomingExtrasObj = safeParseJsonObject(payload.extras) || {};

    const nowIso = new Date().toISOString();

    const nextExtras = {
      ...existingExtrasObj,
      ...incomingExtrasObj,
    };

    nextExtras.mapMeta = buildMapMetaFromMappings(
      payload.mappings,
      incomingSignature,
      nowIso,
    );

    payload.extras = nextExtras;

    slog.info(
      "PTRS v2 saveColumnMap: upserting map",
      safeMeta({
        customerId,
        ptrsId,
        hasMappings: !!payload.mappings,
        hasJoins: !!payload.joins,
        hasCustomFields: !!payload.customFields,
        mappingsType: payload.mappings ? typeof payload.mappings : null,
        joinsType: payload.joins ? typeof payload.joins : null,
        customFieldsType: payload.customFields
          ? typeof payload.customFields
          : null,
      }),
    );

    if (existing) {
      await existing.update(
        {
          ...payload,
          updatedBy: userId || existing.updatedBy || existing.createdBy || null,
        },
        { transaction: t },
      );

      await t.commit();
      return existing.get({ plain: true });
    }

    const row = await db.PtrsColumnMap.create(
      {
        customerId,
        ptrsId,
        ...payload,
        createdBy: userId || null,
        updatedBy: userId || null,
      },
      { transaction: t },
    );

    await t.commit();
    return row.get({ plain: true });
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

// Build and persist the mapped + joined dataset for a ptrs run into PtrsMappedRow
async function buildMappedDatasetForPtrs({
  customerId,
  ptrsId,
  actorId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const BATCH_SIZE = 2000;
  const MAX_HEADER_KEYS = 2000;

  const t = await beginTransactionWithCustomerContext(customerId);

  slog.info("PTRS_TRACE maps", {
    PTRS_TRACE: process.env.PTRS_TRACE,
    PTRS_TRACE_DIR: process.env.PTRS_TRACE_DIR,
  });

  const trace = createPtrsTrace({
    customerId,
    ptrsId,
    actorId,
    logInfo: (msg, meta) => slog.info(msg, meta),
    meta: safeMeta,
  });
  const jobStartNs = process.hrtime.bigint();
  trace?.write("build_begin", { batchSize: BATCH_SIZE });

  try {
    slog.info(
      "PTRS v2 buildMappedDatasetForPtrs: begin",
      safeMeta({
        customerId,
        ptrsId,
      }),
    );

    // Clear any existing mapped rows for this ptrs run so we keep exactly one snapshot.
    // IMPORTANT: PtrsMappedRow is paranoid/soft-delete in some environments; a soft delete will NOT
    // remove the (customerId, ptrsId, rowNo) unique constraint conflicts. Force a hard delete.
    const destroyStartNs = process.hrtime.bigint();
    trace?.write("mapped_rows_destroy_begin");
    await db.PtrsMappedRow.destroy({
      where: { customerId, ptrsId },
      force: true,
      transaction: t,
    });
    trace?.write("mapped_rows_destroy_end", {
      durationMs: hrMsSince(destroyStartNs),
    });

    const existingCount = await db.PtrsMappedRow.count({
      where: { customerId, ptrsId },
      transaction: t,
    });
    trace?.write("mapped_rows_post_destroy_count", { existingCount });

    if (existingCount) {
      slog.warn(
        "PTRS v2 buildMappedDatasetForPtrs: mapped rows still exist after destroy (possible unexpected constraint/paranoid behaviour)",
        safeMeta({ customerId, ptrsId, existingCount }),
      );
    }

    let offset = 0;
    let totalPersisted = 0;
    let canonicalHeaders = [];
    const headersSet = new Set();
    let isFirstBatch = true;
    const nowIso = new Date().toISOString();

    while (true) {
      const batchStartNs = process.hrtime.bigint();
      trace?.write("batch_begin", { offset, limit: BATCH_SIZE });
      // Compose mapped rows for this batch
      const composeStartNs = process.hrtime.bigint();
      const { rows } = await composeMappedRowsForPtrs({
        customerId,
        ptrsId,
        limit: BATCH_SIZE,
        offset,
        transaction: t,
        trace,
      });
      trace?.write("batch_compose_end", {
        offset,
        durationMs: hrMsSince(composeStartNs),
        rowsComposed: Array.isArray(rows) ? rows.length : 0,
      });

      // If we got fewer than BATCH_SIZE rows, we know there cannot be another page.
      // This avoids a useless second compose call that returns 0 rows.
      const isLastBatch =
        Array.isArray(rows) && rows.length > 0 && rows.length < BATCH_SIZE;

      if (!rows || !rows.length) {
        if (isFirstBatch) {
          // No rows at all
          trace?.write("build_no_rows", { totalMs: hrMsSince(jobStartNs) });
          slog.info(
            "PTRS v2 buildMappedDatasetForPtrs: no rows composed, nothing persisted",
            safeMeta({ customerId, ptrsId }),
          );
          await t.commit();
          if (trace) await trace.close();
          return { count: 0, headers: [] };
        }
        break;
      }

      // Build payload for this batch
      const payload = [];
      for (let i = 0; i < rows.length; ++i) {
        const row = rows[i];
        const canonicalRow = sanitizeForJsonbDeep(ensureCanonicalRowShape(row));
        if (isFirstBatch) {
          // Collect header keys from first batch only
          for (const k of Object.keys(canonicalRow)) {
            if (headersSet.size < MAX_HEADER_KEYS) headersSet.add(k);
          }
        }
        payload.push({
          customerId,
          ptrsId,
          rowNo:
            typeof row.row_no === "number" && Number.isFinite(row.row_no)
              ? row.row_no
              : offset + i + 1,
          data: canonicalRow,
          meta: sanitizeForJsonbDeep({
            stage: "ptrs.v2.mapped",
            builtAt: nowIso,
            builtBy: actorId || null,
          }),
        });
      }

      try {
        const persistStartNs = process.hrtime.bigint();
        trace?.write("batch_persist_begin", {
          offset,
          batchSize: payload.length,
        });
        await db.PtrsMappedRow.bulkCreate(payload, {
          transaction: t,
          validate: false,
        });
        trace?.write("batch_persist_end", {
          offset,
          batchSize: payload.length,
          durationMs: hrMsSince(persistStartNs),
        });
      } catch (e) {
        // Surface the underlying Postgres error detail so we can fix the real cause.
        slog.error(
          "PTRS v2 buildMappedDatasetForPtrs: bulkCreate failed",
          safeMeta({
            customerId,
            ptrsId,
            offset,
            batchSize: payload.length,
            message: e?.message || null,
            name: e?.name || null,
            pgMessage: e?.parent?.message || e?.original?.message || null,
            pgDetail: e?.parent?.detail || null,
            pgCode: e?.parent?.code || null,
            errors: Array.isArray(e?.errors)
              ? e.errors.map((x) => ({ message: x?.message, path: x?.path }))
              : null,
          }),
        );
        throw e;
      }
      totalPersisted += payload.length;
      if (isFirstBatch) {
        canonicalHeaders = Array.from(headersSet);
        isFirstBatch = false;
      }
      trace?.write("batch_end", {
        offset,
        persistedSoFar: totalPersisted,
        durationMs: hrMsSince(batchStartNs),
      });
      offset += BATCH_SIZE;

      // Stop after the last partial page to avoid an empty follow-up batch.
      if (isLastBatch) break;
    }

    slog.info(
      "PTRS v2 buildMappedDatasetForPtrs: persisted mapped rows",
      safeMeta({
        customerId,
        ptrsId,
        rowsPersisted: totalPersisted,
        headersCount: Array.isArray(canonicalHeaders)
          ? canonicalHeaders.length
          : 0,
      }),
    );

    trace?.write("build_before_commit", {
      rowsPersisted: totalPersisted,
      headersCount: Array.isArray(canonicalHeaders)
        ? canonicalHeaders.length
        : 0,
      totalMs: hrMsSince(jobStartNs),
    });

    await t.commit();

    trace?.write("build_committed", { totalMs: hrMsSince(jobStartNs) });
    if (trace) await trace.close();

    return {
      count: totalPersisted,
      headers: canonicalHeaders || [],
    };
  } catch (err) {
    trace?.write("build_error", {
      message: err?.message || null,
      statusCode: err?.statusCode || null,
      totalMs: hrMsSince(jobStartNs),
    });
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    if (trace) await trace.close();
    throw err;
  }
}

// Compose mapped rows for a ptrs, including join and column mapping logic
async function composeMappedRowsForPtrs({
  customerId,
  ptrsId,
  limit = 50,
  offset = 0,
  transaction = null,
  trace = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const composeStartNs = process.hrtime.bigint();

  const stageStart = (name) => ({
    name,
    startNs: process.hrtime.bigint(),
  });

  const stageEnd = (s, extra = {}) => {
    if (!s) return;
    trace?.write("compose_stage_end", {
      stage: s.name,
      durationMs: hrMsSince(s.startNs),
      ...extra,
    });
  };

  trace?.write("compose_begin", { limit, offset });

  // Load column map (with joins + rowRules etc.)
  const sLoadMap = stageStart("load_column_map");
  const mapRow = await getColumnMap({ customerId, ptrsId, transaction });
  stageEnd(sLoadMap, {
    hasMap: !!mapRow,
    hasMappings: !!(mapRow && mapRow.mappings),
    hasJoins: !!(mapRow && mapRow.joins),
    hasCustomFields: !!(mapRow && mapRow.customFields),
  });
  const map = mapRow || {};
  const mappings = map.mappings || {};

  // Canonical field map is profile-scoped. We use the profileId saved on the column map.
  const profileId = map.profileId || null;
  const sFieldMap = stageStart("load_field_map");
  let fieldMapRows = [];
  try {
    if (profileId) {
      fieldMapRows = await getFieldMap({
        customerId,
        ptrsId,
        profileId,
        transaction,
      });
    }
  } catch (e) {
    // Non-fatal for MVP: fall back to non-canonical output
    slog.warn(
      "PTRS v2 composeMappedRowsForPtrs: failed to load field map",
      safeMeta({ customerId, ptrsId, profileId, error: e.message }),
    );
    fieldMapRows = [];
  }
  stageEnd(sFieldMap, {
    profileId,
    fieldMapCount: Array.isArray(fieldMapRows) ? fieldMapRows.length : 0,
  });

  if (logger && logger.info) {
    slog.info(
      "PTRS v2 composeMappedRowsForPtrs: field map loaded",
      safeMeta({
        customerId,
        ptrsId,
        profileId,
        fieldMapCount: Array.isArray(fieldMapRows) ? fieldMapRows.length : 0,
      }),
    );
  }
  if (logger && logger.debug) {
    slog.debug(
      "PTRS v2 composeMappedRowsForPtrs: raw joins",
      safeMeta({
        customerId,
        ptrsId,
        hasJoins: !!map.joins,
        joinsType: map.joins ? typeof map.joins : null,
      }),
    );
  }

  // Normalise joins – support both legacy (array) and new object with conditions array
  let joins = map.joins;
  if (typeof joins === "string") {
    try {
      joins = JSON.parse(joins);
    } catch {
      joins = null;
    }
  }

  // Derive joinsArray for uniform handling
  let joinsArray = [];
  if (Array.isArray(joins)) {
    joinsArray = joins;
  } else if (joins && Array.isArray(joins.conditions)) {
    joinsArray = joins.conditions;
  } else {
    joinsArray = [];
  }

  const normalisedJoins = [];
  for (const j of joinsArray) {
    if (!j || typeof j !== "object") continue;

    const from = j.from || {};
    const to = j.to || {};

    const fromRole = String(from.role || "").toLowerCase();
    const toRole = String(to.role || "").toLowerCase();

    const fromCol = from.column;
    const toCol = to.column;

    if (!fromRole || !toRole || !fromCol || !toCol) continue;

    normalisedJoins.push({
      fromRole,
      fromColumn: fromCol,
      fromTransform: from.transform || null,
      toRole,
      toColumn: toCol,
      toTransform: to.transform || null,
    });
  }
  trace?.write("compose_joins_normalised", {
    joinsRawType: joins == null ? null : typeof joins,
    joinsCount: normalisedJoins.length,
  });

  // Defensive log for debugging joins, if logger.info is available
  if (logger && logger.info) {
    slog.info(
      "PTRS v2 composeMappedRowsForPtrs: normalised joins",
      safeMeta({ customerId, ptrsId, joinsCount: normalisedJoins.length }),
    );
  }

  // --- Normalise customFields (like joins)
  let customFields = map.customFields;
  if (typeof customFields === "string") {
    try {
      customFields = JSON.parse(customFields);
    } catch {
      customFields = null;
    }
  }
  if (!Array.isArray(customFields)) {
    customFields = [];
  }
  trace?.write("compose_custom_fields_normalised", {
    customFieldsRawType:
      map.customFields == null ? null : typeof map.customFields,
    customFieldsCount: Array.isArray(customFields) ? customFields.length : 0,
  });

  if (logger && logger.info) {
    slog.info(
      "PTRS v2 composeMappedRowsForPtrs: custom fields normalised",
      safeMeta({
        customerId,
        ptrsId,
        customFieldsCount: Array.isArray(customFields)
          ? customFields.length
          : 0,
        customFieldsType: customFields ? typeof customFields : null,
      }),
    );
  }

  const orderJoinsForExecution = (joins) => {
    const list = Array.isArray(joins) ? joins.slice() : [];
    if (list.length <= 1) return list;

    const norm = (r) =>
      String(r || "")
        .trim()
        .toLowerCase();

    const available = new Set(["main"]);
    let remaining = list.slice();
    const ordered = [];

    let guard = 0;
    while (remaining.length) {
      guard += 1;
      if (guard > list.length + 5) break;

      const passPicked = [];
      const passLeft = [];

      for (const j of remaining) {
        const fromRole = norm(j.fromRole);
        const toRole = norm(j.toRole);

        // malformed joins: keep stable, they’ll get ignored later anyway
        if (!fromRole || !toRole) {
          passPicked.push(j);
          continue;
        }

        if (fromRole === "main" || available.has(fromRole)) {
          passPicked.push(j);
        } else {
          passLeft.push(j);
        }
      }

      if (!passPicked.length) {
        const rolesKnown = Array.from(available);
        const missing = Array.from(
          new Set(
            passLeft
              .map((j) => norm(j.fromRole))
              .filter((r) => r && r !== "main" && !available.has(r)),
          ),
        );

        const e = new Error(
          `Invalid join dependency chain: cannot resolve join order. ` +
            `Roles available: ${rolesKnown.join(", ") || "(none)"}. ` +
            `Missing/blocked fromRoles: ${missing.join(", ") || "(unknown)"}.`,
        );
        e.statusCode = 400;
        throw e;
      }

      for (const j of passPicked) {
        ordered.push(j);
        const toRole = norm(j.toRole);
        if (toRole) available.add(toRole);
      }

      remaining = passLeft;
    }

    return ordered;
  };

  // --- Postgres join SQL debug helpers (for logging only, not execution) ---
  const _sqlIdent = (name) => `"${String(name).replace(/"/g, '""')}"`;

  const _sqlLiteral = (v) => {
    if (v == null) return "NULL";
    const s = String(v);
    return `'${s.replace(/'/g, "''")}'`;
  };

  const _getTableName = (model) => {
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
  };

  const _normSql = (exprSql) => `lower(btrim(coalesce(${exprSql}, '')))`;

  const _isMainRoleSql = (role) => {
    const r = String(role || "")
      .trim()
      .toLowerCase();
    return r === "main" || r.startsWith("main_");
  };

  const buildJoinDebugSql = () => {
    const importTable = _getTableName(db.PtrsImportRaw) || "PtrsImportRaw";
    const datasetTable = _getTableName(db.PtrsDataset) || "PtrsDataset";

    // Use only joins that target a supporting role, and group by toRole (one LATERAL join per supporting role).
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

    // Build deterministic matching: first match wins (ORDER BY rowNo ASC LIMIT 1)
    for (const [toRole, joinsForRole] of byToRole.entries()) {
      const alias = `j_${toRole.replace(/[^a-z0-9_]/g, "_")}`; // safe-ish alias

      // Build OR-of-ANDs: any join condition can match (consistent with "first wins" semantics in JS index).
      // Each condition compares normalised join keys.
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
          : `${mAlias}.${_sqlIdent("data")}->>${_sqlLiteral(`${fromRole}__${fromCol}`)}`;

        const rhsExpr = `${alias}.${_sqlIdent("data")}->>${_sqlLiteral(toCol)}`;

        condSqlParts.push(`${_normSql(lhsExpr)} = ${_normSql(rhsExpr)}`);
      }

      const whereMatch = condSqlParts.length
        ? `(${condSqlParts.join(" OR ")})`
        : "FALSE";

      // Resolve datasetId by role for this ptrs run.
      const datasetIdSql = `(SELECT d.${_sqlIdent("id")} FROM ${datasetTable.startsWith('"') ? datasetTable : _sqlIdent(datasetTable)} d WHERE d.${_sqlIdent("customerId")} = ${_sqlLiteral(customerId)} AND d.${_sqlIdent("ptrsId")} = ${_sqlLiteral(ptrsId)} AND lower(btrim(coalesce(d.${_sqlIdent("role")}, ''))) = ${_sqlLiteral(toRole)} LIMIT 1)`;

      const lateral = `
LEFT JOIN LATERAL (
  SELECT s.${_sqlIdent("data")}
  FROM ${importTable.startsWith('"') ? importTable : _sqlIdent(importTable)} s
  WHERE s.${_sqlIdent("customerId")} = ${_sqlLiteral(customerId)}
    AND s.${_sqlIdent("datasetId")} = ${datasetIdSql}
    AND ${whereMatch}
  ORDER BY s.${_sqlIdent("rowNo")} ASC
  LIMIT 1
) ${alias} ON TRUE`.trim();

      joinClauses.push(lateral);

      // Prefix keys from joined JSON into the merged JSON result, e.g. vendormaster__Tax number
      const prefixedJson = `
COALESCE((
  SELECT jsonb_object_agg(${_sqlLiteral(`${toRole}__`)} || e.key, e.value)
  FROM jsonb_each(${alias}.${_sqlIdent("data")}) e
), '{}'::jsonb)`.trim();

      selectJson = `(${selectJson} || ${prefixedJson})`;
    }

    const sql = `
/* PTRS v2 debug join SQL (generated) */
SELECT
  ${mAlias}.${_sqlIdent("rowNo")} AS row_no,
  ${selectJson} AS joined_data
FROM ${importTable.startsWith('"') ? importTable : _sqlIdent(importTable)} ${mAlias}
${joinClauses.join("\n")}
WHERE ${mAlias}.${_sqlIdent("customerId")} = ${_sqlLiteral(customerId)}
  AND ${mAlias}.${_sqlIdent("ptrsId")} = ${_sqlLiteral(ptrsId)}
ORDER BY ${mAlias}.${_sqlIdent("rowNo")} ASC
LIMIT ${Number.isFinite(Number(limit)) ? Math.max(1, Number(limit)) : 50}
OFFSET ${Number.isFinite(Number(offset)) ? Math.max(0, Number(offset)) : 0};
`.trim();

    return sql;
  };

  // Log generated join SQL early (before join ordering / execution) for crash diagnostics
  try {
    if (logger && logger.info && normalisedJoins.length) {
      const debugSql = buildJoinDebugSql();
      slog.info(
        "PTRS v2 composeMappedRowsForPtrs: generated join SQL (early debug)",
        safeMeta({
          customerId,
          ptrsId,
          joinsCount: normalisedJoins.length,
          sql: debugSql,
        }),
      );
    }
  } catch (e) {
    slog.warn(
      "PTRS v2 composeMappedRowsForPtrs: failed to generate early debug join SQL",
      safeMeta({ customerId, ptrsId, error: e.message }),
    );
  }

  const orderedJoins = orderJoinsForExecution(normalisedJoins);
  trace?.write("compose_joins_ordered", {
    orderedJoinsCount: orderedJoins.length,
  });

  // ---------------- Payment terms (effective-dated) enrichment ----------------
  // We treat payment terms changes as a supporting dataset with an effective-from date.
  // If present, we resolve the payment term code AS-OF the invoice issue date per row.

  const parseDateLoose = (v) => {
    if (v == null) return null;
    if (v instanceof Date && !Number.isNaN(v.getTime())) return v;

    const s = String(v).trim();
    if (!s) return null;

    // ISO-ish
    const iso = new Date(s);
    if (!Number.isNaN(iso.getTime())) return iso;

    // AU format: dd/mm/yyyy
    const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
    if (m) {
      const dd = parseInt(m[1], 10);
      const mm = parseInt(m[2], 10);
      let yyyy = parseInt(m[3], 10);
      if (yyyy < 100) yyyy += 2000;
      const d = new Date(Date.UTC(yyyy, mm - 1, dd));
      if (!Number.isNaN(d.getTime())) return d;
    }

    return null;
  };

  const getPaymentTermsDataset = async () => {
    const roles = ["payment_terms", "payment_terms_changes", "paymentterms"];
    try {
      const ds = await db.PtrsDataset.findOne({
        where: {
          customerId,
          ptrsId,
          role: { [Op.in]: roles },
        },
        attributes: ["id", "role"],
        raw: true,
        transaction,
      });
      return ds || null;
    } catch {
      return null;
    }
  };

  const loadPaymentTermsHistoryIndex = async (datasetId) => {
    // Returns Map<key, [{ from: Date, to: Date|null, term: string|null }]>
    // Key is (company_code|supplier/vendor)

    const history = new Map();

    const RowModel =
      db.PtrsDatasetRow ||
      db.PtrsDatasetData ||
      db.PtrsDatasetDatum ||
      db.PtrsDatasetRecord ||
      null;

    if (!RowModel) {
      throw new Error(
        "Dataset row model not available (expected PtrsDatasetRow/PtrsDatasetData); cannot load payment terms dataset rows",
      );
    }

    // Some schemas use datasetId, others ptrsDatasetId
    const where = { customerId, datasetId };
    if (RowModel.rawAttributes && RowModel.rawAttributes.ptrsDatasetId) {
      delete where.datasetId;
      where.ptrsDatasetId = datasetId;
    }

    const dsRows = await RowModel.findAll({
      where,
      order: [["rowNo", "ASC"]],
      attributes: ["data"],
      raw: true,
      transaction,
    });

    const pickAny = (row, names) => {
      for (const n of names) {
        const v = pickFromRowLoose(row, n);
        if (v !== undefined && v !== null && String(v).trim() !== "") return v;
      }
      return null;
    };

    const supplierNames = [
      "Supplier",
      "supplier",
      "Vendor",
      "vendor",
      "Vendor Account",
      "Vendor Account No",
      "Vendor Account: Name 1",
      "Vendor Account: Number",
    ];

    const companyNames = ["Company Code", "company_code", "Company", "company"];

    const fromNames = [
      "Effective From",
      "effective_from",
      "Valid From",
      "valid_from",
      "From",
      "from",
      "Start Date",
      "start_date",
    ];

    const termNames = [
      "Payment terms",
      "payment_terms",
      "Payment Terms",
      "payment_term",
      "Terms",
      "terms",
      "Term",
      "term",
    ];

    const addEntry = (key, entry) => {
      if (!history.has(key)) history.set(key, []);
      history.get(key).push(entry);
    };

    for (const r of dsRows || []) {
      let d = r?.data || {};
      if (typeof d === "string") {
        try {
          d = JSON.parse(d);
        } catch {
          d = {};
        }
      }

      const supplier = pickAny(d, supplierNames);
      const company = pickAny(d, companyNames);
      const effFromRaw = pickAny(d, fromNames);
      const term = pickAny(d, termNames);

      const fromDate = parseDateLoose(effFromRaw);
      if (!fromDate) continue;

      const k = normalizeJoinKeyValue(`${company ?? ""}|${supplier ?? ""}`);
      if (!k) continue;

      addEntry(k, {
        from: fromDate,
        to: null,
        term: term == null ? null : String(term).trim(),
      });
    }

    // Sort and set window end dates
    for (const [k, arr] of history.entries()) {
      arr.sort((a, b) => a.from.getTime() - b.from.getTime());
      for (let i = 0; i < arr.length; i++) {
        const next = arr[i + 1];
        arr[i].to = next ? next.from : null;
      }
    }

    return history;
  };

  const resolveEffectivePaymentTerm = ({ historyIndex, row }) => {
    if (!historyIndex || !historyIndex.size) return null;

    const supplier =
      pickFromRowLoose(row, "Supplier") ??
      pickFromRowLoose(row, "Vendor") ??
      pickFromRowLoose(row, "Vendor Account") ??
      pickFromRowLoose(row, "Vendor Account No") ??
      pickFromRowLoose(row, "Vendor Account: Name 1") ??
      pickNamespacedAnyRole(row, "Supplier") ??
      pickNamespacedAnyRole(row, "Vendor") ??
      null;

    const company =
      pickFromRowLoose(row, "Company Code") ??
      pickFromRowLoose(row, "company_code") ??
      pickNamespacedAnyRole(row, "Company Code") ??
      pickNamespacedAnyRole(row, "company_code") ??
      null;

    const invDateRaw =
      pickFromRowLoose(row, "invoice_issue_date") ??
      pickFromRowLoose(row, "Invoice Issue Date") ??
      pickFromRowLoose(row, "Document Date") ??
      null;

    const invDate = parseDateLoose(invDateRaw);
    if (!invDate) return null;

    const k = normalizeJoinKeyValue(`${company ?? ""}|${supplier ?? ""}`);
    if (!k) return null;

    const windows = historyIndex.get(k);
    if (!Array.isArray(windows) || !windows.length) return null;

    const t = invDate.getTime();

    let chosen = null;
    for (const w of windows) {
      const fromT = w.from.getTime();
      const toT = w.to ? w.to.getTime() : null;
      if (t >= fromT && (toT == null || t < toT)) {
        chosen = w;
      }
    }

    return chosen ? chosen.term : null;
  };

  // Helper functions for canonical projection

  const _toNum = (v) => {
    if (v == null || v === "") return null;
    // Handle things like "-11,183.65" or "$1,234".
    const s = String(v)
      .replace(/\$/g, "")
      .replace(/[\s,]+/g, "")
      .trim();
    if (!s) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  };

  const applyTransform = ({ value, transformType, transformConfig }) => {
    const tt = (transformType || "").toString().trim().toLowerCase();
    if (!tt) return value;

    // MVP: support absolute numeric amounts (payments/invoices)
    if (tt === "abs" || tt === "absolute" || tt === "absolute_numeric") {
      const n = _toNum(value);
      return n == null ? null : Math.abs(n);
    }

    // Trim strings
    if (tt === "trim") {
      return value == null ? null : String(value).trim();
    }

    // Date normalisation (returns YYYY-MM-DD)
    if (tt === "date" || tt === "date_yyyy_mm_dd") {
      const d = parseDateLoose(value);
      if (!d) return null;
      const yyyy = d.getUTCFullYear();
      const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
      const dd = String(d.getUTCDate()).padStart(2, "0");
      return `${yyyy}-${mm}-${dd}`;
    }

    // Unknown transform: leave as-is for MVP
    return value;
  };

  const resolveCanonicalValue = ({
    sourceRole,
    sourceColumn,
    srcRow,
    outRow,
  }) => {
    const col = sourceColumn;
    if (!col) return null;

    const colSnake = toSnake(col);

    // Deterministic key lookup only: exact key, then its snake_case form.
    // Prefer already-mapped output row first (because mappings are stored in snake_case),
    // then fall back to merged source row.

    const fromOut =
      pickFromRowLoose(outRow, col) ??
      (colSnake ? pickFromRowLoose(outRow, colSnake) : null);

    if (fromOut != null && String(fromOut).trim() !== "") return fromOut;

    const fromSrc =
      pickFromRowLoose(srcRow, col) ??
      (colSnake ? pickFromRowLoose(srcRow, colSnake) : null);

    return fromSrc;
  };

  // Build payment terms history index once (if a suitable dataset exists)
  const sPaymentTerms = stageStart("build_payment_terms_history_index");
  let paymentTermsHistoryIndex = null;
  try {
    const paymentDs = await getPaymentTermsDataset();
    if (paymentDs && paymentDs.id) {
      paymentTermsHistoryIndex = await loadPaymentTermsHistoryIndex(
        paymentDs.id,
      );
      slog.info(
        "PTRS v2 composeMappedRowsForPtrs: payment terms history index built",
        safeMeta({
          customerId,
          ptrsId,
          role: paymentDs.role || null,
          keysCount: paymentTermsHistoryIndex
            ? paymentTermsHistoryIndex.size
            : 0,
        }),
      );
    }
  } catch (e) {
    slog.warn(
      "PTRS v2 composeMappedRowsForPtrs: payment terms history index failed",
      safeMeta({ customerId, ptrsId, error: e.message }),
    );
    paymentTermsHistoryIndex = null;
  }
  stageEnd(sPaymentTerms, {
    paymentTermsKeysCount: paymentTermsHistoryIndex
      ? paymentTermsHistoryIndex.size
      : 0,
  });

  const isMainRole = (role) => {
    const r = String(role || "").toLowerCase();
    return r === "main" || r.startsWith("main_");
  };

  const nsKey = (role, col) => `${String(role)}__${String(col)}`;

  const getJoinLhsValue = (row, role, col) => {
    if (!row) return undefined;
    const r = String(role || "").toLowerCase();
    if (isMainRole(r)) {
      return pickFromRowLoose(row, col);
    }
    // Supporting roles are always namespaced to avoid collisions.
    return pickFromRowLoose(row, nsKey(r, col));
  };

  const mergeRoleRowNamespaced = (row, role, joined) => {
    const r = String(role || "").toLowerCase();
    if (!joined || typeof joined !== "object") return row;
    const out = { ...(row || {}) };
    for (const [k, v] of Object.entries(joined)) {
      out[nsKey(r, k)] = v;
    }
    return out;
  };

  const hasRoleInRow = (row, role) => {
    const r = String(role || "").toLowerCase();
    if (isMainRole(r)) return true;
    const prefix = `${r}__`;
    return Object.keys(row || {}).some((k) => String(k).startsWith(prefix));
  };

  const pickNamespacedAnyRole = (row, col) => {
    if (!row || !col) return null;
    const suffix = `__${String(col)}`;
    const keys = Object.keys(row).filter((k) => String(k).endsWith(suffix));
    if (!keys.length) return null;
    keys.sort((a, b) => String(a).localeCompare(String(b)));
    return pickFromRowLoose(row, keys[0]);
  };

  // ---------------- Transform-aware join indexing (per-condition) ----------------
  // We cannot rely on buildDatasetIndexByRole because join keys may require per-condition transforms
  // (digits_only, strip_prefix, lpad, etc.) and joins must be applied sequentially.
  const joinIndexCache = new Map(); // Map<cacheKey, Map<joinKey, row>>
  const datasetRowsCache = new Map(); // Map<role, Array<rowObject>>

  const joinIndexKey = (role, column, transform) => {
    const op = transform?.op ? String(transform.op) : "";
    const arg = transform?.arg != null ? String(transform.arg) : "";
    return `${role}|${column}|${op}|${arg}`;
  };

  const loadRowsForRole = async (role) => {
    const r = String(role || "").toLowerCase();
    if (!r) return [];
    if (datasetRowsCache.has(r)) return datasetRowsCache.get(r);

    const ds = await db.PtrsDataset.findOne({
      where: { customerId, ptrsId, role: r },
      attributes: ["id"],
      raw: true,
      transaction,
    });

    if (!ds || !ds.id) {
      datasetRowsCache.set(r, []);
      return [];
    }

    const where = { customerId, datasetId: ds.id };
    if (
      db.PtrsImportRaw.rawAttributes &&
      db.PtrsImportRaw.rawAttributes.ptrsDatasetId
    ) {
      delete where.datasetId;
      where.datasetId = ds.id;
    }

    const rows = await db.PtrsImportRaw.findAll({
      where,
      order: [["rowNo", "ASC"]],
      attributes: ["data"],
      raw: true,
      transaction,
    });

    const parsed = (rows || []).map((x) => {
      let d = x?.data || {};
      if (typeof d === "string") {
        try {
          d = JSON.parse(d);
        } catch {
          d = {};
        }
      }
      return d && typeof d === "object" ? d : {};
    });

    datasetRowsCache.set(r, parsed);
    return parsed;
  };

  const getJoinIndex = async ({ role, column, transform }) => {
    const r = String(role || "").toLowerCase();
    if (!r) return new Map();
    if (isMainRole(r)) {
      throw new Error(
        `Invalid join target role '${r}' — cannot build an index for main roles`,
      );
    }

    const cacheKey = joinIndexKey(r, column, transform);
    if (joinIndexCache.has(cacheKey)) return joinIndexCache.get(cacheKey);

    const sIdx = stageStart("build_join_index");
    const rows = await loadRowsForRole(r);
    const idx = new Map();

    for (const row of rows) {
      const rawVal = pickFromRowLoose(row, column);
      const k = normalizeJoinKeyValue(rawVal, transform);
      if (!k) continue;
      // Deterministic: first row wins for a given key.
      if (!idx.has(k)) idx.set(k, row);
    }

    stageEnd(sIdx, {
      role: r,
      column,
      transform: transform || null,
      rowsScanned: Array.isArray(rows) ? rows.length : 0,
      indexSize: idx.size,
    });

    joinIndexCache.set(cacheKey, idx);
    return idx;
  };

  // ---------------- Determine anchor/main dataset for this ptrs run ----------------
  // IMPORTANT: PtrsImportRaw contains rows for *all* datasets (main + supporting).
  // Mapped rows must be built from the anchor/main dataset only, otherwise rowNo will collide.
  let mainDatasetId = null;
  const sMainDataset = stageStart("resolve_main_dataset");
  try {
    const dsRows = await db.PtrsDataset.findAll({
      where: { customerId, ptrsId },
      attributes: ["id", "role", "createdAt"],
      raw: true,
      transaction,
    });

    const normRole = (r) =>
      String(r || "")
        .trim()
        .toLowerCase();
    const byRole = (role) =>
      (dsRows || []).find((d) => normRole(d.role) === role);

    const main = byRole("main");
    const anchor = byRole("anchor");

    if (main && main.id) mainDatasetId = main.id;
    else if (anchor && anchor.id) mainDatasetId = anchor.id;
    else if (Array.isArray(dsRows) && dsRows.length === 1)
      mainDatasetId = dsRows[0].id;

    slog.info(
      "PTRS v2 composeMappedRowsForPtrs: resolved main dataset",
      safeMeta({
        customerId,
        ptrsId,
        mainDatasetId,
        datasetCount: Array.isArray(dsRows) ? dsRows.length : 0,
        roles: Array.isArray(dsRows) ? dsRows.map((d) => d.role) : [],
      }),
    );
  } catch (e) {
    slog.warn(
      "PTRS v2 composeMappedRowsForPtrs: failed to resolve main dataset; falling back to unscoped import_raw",
      safeMeta({ customerId, ptrsId, error: e.message }),
    );
    mainDatasetId = null;
  }
  stageEnd(sMainDataset, { mainDatasetId });

  // Read main rows
  const findOpts = {
    where: mainDatasetId
      ? { customerId, ptrsId, datasetId: mainDatasetId }
      : { customerId, ptrsId },
    order: [["rowNo", "ASC"]],
    attributes: ["rowNo", "data"],
    raw: true,
    transaction,
  };

  const numericLimit = Number(limit);
  if (Number.isFinite(numericLimit) && numericLimit > 0) {
    findOpts.limit = Math.min(numericLimit, 5000);
  }
  if (Number.isFinite(offset) && offset >= 0) {
    findOpts.offset = offset;
  }

  const sLoadMain = stageStart("load_main_rows");
  const mainRows = await db.PtrsImportRaw.findAll(findOpts);
  stageEnd(sLoadMain, {
    rowsLoaded: Array.isArray(mainRows) ? mainRows.length : 0,
  });

  if (!mainDatasetId) {
    try {
      const dsCount = await db.PtrsDataset.count({
        where: { customerId, ptrsId },
        transaction,
      });
      if (dsCount > 1) {
        slog.warn(
          "PTRS v2 composeMappedRowsForPtrs: mainDatasetId not resolved while multiple datasets exist; mapped rows may include supporting datasets",
          safeMeta({ customerId, ptrsId, datasetCount: dsCount }),
        );
      }
    } catch (_) {}
  }

  const loopStartNs = process.hrtime.bigint();

  const counters = {
    rowsInput: Array.isArray(mainRows) ? mainRows.length : 0,
    joinsOrdered: Array.isArray(orderedJoins) ? orderedJoins.length : 0,
    joinAttempts: 0,
    joinSkippedMissingFromRole: 0,
    joinNoKey: 0,
    joinIndexLookups: 0,
    joinMatched: 0,
    joinNoMatch: 0,
    customFieldsApplied: 0,
    canonicalProjectionApplied: 0,
    paymentTermsEffectiveApplied: 0,
  };

  const composed = [];

  let loggedFirst = false;
  let loggedJoinProbe = false;

  for (const r of mainRows) {
    const base = r.data || {};
    let srcRow = base;

    // Apply joins sequentially, supporting main->supporting and supporting->supporting joins.
    // Supporting role columns are merged into the working row using role namespaces to avoid collisions.
    if (orderedJoins.length) {
      let workingRow = srcRow;

      for (const j of orderedJoins) {
        counters.joinAttempts += 1;
        const fromRole = String(j.fromRole || "").toLowerCase();
        const toRole = String(j.toRole || "").toLowerCase();

        const fromCol = j.fromColumn;
        const toCol = j.toColumn;

        const fromTransform = j.fromTransform || null;
        const toTransform = j.toTransform || null;

        if (!fromRole || !toRole || !fromCol || !toCol) continue;

        // If a join reads from a supporting role that isn't present on this row yet,
        // it usually means the earlier join that would have brought that role in
        // simply didn't match for this specific record.
        // That's not a global "invalid join order" — it's a per-row absence.
        // In that case we skip this join for this row.
        if (!isMainRole(fromRole) && !hasRoleInRow(workingRow, fromRole)) {
          counters.joinSkippedMissingFromRole += 1;
          if (!loggedJoinProbe && logger && logger.debug) {
            loggedJoinProbe = true;
            slog.debug(
              "PTRS v2 composeMappedRowsForPtrs: join probe (missing fromRole on row; skipping)",
              safeMeta({
                customerId,
                ptrsId,
                join: j,
                fromRole,
              }),
            );
          }
          continue;
        }

        // Do not allow indexing main roles as targets.
        if (isMainRole(toRole)) {
          throw new Error(
            `Invalid join target: toRole '${toRole}' must be a supporting dataset role`,
          );
        }

        const lhsVal = getJoinLhsValue(workingRow, fromRole, fromCol);
        const key = normalizeJoinKeyValue(lhsVal, fromTransform);

        if (!key) {
          counters.joinNoKey += 1;
          if (!loggedJoinProbe && logger && logger.debug) {
            loggedJoinProbe = true;
            slog.debug(
              "PTRS v2 composeMappedRowsForPtrs: join probe (no key)",
              safeMeta({
                customerId,
                ptrsId,
                join: j,
                rawValue: lhsVal,
                normalisedKey: key,
              }),
            );
          }
          continue;
        }

        counters.joinIndexLookups += 1;
        const idx = await getJoinIndex({
          role: toRole,
          column: toCol,
          transform: toTransform,
        });

        const joined = idx.get(key);

        if (joined) {
          counters.joinMatched += 1;
          workingRow = mergeRoleRowNamespaced(workingRow, toRole, joined);

          if (!loggedJoinProbe && logger && logger.debug) {
            loggedJoinProbe = true;
            slog.debug(
              "PTRS v2 composeMappedRowsForPtrs: join probe (matched)",
              safeMeta({
                customerId,
                ptrsId,
                join: j,
                rawValue: lhsVal,
                normalisedKey: key,
                joinedKeys: Object.keys(joined || {}),
              }),
            );
          }
        } else if (!loggedJoinProbe && logger && logger.debug) {
          counters.joinNoMatch += 1;
          loggedJoinProbe = true;
          slog.debug(
            "PTRS v2 composeMappedRowsForPtrs: join probe (no match)",
            safeMeta({
              customerId,
              ptrsId,
              join: j,
              rawValue: lhsVal,
              normalisedKey: key,
            }),
          );
        }
      }

      srcRow = workingRow;
    }

    const rawPaymentTerms = pickFromRowLoose(srcRow, "Payment terms");

    const effectivePaymentTerms = resolveEffectivePaymentTerm({
      historyIndex: paymentTermsHistoryIndex,
      row: srcRow,
    });

    if (
      effectivePaymentTerms != null &&
      String(effectivePaymentTerms).trim() !== ""
    ) {
      counters.paymentTermsEffectiveApplied += 1;
      srcRow = {
        ...srcRow,
        "Payment terms": effectivePaymentTerms,
        __ptrs_payment_terms_raw: rawPaymentTerms ?? null,
        __ptrs_payment_terms_effective: effectivePaymentTerms,
      };
    } else {
      srcRow = {
        ...srcRow,
        __ptrs_payment_terms_raw: rawPaymentTerms ?? null,
        __ptrs_payment_terms_effective: rawPaymentTerms ?? null,
      };
    }

    let out = applyColumnMappingsToRow({ mappings, sourceRow: srcRow });
    // Apply custom fields at this point, so mapped dataset includes them
    if (Array.isArray(customFields) && customFields.length) {
      out = applyCustomFields({
        row: out,
        rawRow: srcRow,
        customFields,
      });
      counters.customFieldsApplied += 1;
    }
    out.row_no = r.rowNo;

    out.invoice_payment_terms_raw = srcRow.__ptrs_payment_terms_raw;
    out.invoice_payment_terms_effective = srcRow.__ptrs_payment_terms_effective;

    // ---------------- Canonical projection ----------------
    // Project canonical fields onto the mapped row using the profile-scoped field map.
    // We MERGE (non-breaking) so existing mapped keys remain available during MVP.
    if (Array.isArray(fieldMapRows) && fieldMapRows.length) {
      counters.canonicalProjectionApplied += 1;
      const canonicalOut = {};
      for (const fm of fieldMapRows) {
        if (!fm || typeof fm !== "object") continue;
        const canonicalKey = toSnake(fm.canonicalField);
        if (!canonicalKey) continue;

        const rawValue = resolveCanonicalValue({
          sourceRole: fm.sourceRole,
          sourceColumn: fm.sourceColumn,
          srcRow,
          outRow: out,
        });

        const transformed = applyTransform({
          value: rawValue,
          transformType: fm.transformType,
          transformConfig: fm.transformConfig,
        });

        // Always set the key (even null) so downstream headers are stable
        canonicalOut[canonicalKey] = transformed == null ? null : transformed;
      }

      // Canonical values win if there is a collision.
      out = { ...out, ...canonicalOut };
    }

    if (!loggedFirst && logger && logger.debug) {
      loggedFirst = true;
      slog.debug(
        "PTRS v2 composeMappedRowsForPtrs: sample composed row",
        safeMeta({
          customerId,
          ptrsId,
          sampleRowKeys: Object.keys(out || {}),
          hasCustomFieldsApplied:
            Array.isArray(customFields) && customFields.length > 0,
        }),
      );
    }

    composed.push(out);
  }

  trace?.write("compose_loop_complete", {
    durationMs: hrMsSince(loopStartNs),
    ...counters,
  });

  // Sample-based header computation: scan up to first 200 rows, cap at 2000 headers
  const headerSet = new Set();
  for (let i = 0; i < composed.length && i < 200; ++i) {
    const row = composed[i];
    for (const k of Object.keys(row)) {
      if (headerSet.size < 2000) headerSet.add(k);
    }
    if (headerSet.size >= 2000) break;
  }
  const headers = Array.from(headerSet);

  trace?.write("compose_headers_built", {
    headersCount: Array.isArray(headers) ? headers.length : 0,
  });

  trace?.write("compose_end", {
    rowsOut: Array.isArray(composed) ? composed.length : 0,
    totalMs: hrMsSince(composeStartNs),
  });

  return { rows: composed, headers };
}

// /**
//  * Return unified headers and examples across main import + all supporting datasets.
//  * Reuses getImportSample for main rows/headers and augments headerMeta with supporting datasets.
//  */
// async function getUnifiedSample({
//   customerId,
//   ptrsId,
//   limit = 10,
//   offset = 0,
// }) {
//   const t = await beginTransactionWithCustomerContext(customerId);
//   try {
//     // Base = main only
//     const base = await getImportSample({ customerId, ptrsId, limit, offset });
//     const headerSet = new Set(base.headers || []);

//     // Make headerMeta mutable (sources as Set)
//     const headerMeta = {};
//     for (const [k, meta] of Object.entries(base.headerMeta || {})) {
//       headerMeta[k] = {
//         sources: new Set([...(meta.sources || [])]),
//         examples: { ...(meta.examples || {}) },
//       };
//     }

//     // Merge supporting dataset headers + examples
//     try {
//       const dsRows = await db.PtrsDataset.findAll({
//         where: { customerId, ptrsId },
//         attributes: ["id", "meta", "role"],
//         raw: true,
//         transaction: t,
//       });

//       if (Array.isArray(dsRows) && dsRows.length) {
//         const addHeaders = (arr, role) => {
//           for (const h of arr || []) {
//             if (h == null) continue;
//             const s = String(h).trim();
//             if (!s) continue;
//             headerSet.add(s);
//             headerMeta[s] = headerMeta[s] || {
//               sources: new Set(),
//               examples: {},
//             };
//             if (role) headerMeta[s].sources.add(role);
//           }
//         };

//         for (const ds of dsRows) {
//           const role = ds.role || "dataset";
//           const meta = ds.meta || {};
//           let dsHeaders = Array.isArray(meta.headers) ? meta.headers : null;
//           let sampleRows = null;
//           try {
//             const sample = await getDatasetSample({
//               customerId,
//               datasetId: ds.id,
//               limit: 5,
//               offset: 0,
//             });
//             dsHeaders =
//               dsHeaders && dsHeaders.length ? dsHeaders : sample.headers;
//             sampleRows = Array.isArray(sample.rows) ? sample.rows : [];
//           } catch (_) {}

//           addHeaders(dsHeaders, role);

//           if (sampleRows && sampleRows.length) {
//             for (const row of sampleRows) {
//               for (const [k, v] of Object.entries(row)) {
//                 if (v != null && String(v).trim() !== "") {
//                   headerMeta[k] = headerMeta[k] || {
//                     sources: new Set(),
//                     examples: {},
//                   };
//                   if (headerMeta[k].examples[role] == null) {
//                     headerMeta[k].examples[role] = v;
//                   }
//                 }
//               }
//             }
//           }
//         }
//       }
//     } catch (e) {
//       slog.warn(
//         "PTRS v2 getUnifiedSample: failed merging supporting datasets",
//         {
//           action: "PtrsV2GetUnifiedSampleMergeHeaders",
//           customerId,
//           ptrsId,
//           error: e.message,
//         }
//       );
//     }

//     // Finalise: convert Sets to arrays and pick a preferred example
//     const unifiedHeaders = Array.from(headerSet.values());
//     const finalizedHeaderMeta = {};
//     for (const key of Object.keys(headerMeta)) {
//       const meta = headerMeta[key];
//       const sources = Array.from(meta.sources || []);
//       let example = null;
//       if (meta.examples) {
//         if (meta.examples.main != null) example = meta.examples.main;
//         else {
//           const firstRole = Object.keys(meta.examples)[0];
//           if (firstRole) example = meta.examples[firstRole];
//         }
//       }
//       finalizedHeaderMeta[key] = {
//         sources,
//         examples: meta.examples || {},
//         example,
//       };
//     }

//     slog.info("PTRS v2 getUnifiedSample: done", {
//       action: "PtrsV2GetUnifiedSample",
//       customerId,
//       ptrsId,
//       rowsReturned: Array.isArray(base.rows) ? base.rows.length : 0,
//       total: base.total || 0,
//       unifiedHeadersCount: unifiedHeaders.length,
//       headerMetaKeys: Object.keys(finalizedHeaderMeta).length,
//     });

//     await t.commit();
//     return {
//       rows: base.rows || [],
//       total: base.total || 0,
//       headers: unifiedHeaders,
//       headerMeta: finalizedHeaderMeta,
//     };
//   } catch (err) {
//     if (!t.finished) {
//       try {
//         await t.rollback();
//       } catch (_) {}
//     }
//     throw err;
//   }
// }

function applyColumnMappingsToRow({ mappings, sourceRow }) {
  const out = {};
  for (const [sourceHeader, cfg] of Object.entries(mappings || {})) {
    if (!cfg) continue;
    const target = cfg.field || cfg.target;
    if (!target) continue;
    let value;
    if (Object.prototype.hasOwnProperty.call(cfg, "value")) {
      value = cfg.value;
    } else {
      value = pickFromRowLoose(sourceRow, sourceHeader);
    }
    out[toSnake(target)] = value ?? null;
  }
  return out;
}

/**
 * Apply custom fields logic to a mapped row.
 * @param {Object} param0
 * @param {Object} param0.row - The mapped row (already mapped).
 * @param {Object} param0.rawRow - The full source row (joined+input).
 * @param {Array} param0.customFields - Array of custom field configs.
 * @returns {Object} - New row with custom fields applied.
 */
/**
 * Apply custom fields logic to a mapped row.
 * @param {Object} param0
 * @param {Object} param0.row - The mapped row (already mapped).
 * @param {Object} param0.rawRow - The full source row (joined+input).
 * @param {Array} param0.customFields - Array of custom field configs.
 * @returns {Object} - New row with custom fields applied.
 */
function applyCustomFields({ row, rawRow, customFields }) {
  const out = { ...row };
  if (!Array.isArray(customFields)) return out;

  for (const cf of customFields) {
    if (!cf || typeof cf !== "object") continue;

    const key = cf.key || cf.field;
    if (!key) continue;

    const type = cf.type || "concat";

    if (type === "concat") {
      const segments = Array.isArray(cf.segments) ? cf.segments : [];
      const parts = [];

      for (const segment of segments) {
        if (!segment || typeof segment !== "object") continue;

        if (segment.kind === "literal") {
          if (segment.value !== null && segment.value !== undefined) {
            parts.push(String(segment.value));
          }
        } else if (segment.kind === "field") {
          const value = pickFromRowLoose(rawRow, segment.name);
          if (value !== null && value !== undefined) {
            parts.push(String(value));
          }
        }
      }

      // Use snake_case for the final column name to match the rest of the mapped schema
      out[toSnake(key)] = parts.join("");
    }

    // Other custom field types can be added here in future (e.g. arithmetic, case transforms, etc.)
  }

  return out;
}

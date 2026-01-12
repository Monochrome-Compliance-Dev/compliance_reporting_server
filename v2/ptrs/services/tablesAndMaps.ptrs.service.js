const db = require("@/db/database");

const { logger } = require("@/helpers/logger");
const {
  safeMeta,
  slog,
  mergeJoinedRow,
  normalizeJoinKeyValue,
  toSnake,
} = require("@/v2/ptrs/services/ptrs.service");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  pickFromRowLoose,
  buildDatasetIndexByRole,
  getDatasetSample,
} = require("@/v2/ptrs/services/data.ptrs.service");

const { normalizeAmountLike } = require("@/helpers/amountNormaliser");
const { Op } = require("sequelize");

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
};

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
      })
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
    new Set(composed.flatMap((row) => Object.keys(row)))
  );

  if (logger && logger.debug && composed.length) {
    slog.debug(
      "PTRS v2 loadMappedRowsForPtrs: sample composed row",
      safeMeta({
        customerId,
        ptrsId,
        sampleRowKeys: Object.keys(composed[0] || {}),
        headersCount: headers.length,
      })
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
  map.joins = maybeParse(map.joins);
  map.rowRules = maybeParse(map.rowRules);
  map.customFields = maybeParse(map.customFields);
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
      })
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

/**
 * Return a small window of staged rows plus count and inferred headers.
 * Also returns headerMeta: sources and example values per header.
 */
async function getImportSample({ customerId, ptrsId, limit = 10, offset = 0 }) {
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
      where: { customerId, ptrsId },
      order: [["rowNo", "ASC"]],
      limit,
      offset,
      attributes: ["rowNo", "data"],
      raw: true,
      transaction: t,
    });

    // total
    const total = await db.PtrsImportRaw.count({
      where: { customerId, ptrsId },
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
      where: { customerId, ptrsId },
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
          }
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
  joins = null,
  rowRules = null,
  profileId = null,
  customFields = null,
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

    const resolveField = (incoming, existingValue) =>
      typeof incoming === "undefined" ? existingValue : incoming;

    // Special handling for joins:
    // - Joins saved via the JoinsDesigner are sent as an object (e.g. { conditions: [...] })
    // - Calls that don't intend to touch joins (e.g. MapPanel) currently send a bare []
    //   due to controller defaults. Treat that bare [] as "no change", not "clear joins".
    let nextJoins;
    if (Array.isArray(joins)) {
      // If the UI really wants to clear joins, it should send an explicit object,
      // e.g. { conditions: [] }. A naked [] here is treated as "no joins payload".
      nextJoins = existing ? existing.joins : null;
    } else {
      nextJoins = resolveField(joins, existing ? existing.joins : null);
    }

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
      })
    );

    if (existing) {
      await existing.update(
        {
          ...payload,
          updatedBy: userId || existing.updatedBy || existing.createdBy || null,
        },
        { transaction: t }
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
      { transaction: t }
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

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    slog.info(
      "PTRS v2 buildMappedDatasetForPtrs: begin",
      safeMeta({
        customerId,
        ptrsId,
      })
    );

    // Compose the fully mapped + joined rows for this ptrs run.
    // We intentionally pass limit: null here so the composer decides how much to load
    // (typically the full dataset for this run).
    const { rows, headers } = await composeMappedRowsForPtrs({
      customerId,
      ptrsId,
      limit: null,
      transaction: t,
    });

    // Ensure canonical defaults/transforms are applied BEFORE persisting mapped rows.
    // Stage reads from tbl_ptrs_mapped_row, so this is the correct place to guarantee
    // trade credit flags + amount normalisation are present end-to-end.
    const canonicalRows = (rows || []).map((r) => ensureCanonicalRowShape(r));

    // Recompute headers from canonical rows to reflect any injected canonical keys.
    const canonicalHeaders = Array.from(
      new Set(canonicalRows.flatMap((row) => Object.keys(row || {})))
    );

    const total = Array.isArray(canonicalRows) ? canonicalRows.length : 0;

    // Clear any existing mapped rows for this ptrs run so we keep exactly one snapshot
    await db.PtrsMappedRow.destroy({
      where: { customerId, ptrsId },
      transaction: t,
    });

    if (!total) {
      slog.info(
        "PTRS v2 buildMappedDatasetForPtrs: no rows composed, nothing persisted",
        safeMeta({ customerId, ptrsId })
      );
      await t.commit();
      return { count: 0, headers: canonicalHeaders || [] };
    }

    const nowIso = new Date().toISOString();

    const payload = canonicalRows.map((row, index) => ({
      customerId,
      ptrsId,
      // Prefer an explicit row_no from the composer if present; otherwise fallback to index
      rowNo:
        typeof row.row_no === "number" && Number.isFinite(row.row_no)
          ? row.row_no
          : index + 1,
      data: row,
      meta: {
        stage: "ptrs.v2.mapped",
        builtAt: nowIso,
        builtBy: actorId || null,
      },
    }));

    await db.PtrsMappedRow.bulkCreate(payload, {
      transaction: t,
      validate: false,
    });

    slog.info(
      "PTRS v2 buildMappedDatasetForPtrs: persisted mapped rows",
      safeMeta({
        customerId,
        ptrsId,
        rowsPersisted: total,
        headersCount: Array.isArray(canonicalHeaders)
          ? canonicalHeaders.length
          : 0,
      })
    );

    await t.commit();

    return {
      count: total,
      headers: canonicalHeaders || [],
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

// Compose mapped rows for a ptrs, including join and column mapping logic
async function composeMappedRowsForPtrs({
  customerId,
  ptrsId,
  limit = 50,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  // Load column map (with joins + rowRules etc.)
  const mapRow = await getColumnMap({ customerId, ptrsId, transaction });
  const map = mapRow || {};
  const mappings = map.mappings || {};

  // Canonical field map is profile-scoped. We use the profileId saved on the column map.
  const profileId = map.profileId || null;
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
      safeMeta({ customerId, ptrsId, profileId, error: e.message })
    );
    fieldMapRows = [];
  }

  if (logger && logger.info) {
    slog.info(
      "PTRS v2 composeMappedRowsForPtrs: field map loaded",
      safeMeta({
        customerId,
        ptrsId,
        profileId,
        fieldMapCount: Array.isArray(fieldMapRows) ? fieldMapRows.length : 0,
      })
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
      })
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

    const fromRole = (from.role || "").toLowerCase();
    const toRole = (to.role || "").toLowerCase();
    const fromCol = from.column;
    const toCol = to.column;

    if (!fromRole || !toRole || !fromCol || !toCol) continue;

    // Only support joins that involve the main dataset on one side
    const isFromMain = fromRole === "main";
    const isToMain = toRole === "main";
    if (!isFromMain && !isToMain) continue;

    const mainSide = isFromMain ? from : to;
    const otherSide = isFromMain ? to : from;

    if (!otherSide.role || !otherSide.column) continue;

    normalisedJoins.push({
      mainColumn: mainSide.column,
      otherRole: String(otherSide.role).toLowerCase(),
      otherColumn: otherSide.column,
    });
  }

  // Defensive log for debugging joins, if logger.info is available
  if (logger && logger.info) {
    slog.info(
      "PTRS v2 composeMappedRowsForPtrs: normalised joins",
      safeMeta({ customerId, ptrsId, joinsCount: normalisedJoins.length })
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
      })
    );
  }

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
        "Dataset row model not available (expected PtrsDatasetRow/PtrsDatasetData); cannot load payment terms dataset rows"
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
      null;

    const company =
      pickFromRowLoose(row, "Company Code") ??
      pickFromRowLoose(row, "company_code") ??
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
  let paymentTermsHistoryIndex = null;
  try {
    const paymentDs = await getPaymentTermsDataset();
    if (paymentDs && paymentDs.id) {
      paymentTermsHistoryIndex = await loadPaymentTermsHistoryIndex(
        paymentDs.id
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
        })
      );
    }
  } catch (e) {
    slog.warn(
      "PTRS v2 composeMappedRowsForPtrs: payment terms history index failed",
      safeMeta({ customerId, ptrsId, error: e.message })
    );
    paymentTermsHistoryIndex = null;
  }

  // Build indexes for each supporting dataset role referenced in joins
  const roleIndexes = new Map();
  for (const j of normalisedJoins) {
    if (!j.otherRole || !j.otherColumn) continue;
    if (roleIndexes.has(j.otherRole)) continue;

    const idx = await buildDatasetIndexByRole({
      customerId,
      ptrsId,
      role: j.otherRole,
      keyColumn: j.otherColumn,
      transaction,
    });

    roleIndexes.set(
      j.otherRole,
      idx || { map: new Map(), headers: [], rowsIndexed: 0 }
    );
  }

  if (logger && logger.info) {
    const rolesMeta = [];
    for (const [role, idx] of roleIndexes.entries()) {
      rolesMeta.push({
        role,
        rowsIndexed:
          idx && typeof idx.rowsIndexed === "number"
            ? idx.rowsIndexed
            : idx && idx.map && idx.map.size
              ? idx.map.size
              : 0,
        headersCount: Array.isArray(idx && idx.headers)
          ? idx.headers.length
          : 0,
      });
    }
    slog.info(
      "PTRS v2 composeMappedRowsForPtrs: role indexes built",
      safeMeta({ customerId, ptrsId, rolesMeta })
    );
  }

  // Read main rows
  const findOpts = {
    where: { customerId, ptrsId },
    order: [["rowNo", "ASC"]],
    attributes: ["rowNo", "data"],
    raw: true,
    transaction,
  };

  const numericLimit = Number(limit);
  if (Number.isFinite(numericLimit) && numericLimit > 0) {
    // For previews we still want a cap; for full rules-apply we will pass null
    findOpts.limit = Math.min(numericLimit, 5000);
  }

  const mainRows = await db.PtrsImportRaw.findAll(findOpts);

  const composed = [];

  let loggedFirst = false;
  let loggedJoinProbe = false;

  for (const r of mainRows) {
    const base = r.data || {};
    let srcRow = base;

    // Apply each join in turn, merging any matched supporting-row data
    if (normalisedJoins.length && roleIndexes.size) {
      for (const j of normalisedJoins) {
        const idx = roleIndexes.get(j.otherRole);
        if (!idx || !idx.map || !idx.map.size) {
          continue;
        }

        const lhsVal = pickFromRowLoose(base, j.mainColumn);
        const key = normalizeJoinKeyValue(lhsVal);

        if (!key) {
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
              })
            );
          }
          continue;
        }

        const joined = idx.map.get(key);
        if (joined) {
          srcRow = mergeJoinedRow(srcRow, joined);
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
              })
            );
          }
        } else if (!loggedJoinProbe && logger && logger.debug) {
          loggedJoinProbe = true;
          slog.debug(
            "PTRS v2 composeMappedRowsForPtrs: join probe (no match)",
            safeMeta({
              customerId,
              ptrsId,
              join: j,
              rawValue: lhsVal,
              normalisedKey: key,
            })
          );
        }
      }
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
    }
    out.row_no = r.rowNo;

    out.invoice_payment_terms_raw = srcRow.__ptrs_payment_terms_raw;
    out.invoice_payment_terms_effective = srcRow.__ptrs_payment_terms_effective;

    // ---------------- Canonical projection ----------------
    // Project canonical fields onto the mapped row using the profile-scoped field map.
    // We MERGE (non-breaking) so existing mapped keys remain available during MVP.
    if (Array.isArray(fieldMapRows) && fieldMapRows.length) {
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
        })
      );
    }

    composed.push(out);
  }

  const headers = Array.from(
    new Set(composed.flatMap((row) => Object.keys(row)))
  );

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

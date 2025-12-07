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

module.exports = {
  getMap,
  getColumnMap,
  getImportSample,
  saveColumnMap,
  buildMappedDatasetForPtrs,
  composeMappedRowsForPtrs,
  loadMappedRowsForPtrs,
  // getUnifiedSample,
};

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

  const composed = rows.map((r) => {
    const base = r.data || {};
    // ensure row_no is present for downstream logic
    return { ...base, row_no: r.rowNo };
  });

  // Simple header inference from the mapped rows
  const headers = Array.from(
    new Set(composed.flatMap((row) => Object.keys(row)))
  );

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
async function getColumnMap({ customerId, ptrsId }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const map = await db.PtrsColumnMap.findOne({
      where: { customerId, ptrsId },
      transaction: t,
      raw: true,
    });
    await t.commit();
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
      })
    );
    return map || null;
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

    const payload = {
      mappings,
      extras,
      fallbacks,
      defaults,
      joins,
      rowRules,
      profileId,
      customFields,
    };

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

    const total = Array.isArray(rows) ? rows.length : 0;

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
      return { count: 0, headers: headers || [] };
    }

    const nowIso = new Date().toISOString();

    const payload = rows.map((row, index) => ({
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
        headersCount: Array.isArray(headers) ? headers.length : 0,
      })
    );

    await t.commit();

    return {
      count: total,
      headers: headers || [],
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
  console.log("map.joins raw =", map.joins);

  // Normalise joins – we currently support joins where one side is "main"
  let joins = map.joins;
  if (typeof joins === "string") {
    try {
      joins = JSON.parse(joins);
    } catch {
      joins = null;
    }
  }

  const normalisedJoins = [];
  if (Array.isArray(joins)) {
    for (const j of joins) {
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

        if (!key) continue;

        const joined = idx.map.get(key);
        if (joined) {
          srcRow = mergeJoinedRow(srcRow, joined);
        } else {
        }
      }
    }

    const out = applyColumnMappingsToRow({ mappings, sourceRow: srcRow });
    out.row_no = r.rowNo;

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

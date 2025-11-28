const db = require("@/db/database");

const { logger } = require("@/helpers/logger");
const { slog } = require("@/v2/ptrs/services/ptrs.service");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { getDatasetSample } = require("@/v2/ptrs/services/data.ptrs.service");

module.exports = {
  getColumnMap,
  getImportSample,
  saveColumnMap,
};

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
    console.log("getColumnMap", map);
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
    await t.rollback();
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
    await t.rollback();
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

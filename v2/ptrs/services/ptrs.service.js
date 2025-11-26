const db = require("@/db/database");
const csv = require("fast-csv");
const path = require("path");
const { Readable } = require("stream");
const fs = require("fs");

const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

const { Worker } = require("worker_threads");

/**
 * Return a small sample of rows from a raw dataset file plus headers and total row count.
 * Handles CSV (and Excel that was already normalised to CSV on upload).
 */
async function getDatasetSample({
  customerId,
  datasetId,
  limit = 10,
  offset = 0,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!datasetId) throw new Error("datasetId is required");

  let row;
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    row = await db.PtrsDataset.findOne({
      where: { id: datasetId, customerId },
      raw: true,
      transaction: t,
    });
    await t.commit();
  } catch (err) {
    await t.rollback();
    throw err;
  }

  if (!row) {
    const e = new Error("Dataset not found");
    e.statusCode = 404;
    throw e;
  }

  const storageRef = row.storageRef;
  if (!storageRef || !fs.existsSync(storageRef)) {
    const e = new Error("Dataset file missing");
    e.statusCode = 404;
    throw e;
  }

  function readFirstLine(filePath) {
    const fd = fs.openSync(filePath, "r");
    try {
      const CHUNK = 64 * 1024;
      const buf = Buffer.alloc(CHUNK);
      let acc = "";
      let pos = 0;
      while (true) {
        const bytes = fs.readSync(fd, buf, 0, CHUNK, pos);
        if (!bytes) break;
        const chunk = buf.toString("utf8", 0, bytes);
        const nl = chunk.search(/\r?\n/);
        if (nl >= 0) {
          acc += chunk.slice(0, nl);
          break;
        }
        acc += chunk;
        pos += bytes;
        if (pos > 1024 * 1024) break; // safety cap at 1MB
      }
      return acc.replace(/^\uFEFF/, "");
    } finally {
      fs.closeSync(fd);
    }
  }

  function splitCsvLine(line) {
    const out = [];
    let cur = "";
    let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQ && line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQ = !inQ;
        }
      } else if (ch === "," && !inQ) {
        out.push(cur);
        cur = "";
      } else if ((ch === "\r" || ch === "\n") && !inQ) {
        /* skip */
      } else {
        cur += ch;
      }
    }
    out.push(cur);
    return out.map((s) => String(s || "").trim());
  }

  function dedupeHeaders(rawHeaders) {
    const seen = new Map();
    return rawHeaders.map((h, i) => {
      const label = h && h.trim().length ? h.trim() : `column_${i + 1}`;
      const n = (seen.get(label) || 0) + 1;
      seen.set(label, n);
      return n === 1 ? label : `${label}_${n}`;
    });
  }

  const headerLine = readFirstLine(storageRef);
  const rawHeaders = splitCsvLine(headerLine);
  const headers = dedupeHeaders(rawHeaders);

  const rows = [];
  let total = 0;

  await new Promise((resolve, reject) => {
    const stream = fs.createReadStream(storageRef);
    const parser = csv
      .parse({
        headers,
        renameHeaders: false,
        trim: true,
        skipLines: 1, // skip original header row
        ignoreEmpty: true,
        strictColumnHandling: false,
        discardUnmappedColumns: true,
      })
      .on("error", reject)
      .on("data", (row) => {
        if (total >= offset && rows.length < limit) rows.push(row);
        total += 1;
      })
      .on("end", () => resolve());

    stream.pipe(parser);
  });

  return { headers, rows, total };
}

// --- Safe logging helpers for service layer ---
function _svcReplacer() {
  const seen = new WeakSet();
  return function (key, value) {
    if (typeof value === "bigint") return value.toString();
    if (value instanceof Set) return Array.from(value);
    if (value instanceof Map) return Object.fromEntries(value);
    if (Buffer.isBuffer?.(value))
      return { __type: "Buffer", length: value.length };
    if (value instanceof Error) {
      return { name: value.name, message: value.message, stack: value.stack };
    }
    if (typeof value === "object" && value !== null) {
      if (seen.has(value)) return "[Circular]";
      seen.add(value);
    }
    return value;
  };
}
function safeMeta(meta) {
  try {
    return JSON.parse(JSON.stringify(meta, _svcReplacer()));
  } catch {
    return { note: "unserializable meta" };
  }
}
const slog = {
  info: (msg, meta) => logger?.info?.(msg, safeMeta(meta)),
  warn: (msg, meta) => logger?.warn?.(msg, safeMeta(meta)),
  error: (msg, meta) => logger?.error?.(msg, safeMeta(meta)),
  debug: (msg, meta) => logger?.debug?.(msg, safeMeta(meta)),
};

// --- identifier helpers (enforce snake_case everywhere for SQL identifiers) ---
function toSnake(str) {
  if (str == null) return "";
  return (
    String(str)
      // replace non-alphanumerics with underscores
      .replace(/[^a-zA-Z0-9]+/g, "_")
      // insert underscores between camelCase boundaries
      .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
      .toLowerCase()
      .replace(/^_+|_+$/g, "")
      .replace(/_{2,}/g, "_")
  );
}

// // Build tolerant JSONB extract that tries snake_case, original, and underscored variants
// function jsonKeyVariants(key) {
//   const original = String(key || "");
//   const snake = toSnake(original);
//   const underscored = original.replace(/\s+/g, "_");
//   // ensure unique order-preserving
//   const seen = new Set();
//   return [snake, original, underscored].filter((k) => {
//     if (seen.has(k)) return false;
//     seen.add(k);
//     return true;
//   });
// }
// function buildCoalesceJsonbExpr(sourceKey) {
//   const variants = jsonKeyVariants(sourceKey).map(
//     (k) => `data->>'${k.replace(/'/g, "''")}'`
//   );
//   return `COALESCE(${variants.join(", ")})`;
// }

// ----- JOIN HELPERS (file-backed datasets) -----
function normalizeJoinKeyValue(v) {
  console.log("[JOIN DEEP] normalizeJoinKeyValue called with:", v);
  if (v == null) return "";
  const out = String(v).trim().toUpperCase();
  // Existing debug log retained for compatibility
  console.log(
    "[JOIN DEBUG] normalizeJoinKeyValue input:",
    v,
    "output:",
    String(v == null ? "" : String(v).trim().toUpperCase())
  );
  console.log("[JOIN DEEP] normalizeJoinKeyValue returning:", out);
  return out;
}

function headerVariants(key) {
  console.log("[JOIN DEEP] headerVariants input:", key);
  const original = String(key || "");
  const snake = toSnake(original);
  const underscored = original.replace(/\s+/g, "_");
  const cased = original.toLowerCase();
  const set = new Set([original, snake, underscored, cased]);
  const result = Array.from(set.values());
  console.log("[JOIN DEEP] headerVariants output:", result);
  return result;
}

function pickFromRowLoose(row, header) {
  console.log(
    "[JOIN DEEP] pickFromRowLoose START header=",
    header,
    "row keys=",
    row ? Object.keys(row) : null
  );
  // Existing debug log retained for compatibility
  console.log(
    "[JOIN DEBUG] pickFromRowLoose header=",
    header,
    "row keys=",
    row ? Object.keys(row) : null
  );
  if (!row || !header) return undefined;
  for (const h of headerVariants(header)) {
    if (row[h] != null) {
      console.log(
        "[JOIN DEEP] pickFromRowLoose MATCH key=",
        h,
        "value=",
        row[h]
      );
      return row[h];
    }
    const kh = Object.keys(row).find(
      (k) => String(k).toLowerCase() === String(h).toLowerCase()
    );
    if (kh && row[kh] != null) {
      console.log(
        "[JOIN DEEP] pickFromRowLoose MATCH key=",
        kh || h,
        "value=",
        row[kh || h]
      );
      return row[kh];
    }
  }
  console.log(
    "[JOIN DEEP] pickFromRowLoose no match found for header=",
    header
  );
  return undefined;
}

async function buildDatasetIndexByRole({
  customerId,
  ptrsId,
  role,
  keyColumn,
}) {
  console.log("[JOIN DEEP] buildDatasetIndexByRole START", {
    customerId,
    ptrsId,
    role,
    keyColumn,
  });

  // 🔐 Ensure RLS customer context is set for this lookup
  const t = await beginTransactionWithCustomerContext(customerId);
  let ds;
  try {
    ds = await db.PtrsDataset.findOne({
      where: { customerId, ptrsId, role },
      raw: true,
      transaction: t,
    });
    await t.commit();
  } catch (err) {
    try {
      await t.rollback();
    } catch (_) {}
    throw err;
  }

  if (!ds) {
    console.log("[JOIN DEEP] buildDatasetIndexByRole NO DATASET", {
      customerId,
      ptrsId,
      role,
    });
    return { map: new Map(), headers: [], rowsIndexed: 0 };
  }

  const storageRef = ds.storageRef;
  if (!storageRef || !fs.existsSync(storageRef)) {
    console.log("[JOIN DEEP] buildDatasetIndexByRole MISSING FILE", {
      customerId,
      ptrsId,
      role,
      storageRef,
      exists: storageRef ? fs.existsSync(storageRef) : false,
    });
    return { map: new Map(), headers: [], rowsIndexed: 0 };
  }

  const index = new Map();
  let headers = [];

  await new Promise((resolve, reject) => {
    let isFirst = true;
    const stream = fs.createReadStream(storageRef);
    const parser = csv
      .parse({ headers: true, trim: true, ignoreEmpty: true })
      .on("error", (err) => {
        console.error(
          "[JOIN DEEP] buildDatasetIndexByRole PARSE ERROR",
          err.message
        );
        reject(err);
      })
      .on("data", (row) => {
        if (isFirst) {
          headers = Object.keys(row || {});
          isFirst = false;
        }
        const rawKey = pickFromRowLoose(row, keyColumn);
        const normKey = normalizeJoinKeyValue(rawKey);
        console.log(
          "[JOIN DEBUG] buildDatasetIndexByRole role=",
          role,
          "keyColumn=",
          keyColumn,
          "rawKey=",
          rawKey,
          "normKey=",
          normKey
        );
        if (!index.has(normKey)) index.set(normKey, row);
      })
      .on("end", () => {
        console.log(
          "[JOIN DEEP] buildDatasetIndexByRole STREAM END",
          "role=",
          role,
          "rowsIndexed=",
          index.size
        );
        resolve();
      });

    stream.pipe(parser);
  });

  console.log(
    "[JOIN DEBUG] buildDatasetIndexByRole complete role=",
    role,
    "rowsIndexed=",
    index.size
  );
  console.log("[JOIN DEEP] buildDatasetIndexByRole END", {
    rowsIndexed: index.size,
    headers,
  });

  return { map: index, headers, rowsIndexed: index.size };
}

function mergeJoinedRow(mainRowData, joinedRow) {
  console.log("[JOIN TRACE] mergeJoinedRow called", {
    mainKeys: mainRowData ? Object.keys(mainRowData) : null,
    joinedKeys: joinedRow ? Object.keys(joinedRow) : null,
  });

  if (!joinedRow) {
    console.log(
      "[JOIN TRACE] mergeJoinedRow: no joined row, returning mainRowData"
    );
    return mainRowData;
  }

  const merged = { ...joinedRow, ...mainRowData };

  console.log("[JOIN TRACE] mergeJoinedRow result keys", {
    mergedKeys: Object.keys(merged),
  });

  return merged;
}

function applyColumnMappingsToRow({ mappings, sourceRow }) {
  console.log(
    "[JOIN DEEP] applyColumnMappingsToRow START sourceRow keys=",
    sourceRow ? Object.keys(sourceRow) : null
  );
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
  // Existing debug log retained for compatibility
  console.log(
    "[JOIN DEBUG] applyColumnMappingsToRow sourceRow keys=",
    sourceRow ? Object.keys(sourceRow) : null,
    "output keys=",
    Object.keys(out)
  );
  console.log(
    "[JOIN DEEP] applyColumnMappingsToRow END output keys=",
    Object.keys(out)
  );
  return out;
}

// // ----- RULE ENGINE HELPERS -----
// function _toNum(v) {
//   if (v == null || v === "") return 0;
//   const s = String(v).replace(/[, ]+/g, "");
//   const n = Number(s);
//   return Number.isFinite(n) ? n : 0;
// }
// function _round(n, dp = 2) {
//   const f = Math.pow(10, dp);
//   return Math.round(n * f) / f;
// }
// function _matches(row, cond) {
//   if (!cond || !cond.field) return false;
//   const a = row[cond.field];
//   const op = String(cond.op || "eq").toLowerCase();
//   switch (op) {
//     case "eq":
//       return String(a) === String(cond.value);
//     case "neq":
//       return String(a) !== String(cond.value);
//     case "in": {
//       const arr = Array.isArray(cond.value) ? cond.value : [cond.value];
//       return arr.map(String).includes(String(a));
//     }
//     case "nin": {
//       const arr = Array.isArray(cond.value) ? cond.value : [cond.value];
//       return !arr.map(String).includes(String(a));
//     }
//     case "gt":
//       return _toNum(a) > _toNum(cond.value);
//     case "gte":
//       return _toNum(a) >= _toNum(cond.value);
//     case "lt":
//       return _toNum(a) < _toNum(cond.value);
//     case "lte":
//       return _toNum(a) <= _toNum(cond.value);
//     case "is_null":
//       return a == null || a === "";
//     case "not_null":
//       return !(a == null || a === "");
//     default:
//       return false;
//   }
// }

// function _applyAction(row, act) {
//   if (!act || !act.op) return;
//   if (act.op === "exclude") {
//     row.exclude = true;
//     row._warnings = Array.isArray(row._warnings) ? row._warnings : [];
//     row._warnings.push(act.reason || "Excluded by rule");
//     return;
//   }
//   if (!act.field) return;
//   const cur = _toNum(row[act.field]);
//   const val = Object.prototype.hasOwnProperty.call(act, "value")
//     ? _toNum(act.value)
//     : act.valueField
//       ? _toNum(row[act.valueField])
//       : 0;
//   let next = cur;
//   switch (act.op) {
//     case "add":
//       next = cur + val;
//       break;
//     case "sub":
//       next = cur - val;
//       break;
//     case "mul":
//       next = cur * val;
//       break;
//     case "div":
//       next = val === 0 ? cur : cur / val;
//       break;
//     case "assign":
//       next = val;
//       break;
//     default:
//       return;
//   }
//   row[act.field] = act.round != null ? _round(next, act.round) : next;
// }

function applyRules(rows, rules = []) {
  const enabled = (rules || []).filter((r) => r && r.enabled !== false);
  const stats = { rulesTried: enabled.length, rowsAffected: 0, actions: 0 };
  if (!Array.isArray(rows) || !rows.length || !enabled.length) {
    return { rows: rows || [], stats };
  }
  for (const row of rows) {
    let touched = false;
    for (const rule of enabled) {
      const conds = Array.isArray(rule.when) ? rule.when : [];
      const ok = conds.every((c) => _matches(row, c));
      if (!ok) continue;
      const actions = Array.isArray(rule.then) ? rule.then : [];
      for (const act of actions) {
        _applyAction(row, act);
        stats.actions++;
        touched = true;
      }
      row._appliedRules = row._appliedRules || [];
      row._appliedRules.push(rule.id || rule.label || "rule");
    }
    if (touched) stats.rowsAffected++;
  }
  return { rows, stats };
}

// let XLSX = null;
// try {
//   XLSX = require("xlsx");
// } catch (e) {
//   if (logger && logger.warn) {
//     logger.warn(
//       "XLSX module not found — install 'xlsx' to enable Excel uploads."
//     );
//   }
// }

function looksLikeXlsx(buffer, mime) {
  if (mime && /spreadsheetml|ms-excel/i.test(mime)) return true;
  if (!buffer || buffer.length < 4) return false;
  // XLSX is a ZIP starting with 'PK\x03\x04'
  return (
    buffer[0] === 0x50 &&
    buffer[1] === 0x4b &&
    buffer[2] === 0x03 &&
    buffer[3] === 0x04
  );
}

function excelBufferToCsv(buffer, { timeoutMs = 15000 } = {}) {
  return new Promise((resolve, reject) => {
    const workerPath = path.resolve(
      __dirname,
      "../workers/xlsxToCsv.worker.js"
    );
    let settled = false;
    const worker = new Worker(workerPath);

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      try {
        worker.terminate();
      } catch {}
      const err = new Error("Excel conversion timed out");
      err.statusCode = 408;
      return reject(err);
    }, timeoutMs);

    worker.on("message", (msg) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (msg && msg.ok) return resolve(msg.csv);
      const err = new Error(msg?.error?.message || "Excel conversion failed");
      err.statusCode = 400;
      return reject(err);
    });

    worker.on("error", (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      err.statusCode = 500;
      return reject(err);
    });

    worker.on("exit", (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (code !== 0) {
        const err = new Error(
          "Excel conversion worker exited with code " + code
        );
        err.statusCode = 500;
        return reject(err);
      }
    });

    // Transfer the underlying ArrayBuffer for zero-copy
    const ab = buffer.buffer.slice(
      buffer.byteOffset,
      buffer.byteOffset + buffer.byteLength
    );
    worker.postMessage({ buffer: ab }, [ab]);
  });
}

async function parseCsvMetaFromStream(stream) {
  // 1) Read full text and normalize
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(typeof chunk === "string" ? chunk : chunk.toString("utf8"));
  }
  let text = chunks.join("");
  text = text.replace(/^\uFEFF/, ""); // strip BOM
  text = text.replace(/^\s*[\r\n]+/, ""); // strip leading blank lines

  // Grab the first line as header row (robust split)
  const firstNewlineIdx = text.search(/\r?\n/);
  const headerLine =
    firstNewlineIdx >= 0 ? text.slice(0, firstNewlineIdx) : text;

  // Minimal CSV header splitter (handles quotes and escaped quotes)
  const splitCsvLine = (line) => {
    const out = [];
    let cur = "";
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQuotes && line[i + 1] === '"') {
          cur += '"';
          i++; // skip escaped quote
        } else {
          inQuotes = !inQuotes;
        }
      } else if (ch === "," && !inQuotes) {
        out.push(cur);
        cur = "";
      } else if ((ch === "\r" || ch === "\n") && !inQuotes) {
        // ignore stray EOL in header
      } else {
        cur += ch;
      }
    }
    out.push(cur);
    return out;
  };

  let rawHeaders = splitCsvLine(headerLine).map((s) => String(s || "").trim());
  if (!rawHeaders.length || rawHeaders.every((h) => h === "")) {
    const err = new Error("CSV appears to have no header row");
    err.statusCode = 400;
    throw err;
  }

  // 2) Deduplicate/repair headers
  const seen = new Map();
  const headersArray = rawHeaders.map((h, i) => {
    const label = h && h.length ? h : `column_${i + 1}`;
    const n = (seen.get(label) || 0) + 1;
    seen.set(label, n);
    return n === 1 ? label : `${label}_${n}`;
  });

  // 3) Second pass: parse the whole CSV with the fixed headers, skipping the first line
  const fixedStream = Readable.from(text);
  return new Promise((resolve, reject) => {
    let rowsCount = 0;
    const nonEmptyByHeader = Object.fromEntries(
      headersArray.map((h) => [h, false])
    );

    fixedStream
      .pipe(
        csv.parse({
          headers: headersArray, // supply our deduped headers
          renameHeaders: false,
          ignoreEmpty: true,
          trim: true,
          strictColumnHandling: false,
          skipLines: 1, // skip the original header row we consumed
          discardUnmappedColumns: true,
        })
      )
      .on("error", (err) => reject(err))
      .on("data", (row) => {
        rowsCount += 1;
        // Track which headers have at least one non-empty value
        for (const [key, val] of Object.entries(row)) {
          if (val != null && String(val).trim() !== "") {
            nonEmptyByHeader[key] = true;
          }
        }
      })
      .on("end", () => {
        // Keep all headers (we already synthesized names for blanks)
        resolve({ headers: headersArray, rowsCount });
      });
  });
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

// /** Controller-friendly wrapper: getMap (normalises JSON-ish fields) */
// async function getMap({ customerId, ptrsId }) {
//   const map = await getColumnMap({ customerId, ptrsId });
//   if (!map) return null;
//   const maybeParse = (v) => {
//     if (v == null || typeof v !== "string") return v;
//     try {
//       return JSON.parse(v);
//     } catch {
//       return v;
//     }
//   };
//   map.extras = maybeParse(map.extras);
//   map.fallbacks = maybeParse(map.fallbacks);
//   map.defaults = maybeParse(map.defaults);
//   map.joins = maybeParse(map.joins);
//   map.rowRules = maybeParse(map.rowRules);
//   return map;
// }

// /** Controller-friendly wrapper: saveMap delegates to saveColumnMap */
// async function saveMap(payload) {
//   return saveColumnMap(payload);
// }

// /** Update only rules-related fields without touching mappings/defaults/joins */
// async function updateRulesOnly({
//   customerId,
//   ptrsId,
//   rowRules = [],
//   crossRowRules = [],
//   userId = null,
// }) {
//   if (!customerId) throw new Error("customerId is required");
//   if (!ptrsId) throw new Error("ptrsId is required");

//   // Load existing column map row
//   const existing = await db.PtrsColumnMap.findOne({
//     where: { customerId, ptrsId },
//   });

//   // Helper to parse JSON/TEXT extras safely
//   const parseMaybe = (v) => {
//     if (v == null) return null;
//     if (typeof v === "string") {
//       try {
//         return JSON.parse(v);
//       } catch {
//         return null;
//       }
//     }
//     if (typeof v === "object") return v;
//     return null;
//   };

//   if (!existing) {
//     // Create a minimal row; keep other config fields null/untouched
//     const row = await db.PtrsColumnMap.create({
//       customerId,
//       ptrsId,
//       // Some schemas set NOT NULL on mappings; use {} to be safe.
//       mappings: {},
//       extras: {
//         __experimentalCrossRowRules: Array.isArray(crossRowRules)
//           ? crossRowRules
//           : [],
//       },
//       fallbacks: null,
//       defaults: null,
//       joins: null,
//       rowRules: Array.isArray(rowRules) ? rowRules : [],
//       createdBy: userId || null,
//       updatedBy: userId || null,
//     });
//     return row.get({ plain: true });
//   }

//   // Merge into existing.extras without clobbering other keys
//   const prevExtras = parseMaybe(existing.extras) || {};
//   const nextExtras = {
//     ...prevExtras,
//     __experimentalCrossRowRules: Array.isArray(crossRowRules)
//       ? crossRowRules
//       : [],
//   };

//   await existing.update({
//     // Only touch rules-related fields
//     rowRules: Array.isArray(rowRules) ? rowRules : [],
//     extras: nextExtras,
//     updatedBy: userId || existing.updatedBy || existing.createdBy || null,
//   });

//   return existing.get({ plain: true });
// }

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

/**
 * Return unified headers and examples across main import + all supporting datasets.
 * Reuses getImportSample for main rows/headers and augments headerMeta with supporting datasets.
 */
async function getUnifiedSample({
  customerId,
  ptrsId,
  limit = 10,
  offset = 0,
}) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    // Base = main only
    const base = await getImportSample({ customerId, ptrsId, limit, offset });
    const headerSet = new Set(base.headers || []);

    // Make headerMeta mutable (sources as Set)
    const headerMeta = {};
    for (const [k, meta] of Object.entries(base.headerMeta || {})) {
      headerMeta[k] = {
        sources: new Set([...(meta.sources || [])]),
        examples: { ...(meta.examples || {}) },
      };
    }

    // Merge supporting dataset headers + examples
    try {
      const dsRows = await db.PtrsDataset.findAll({
        where: { customerId, ptrsId },
        attributes: ["id", "meta", "role"],
        raw: true,
        transaction: t,
      });

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
          const meta = ds.meta || {};
          let dsHeaders = Array.isArray(meta.headers) ? meta.headers : null;
          let sampleRows = null;
          try {
            const sample = await getDatasetSample({
              customerId,
              datasetId: ds.id,
              limit: 5,
              offset: 0,
            });
            dsHeaders =
              dsHeaders && dsHeaders.length ? dsHeaders : sample.headers;
            sampleRows = Array.isArray(sample.rows) ? sample.rows : [];
          } catch (_) {}

          addHeaders(dsHeaders, role);

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
      }
    } catch (e) {
      slog.warn(
        "PTRS v2 getUnifiedSample: failed merging supporting datasets",
        {
          action: "PtrsV2GetUnifiedSampleMergeHeaders",
          customerId,
          ptrsId,
          error: e.message,
        }
      );
    }

    // Finalise: convert Sets to arrays and pick a preferred example
    const unifiedHeaders = Array.from(headerSet.values());
    const finalizedHeaderMeta = {};
    for (const key of Object.keys(headerMeta)) {
      const meta = headerMeta[key];
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

    slog.info("PTRS v2 getUnifiedSample: done", {
      action: "PtrsV2GetUnifiedSample",
      customerId,
      ptrsId,
      rowsReturned: Array.isArray(base.rows) ? base.rows.length : 0,
      total: base.total || 0,
      unifiedHeadersCount: unifiedHeaders.length,
      headerMetaKeys: Object.keys(finalizedHeaderMeta).length,
    });

    await t.commit();
    return {
      rows: base.rows || [],
      total: base.total || 0,
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

/** Fetch one ptrs/upload metadata (tenant-scoped) */
async function getPtrs({ customerId, ptrsId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const row = await db.Ptrs.findOne({
      where: { id: ptrsId, customerId },
      transaction: t,
      raw: true,
    });
    await t.commit();
    return row || null;
  } catch (err) {
    await t.rollback();
    throw err;
  }
}

async function updatePtrs({
  customerId,
  ptrsId,
  currentStep,
  label,
  periodStart,
  periodEnd,
  reportingEntityName,
  profileId,
  status,
  meta,
  userId,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const ptrs = await db.Ptrs.findOne({
      where: { id: ptrsId, customerId },
      transaction: t,
    });

    if (!ptrs) {
      const e = new Error("Ptrs not found");
      e.statusCode = 404;
      throw e;
    }

    const updates = {};

    if (currentStep != null) updates.currentStep = currentStep;
    if (label != null) updates.label = label;
    if (periodStart != null) updates.periodStart = periodStart;
    if (periodEnd != null) updates.periodEnd = periodEnd;
    if (reportingEntityName != null)
      updates.reportingEntityName = reportingEntityName;
    if (profileId != null) updates.profileId = profileId;
    if (status != null) updates.status = status;
    if (meta != null) updates.meta = meta;

    if (Object.keys(updates).length === 0) {
      await t.commit();
      return ptrs.get({ plain: true });
    }

    updates.updatedBy = userId || ptrs.updatedBy || ptrs.createdBy || null;

    await ptrs.update(updates, { transaction: t });
    await t.commit();

    return ptrs.get({ plain: true });
  } catch (err) {
    await t.rollback();
    throw err;
  }
}

/** Fetch one upload (tenant-scoped) */
async function getUpload({ ptrsId, customerId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const upload = await db.PtrsUpload.findOne({
      where: { ptrsId, customerId },
      transaction: t,
      raw: true,
    });
    await t.commit();
    return upload || null;
  } catch (err) {
    await t.rollback();
    throw err;
  }
}

/**
 * Stream a CSV into tbl_ptrs_import_raw as JSONB rows
 * - stream: Readable of CSV
 * - returns rowsInserted (int)
 */
async function importCsvStream({
  customerId,
  ptrsId,
  stream,
  fileMeta = null,
}) {
  console.log("PTRS v2 importCsvStream: begin", {
    action: "PtrsV2ImportCsvStream",
    customerId,
    ptrsId,
    fileMeta,
    stream,
  });
  let rowNo = 0;
  let rowsInserted = 0;

  const BATCH_SIZE = 1000;
  const batch = [];

  // Buffer once so we can synthesise headers
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(typeof chunk === "string" ? chunk : chunk.toString("utf8"));
  }
  let text = chunks.join("");
  text = text.replace(/^\uFEFF/, ""); // strip BOM
  text = text.replace(/^\s*[\r\n]+/, ""); // strip leading blank lines

  // First line = header
  const firstNewlineIdx = text.search(/\r?\n/);
  const headerLine =
    firstNewlineIdx >= 0 ? text.slice(0, firstNewlineIdx) : text;

  // Minimal CSV splitter for one line
  const splitCsvLine = (line) => {
    const out = [];
    let cur = "";
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQuotes && line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (ch === "," && !inQuotes) {
        out.push(cur);
        cur = "";
      } else if ((ch === "\r" || ch === "\n") && !inQuotes) {
        /* ignore */
      } else {
        cur += ch;
      }
    }
    out.push(cur);
    return out;
  };

  const rawHeaders = splitCsvLine(headerLine).map((s) =>
    String(s || "").trim()
  );
  if (!rawHeaders.length) {
    const err = new Error("CSV appears to have no header row");
    err.statusCode = 400;
    throw err;
  }

  // Synthesize blanks + dedupe
  const seen = new Map();
  const headersArray = rawHeaders.map((h, i) => {
    const label = h && h.length ? h : `column_${i + 1}`;
    const n = (seen.get(label) || 0) + 1;
    seen.set(label, n);
    return n === 1 ? label : `${label}_${n}`;
  });

  const fixedStream = Readable.from(text);

  // Begin a customer-scoped transaction for all DB writes (RLS-safe)
  const t = await beginTransactionWithCustomerContext(customerId);

  const flush = async () => {
    if (!batch.length) return;
    try {
      await db.PtrsImportRaw.bulkCreate(batch, {
        validate: false,
        transaction: t,
      });
      rowsInserted += batch.length;
      console.log("Inserted batch, total rowsInserted =", rowsInserted);
    } finally {
      batch.length = 0;
    }
  };

  return new Promise((resolve, reject) => {
    const handleFatal = (err) => {
      // Ensure we rollback the transaction on any fatal error
      t.rollback()
        .catch(() => {})
        .finally(() => {
          reject(err);
        });
    };

    const parser = csv
      .parse({
        headers: headersArray,
        renameHeaders: false,
        ignoreEmpty: true,
        trim: true,
        strictColumnHandling: false,
        skipLines: 1, // skip original header row
        discardUnmappedColumns: true,
      })
      .on("error", (err) => {
        handleFatal(err);
      })
      .on("data", (row) => {
        rowNo += 1;
        batch.push({ customerId, ptrsId, rowNo, data: row, errors: null });
        if (batch.length >= BATCH_SIZE) {
          parser.pause();
          flush()
            .then(() => parser.resume())
            .catch((err) => handleFatal(err));
        }
      })
      .on("end", async () => {
        try {
          await flush();

          // Ensure there is a PtrsUpload record for this run, and update its status/row count.
          const uploadWhere = { customerId, ptrsId };
          const defaults = {
            customerId,
            ptrsId,
            originalName: fileMeta?.originalName || null,
            mimeType: fileMeta?.mimeType || null,
            sizeBytes: fileMeta?.sizeBytes ?? null,
            storagePath: uploadWhere.storagePath || null,
          };

          console.log("About to findOrCreate PtrsUpload");
          const [upload, created] = await db.PtrsUpload.findOrCreate({
            where: uploadWhere,
            defaults,
            transaction: t,
          });

          if (!created) {
            const updatePayload = {
              status: "Ingested",
              rowCount: rowsInserted,
              originalName: fileMeta?.originalName || null,
              mimeType: fileMeta?.mimeType || null,
              sizeBytes: fileMeta?.sizeBytes ?? null,
            };
            await upload.update(updatePayload, { transaction: t });
          }

          await t.commit();
          resolve(rowsInserted);
        } catch (e) {
          handleFatal(e);
        }
      });

    try {
      fixedStream.pipe(parser);
    } catch (e) {
      handleFatal(e);
    }
  });
}

/**
 * Create a raw dataset record and persist the uploaded file to local storage.
 * Returns the created dataset row (plain) including a populated meta block.
 * `buffer` is required (from multer). Role is required.
 */
async function addDataset({
  customerId,
  ptrsId,
  role,
  sourceName = null,
  fileName = null,
  fileSize = null,
  mimeType = null,
  buffer,
  userId = null,
}) {
  const rawRole = typeof role === "string" ? role.trim() : "";
  const normalisedRole = rawRole.toLowerCase();
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!normalisedRole) throw new Error("role is required");
  if (!buffer || !Buffer.isBuffer(buffer)) {
    throw new Error("file buffer is required");
  }

  const t = await beginTransactionWithCustomerContext(customerId);
  let storagePath = null;

  try {
    // Ensure ptrs exists for tenant
    const ptrs = await db.Ptrs.findOne({
      where: { id: ptrsId, customerId },
      transaction: t,
    });
    if (!ptrs) {
      const e = new Error("Ptrs not found");
      e.statusCode = 404;
      throw e;
    }

    // Normalise to CSV if an Excel file is uploaded
    let workBuffer = buffer;
    let workMime = mimeType || null;
    let workExt = (fileName && path.extname(fileName)) || ".csv";

    try {
      const MAX_EXCEL_BYTES = 25 * 1024 * 1024; // 25 MB
      if (looksLikeXlsx(buffer, mimeType)) {
        if (buffer.length > MAX_EXCEL_BYTES) {
          const e = new Error("Excel file too large; please split and retry");
          e.statusCode = 413;
          throw e;
        }
        const csvText = await excelBufferToCsv(buffer, { timeoutMs: 15000 });
        workBuffer = Buffer.from(csvText, "utf8");
        workMime = "text/csv";
        workExt = ".csv";
      }
    } catch (convErr) {
      convErr.statusCode = convErr.statusCode || 400;
      if (logger && logger.error) {
        logger.error("PTRS v2 XLSX->CSV conversion failed", {
          action: "PtrsV2AddDatasetConvert",
          ptrsId,
          customerId,
          error: convErr.message,
        });
      }
      throw convErr;
    }

    // Create DB row first to get dataset id
    const row = await db.PtrsDataset.create(
      {
        customerId,
        ptrsId,
        role: normalisedRole,
        sourceName: sourceName || fileName || null,
        fileName: fileName || null,
        fileSize: Number.isFinite(fileSize)
          ? fileSize
          : workBuffer.length || null,
        mimeType: workMime || mimeType || null,
        storageRef: null,
        rowsCount: null,
        status: "uploaded",
        meta: null,
        createdBy: userId || null,
        updatedBy: userId || null,
      },
      { transaction: t }
    );

    const datasetId = row.id;

    // Persist bytes to local storage
    const baseDir = path.resolve(
      process.cwd(),
      "storage",
      "ptrs_datasets",
      String(customerId),
      String(ptrsId)
    );
    fs.mkdirSync(baseDir, { recursive: true });
    const ext = workExt || ".csv";
    storagePath = path.join(baseDir, `${datasetId}${ext}`);
    fs.writeFileSync(storagePath, workBuffer);

    // Parse headers + count rows
    const { headers, rowsCount } = await parseCsvMetaFromStream(
      Readable.from(workBuffer)
    );
    const meta = { headers, rowsCount };

    await row.update(
      { storageRef: storagePath, rowsCount, meta },
      { transaction: t }
    );

    await t.commit();
    return row.get({ plain: true });
  } catch (err) {
    try {
      await t.rollback();
    } catch {
      // ignore rollback errors
    }
    if (storagePath) {
      try {
        fs.unlinkSync(storagePath);
      } catch {
        // ignore unlink errors
      }
    }
    throw err;
  }
}

/** List datasets attached to a ptrs (tenant-scoped) */
async function listDatasets({ customerId, ptrsId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const rows = await db.PtrsDataset.findAll({
      where: { customerId, ptrsId },
      order: [["createdAt", "DESC"]],
      raw: true,
      transaction: t,
    });

    await t.commit();

    return rows;
  } catch (err) {
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

/** Remove a dataset (deletes DB row and best-effort removes stored file) */
async function removeDataset({ customerId, ptrsId, datasetId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!datasetId) throw new Error("datasetId is required");

  const t = await beginTransactionWithCustomerContext(customerId);
  let storageRef = null;

  try {
    const row = await db.PtrsDataset.findOne({
      where: { id: datasetId, customerId, ptrsId },
      raw: false,
      transaction: t,
    });

    if (!row) {
      const e = new Error("Dataset not found");
      e.statusCode = 404;
      throw e;
    }

    storageRef = row.get("storageRef");

    await row.destroy({ transaction: t });
    await t.commit();
  } catch (err) {
    try {
      await t.rollback();
    } catch {
      // ignore rollback errors
    }
    throw err;
  }

  if (storageRef) {
    try {
      fs.unlinkSync(storageRef);
    } catch (e) {
      logger.info("PTRS v2 removeDataset: could not delete file", {
        action: "PtrsV2RemoveDataset",
        datasetId,
        storageRef,
        error: e.message,
      });
    }
  }

  return { ok: true };
}

/**
 * Create a new PTRS v2 run record (tbl_ptrs).
 * @param {Object} params
 * @param {string} params.customerId
 * @param {string} [params.profileId]
 * @param {string} [params.label]
 * @param {string} [params.periodStart] - YYYY-MM-DD
 * @param {string} [params.periodEnd] - YYYY-MM-DD
 * @param {string} [params.reportingEntityName]
 * @param {string} [params.createdBy]
 */
async function createPtrs(params) {
  const {
    customerId,
    profileId,
    label,
    periodStart,
    periodEnd,
    reportingEntityName,
    createdBy,
  } = params || {};

  if (!customerId) throw new Error("customerId is required");

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const row = await db.Ptrs.create(
      {
        customerId,
        profileId: profileId || null,
        label: label || null,
        periodStart: periodStart || null,
        periodEnd: periodEnd || null,
        reportingEntityName: reportingEntityName || null,
        status: "draft",
        currentStep: "create",
        meta: null,
        createdBy: createdBy || null,
        updatedBy: createdBy || null,
      },
      { transaction: t }
    );

    await t.commit();

    return row.get ? row.get({ plain: true }) : row;
  } catch (err) {
    if (!t.finished) {
      await t.rollback();
    }
    logger.logEvent("error", "Failed to create PTRS v2 record", {
      action: "PtrsV2CreatePtrs",
      customerId,
      profileId: profileId || null,
      label: label || null,
      error: err.message,
    });
    throw err;
  }
}

// /**
//  * Compile and ptrs a preview over staged data (+column map).
//  * Supports simple step kinds: filter | derive | rename.
//  * - filter: { field, op: 'eq|ne|gt|gte|lt|lte|contains|in', value }
//  * - derive: { as, sql }   // SQL snippet referencing logical fields
//  * - rename: { from, to }
//  */
// async function previewTransform({ customerId, ptrsId, steps = [], limit = 50 }) {
//   // Load column map (required to project JSONB -> columns)
//   const mapRow = await getColumnMap({ customerId, ptrsId });
//   if (!mapRow || !mapRow.mappings) {
//     throw new Error("No column map saved for this ptrs");
//   }
//   const mappings = mapRow.mappings || {};

//   // Build projection list from JSONB to SQL columns using mapping types.
//   // Group by logical field so multiple source headers coalesce into a single column.
//   const byField = new Map(); // field_snk -> { type, sources: [] }
//   for (const [sourceHeader, cfg] of Object.entries(mappings)) {
//     const fieldRaw = cfg.field;
//     if (!fieldRaw) continue;
//     const field = toSnake(fieldRaw);
//     const type = (cfg.type || "string").toLowerCase();
//     const existing = byField.get(field);
//     if (existing) {
//       // Optionally promote type if any mapping asks for stronger typing
//       const promote = (prev, next) => {
//         const order = [
//           "string",
//           "number",
//           "numeric",
//           "decimal",
//           "date",
//           "datetime",
//           "timestamp",
//         ];
//         const ip = order.indexOf(prev);
//         const inx = order.indexOf(next);
//         return ip === -1 ? next : inx === -1 ? prev : order[Math.max(ip, inx)];
//       };
//       existing.type = promote(existing.type, type);
//       existing.sources.push(sourceHeader);
//     } else {
//       byField.set(field, { type, sources: [sourceHeader] });
//     }
//   }

//   const projections = [];
//   const logicalFields = new Set(byField.keys());
//   for (const [field, { type, sources }] of byField.entries()) {
//     // Expand each source header into tolerant json key variants (snake, original, underscored).
//     const variants = sources.flatMap((s) => jsonKeyVariants(s));
//     // Build COALESCE(data->>'a', data->>'b', ...)
//     const jsonExprs = variants.map((k) => `data->>'${k.replace(/'/g, "''")}'`);
//     const coalesced = `COALESCE(${jsonExprs.join(", ")})`;
//     let cast = "";
//     const ty = type;
//     if (ty === "number" || ty === "numeric" || ty === "decimal")
//       cast = "::numeric";
//     else if (ty === "date" || ty === "datetime" || ty === "timestamp")
//       cast = "::timestamptz";
//     const expr =
//       ty === "string"
//         ? `${coalesced} AS "${field}"`
//         : `(${coalesced})${cast} AS "${field}"`;
//     projections.push(expr);
//   }

//   if (projections.length === 0) {
//     throw new Error("Column map has no usable field mappings");
//   }

//   console.log("projections: ", projections);

//   const projectedFields = Array.isArray(projections)
//     ? projections.map((p) => p.as || p.key || p)
//     : [];

//   if (logger && logger.info) {
//     logger.info("PTRS v2 getStagePreview: projections", {
//       action: "PtrsV2GetStagePreviewProjections",
//       customerId,
//       ptrsId,
//       projectedCount: projectedFields.length,
//       projectedSample: projectedFields.slice(0, 20),
//     });
//   }

//   // Build WHERE clause and parameters from filter steps
//   const where = [];
//   const params = { customerId, ptrsId, limit };
//   let pIndex = 0;
//   const param = (val) => {
//     const key = `p${pIndex++}`;
//     params[key] = val;
//     return `:${key}`;
//   };

//   const renamePairs = []; // [{from,to}]
//   const deriveExprs = []; // [`<sql> AS "alias"`]

//   for (const step of steps) {
//     if (!step || typeof step !== "object") continue;
//     const { kind, config = {} } = step;
//     if (kind === "filter") {
//       const fieldName = toSnake(config.field);
//       const { op, value } = config;
//       if (!fieldName || !logicalFields.has(fieldName)) continue;
//       switch ((op || "eq").toLowerCase()) {
//         case "eq":
//           where.push(`"${fieldName}" = ${param(value)}`);
//           break;
//         case "ne":
//           where.push(`"${fieldName}" <> ${param(value)}`);
//           break;
//         case "gt":
//           where.push(`"${fieldName}" > ${param(value)}`);
//           break;
//         case "gte":
//           where.push(`"${fieldName}" >= ${param(value)}`);
//           break;
//         case "lt":
//           where.push(`"${fieldName}" < ${param(value)}`);
//           break;
//         case "lte":
//           where.push(`"${fieldName}" <= ${param(value)}`);
//           break;
//         case "contains":
//           where.push(
//             `CAST("${fieldName}" AS text) ILIKE '%' || ${param(String(value))} || '%'`
//           );
//           break;
//         case "in": {
//           const arr = Array.isArray(value) ? value : [value];
//           const placeholders = arr.map((v) => param(v)).join(", ");
//           where.push(`"${fieldName}" IN (${placeholders})`);
//           break;
//         }
//         default:
//           // unknown op -> skip
//           break;
//       }
//     } else if (kind === "rename") {
//       const { from, to } = config || {};
//       const fromSnake = toSnake(from);
//       const toSnakeName = toSnake(to);
//       if (fromSnake && toSnakeName && logicalFields.has(fromSnake)) {
//         renamePairs.push({ from: fromSnake, to: toSnakeName });
//       }
//     } else if (kind === "derive") {
//       const { as, sql } = config || {};
//       if (as && sql) {
//         const asSnake = toSnake(as);
//         deriveExprs.push(`${sql} AS "${asSnake}"`);
//       }
//     }
//   }

//   // Use a transaction for a temp table scope
//   return db.sequelize.transaction(async (t) => {
//     await db.sequelize.query(
//       `SET LOCAL app.current_customer_id = :customerId;`,
//       {
//         transaction: t,
//         replacements: { customerId },
//       }
//     );

//     // Create temp table with projected columns
//     const createTempSql = `
//       CREATE TEMP TABLE tmp_ptrs_preview ON COMMIT DROP AS
//       SELECT "rowNo", "data",
//              ${projections.join(",\n               ")}
//       FROM "tbl_ptrs_stage_row"
//       WHERE "ptrsId" = :ptrsId AND "customerId" = :customerId;
//     `;

//     if (logger && logger.debug) {
//       logger.debug("PTRS v2 getStagePreview: createTempSql", {
//         action: "PtrsV2GetStagePreviewSQL",
//         sql: createTempSql,
//       });
//     }

//     await db.sequelize.query(createTempSql, {
//       transaction: t,
//       replacements: { ptrsId, customerId },
//     });

//     // Build filtered CTE
//     const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

//     // Build final select list (apply renames and derives)
//     const baseCols = Array.from(logicalFields).map((f) => `"${f}"`);
//     // Apply renames by adding aliased duplicates (non-destructive)
//     for (const { from, to } of renamePairs) {
//       baseCols.push(`"${from}" AS "${to}"`);
//     }
//     // Add derives
//     for (const expr of deriveExprs) {
//       baseCols.push(expr);
//     }

//     const countSql = `
//       WITH filtered AS (
//         SELECT "rowNo", ${Array.from(logicalFields)
//           .map((f) => `"${f}"`)
//           .join(", ")}
//         FROM tmp_ptrs_preview
//         ${whereSql}
//       )
//       SELECT COUNT(*)::int AS cnt FROM filtered;
//     `;
//     const [countRows] = await db.sequelize.query(countSql, {
//       type: db.sequelize.QueryTypes.SELECT,
//       transaction: t,
//       replacements: { ...params },
//     });
//     const affectedCount = countRows ? countRows.cnt || 0 : 0;

//     const sampleSql = `
//       WITH filtered AS (
//         SELECT * FROM tmp_ptrs_preview
//         ${whereSql}
//       )
//       SELECT ${baseCols.join(", ")}
//       FROM filtered
//       ORDER BY "rowNo"
//       LIMIT :limit;
//     `;
//     const sample = await db.sequelize.query(sampleSql, {
//       type: db.sequelize.QueryTypes.SELECT,
//       transaction: t,
//       replacements: { ...params },
//     });

//     try {
//       const peek = await db.PtrsStageRow.findOne({
//         where: { customerId, ptrsId },
//         attributes: ["rowNo", "data"],
//         order: [["rowNo", "ASC"]],
//         raw: true,
//       });
//       if (logger && logger.info) {
//         logger.info("PTRS v2 getStagePreview: staged row peek", {
//           action: "PtrsV2GetStagePreviewPeek",
//           rowNo: peek?.rowNo ?? null,
//           dataKeys: peek?.data ? Object.keys(peek.data) : [],
//         });
//       }
//     } catch (e) {
//       if (logger && logger.warn) {
//         logger.warn("PTRS v2 getStagePreview: failed to peek staged row", {
//           action: "PtrsV2GetStagePreviewPeekError",
//           error: e.message,
//         });
//       }
//     }

//     return { sample, affectedCount };
//   });
// }

// async function materialiseCsvDatasetToTemp({
//   sequelize,
//   transaction,
//   storageRef,
//   tempName,
//   selectCols,
// }) {
//   // selectCols: array of { name: 'abn', fromHeader: 'ABN' } etc.
//   const { Readable } = require("stream");
//   const _csv = require("fast-csv");
//   const fs = require("fs");

//   // Normalise selectCols to ensure snake_case everywhere and support multiple fallback headers
//   const normCols = selectCols.map((c) => ({
//     name: toSnake(c.name),
//     fromHeaders: Array.isArray(c.fromHeader) ? c.fromHeader : [c.fromHeader],
//   }));

//   // 1) Create a simple temp table of text columns, drop if exists, ensure ON COMMIT DROP
//   const colsSql = normCols.map((c) => `"${c.name}" text`).join(", ");
//   await sequelize.query(`DROP TABLE IF EXISTS ${tempName};`, { transaction });
//   await sequelize.query(
//     `CREATE TEMP TABLE ${tempName} (${colsSql}) ON COMMIT DROP;`,
//     { transaction }
//   );

//   // 2) Stream rows and batch insert
//   const BATCH = 2000;
//   let batch = [];

//   const flush = async () => {
//     if (!batch.length) return;
//     const placeholders = batch
//       .map(
//         (row, i) =>
//           `(${normCols.map((_, j) => `$${i * normCols.length + j + 1}`).join(",")})`
//       )
//       .join(",");
//     const values = batch.flatMap((row) =>
//       normCols.map((c) => row[c.name] ?? null)
//     );
//     const insertSql = `INSERT INTO ${tempName} (${normCols.map((c) => `"${c.name}"`).join(",")}) VALUES ${placeholders};`;
//     await sequelize.query(insertSql, { bind: values, transaction });
//     batch = [];
//   };

//   await new Promise((resolve, reject) => {
//     // --- Read first line to build a de-duplicated header array ---
//     function readFirstLine(filePath) {
//       const fd = fs.openSync(filePath, "r");
//       try {
//         const CHUNK = 64 * 1024;
//         const buf = Buffer.alloc(CHUNK);
//         let acc = "";
//         let pos = 0;
//         while (true) {
//           const bytes = fs.readSync(fd, buf, 0, CHUNK, pos);
//           if (!bytes) break;
//           const chunk = buf.toString("utf8", 0, bytes);
//           const nl = chunk.search(/\r?\n/);
//           if (nl >= 0) {
//             acc += chunk.slice(0, nl);
//             break;
//           }
//           acc += chunk;
//           pos += bytes;
//           if (pos > 1024 * 1024) break; // safety cap at 1MB
//         }
//         return acc.replace(/^\uFEFF/, "");
//       } finally {
//         fs.closeSync(fd);
//       }
//     }

//     function splitCsvLine(line) {
//       const out = [];
//       let cur = "";
//       let inQ = false;
//       for (let i = 0; i < line.length; i++) {
//         const ch = line[i];
//         if (ch === '"') {
//           if (inQ && line[i + 1] === '"') {
//             cur += '"';
//             i++;
//           } else {
//             inQ = !inQ;
//           }
//         } else if (ch === "," && !inQ) {
//           out.push(cur);
//           cur = "";
//         } else if ((ch === "\r" || ch === "\n") && !inQ) {
//           /* skip */
//         } else {
//           cur += ch;
//         }
//       }
//       out.push(cur);
//       return out.map((s) => String(s || "").trim());
//     }

//     function dedupeHeaders(rawHeaders) {
//       const seen = new Map();
//       return rawHeaders.map((h, i) => {
//         const label = h && h.trim().length ? h.trim() : `column_${i + 1}`;
//         const n = (seen.get(label) || 0) + 1;
//         seen.set(label, n);
//         return n === 1 ? label : `${label}_${n}`;
//       });
//     }

//     const headerLine = readFirstLine(storageRef);
//     const rawHeaders = splitCsvLine(headerLine);
//     const headersArray = dedupeHeaders(rawHeaders);

//     const stream = fs.createReadStream(storageRef);
//     const parser = _csv
//       .parse({
//         headers: headersArray,
//         renameHeaders: false,
//         trim: true,
//         skipLines: 1,
//         ignoreEmpty: true,
//         strictColumnHandling: false,
//         discardUnmappedColumns: true,
//       })
//       .on("error", reject)
//       .on("data", (row) => {
//         const out = {};
//         for (const col of normCols) {
//           let value = null;
//           for (const h of col.fromHeaders) {
//             if (h && row[h] != null && String(row[h]).trim() !== "") {
//               value = row[h];
//               break;
//             }
//           }
//           out[col.name] = value;
//         }
//         batch.push(out);
//         if (batch.length >= BATCH) {
//           parser.pause();
//           flush()
//             .then(() => parser.resume())
//             .catch(reject);
//         }
//       })
//       .on("end", async () => {
//         try {
//           await flush();
//           resolve();
//         } catch (e) {
//           reject(e);
//         }
//       });

//     stream.pipe(parser);
//   });
// }

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

  console.log(
    "[JOIN TRACE] composeMappedRowsForPtrs normalisedJoins",
    normalisedJoins
  );

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
  const mainRows = await db.PtrsImportRaw.findAll({
    where: { customerId, ptrsId },
    order: [["rowNo", "ASC"]],
    limit: Math.min(Number(limit) || 50, 500),
    attributes: ["rowNo", "data"],
    raw: true,
    transaction,
  });

  const composed = [];

  for (const r of mainRows) {
    const base = r.data || {};
    let srcRow = base;

    console.log("[JOIN TRACE] composeMappedRowsForPtrs main row start", {
      rowNo: r.rowNo,
      baseKeys: base ? Object.keys(base) : null,
    });

    // Apply each join in turn, merging any matched supporting-row data
    if (normalisedJoins.length && roleIndexes.size) {
      for (const j of normalisedJoins) {
        const idx = roleIndexes.get(j.otherRole);
        if (!idx || !idx.map || !idx.map.size) {
          console.log(
            "[JOIN TRACE] composeMappedRowsForPtrs join skipped - empty index",
            {
              rowNo: r.rowNo,
              join: j,
              hasIndex: !!idx,
              indexSize: idx && idx.map ? idx.map.size : 0,
            }
          );
          continue;
        }

        const lhsVal = pickFromRowLoose(base, j.mainColumn);
        const key = normalizeJoinKeyValue(lhsVal);

        console.log("[JOIN TRACE] composeMappedRowsForPtrs join lookup", {
          rowNo: r.rowNo,
          join: j,
          lhsVal,
          key,
          hasKey: key ? idx.map.has(key) : false,
        });

        if (!key) continue;

        const joined = idx.map.get(key);
        if (joined) {
          console.log("[JOIN TRACE] composeMappedRowsForPtrs join hit", {
            rowNo: r.rowNo,
            join: j,
            joinedKeys: Object.keys(joined),
          });
          srcRow = mergeJoinedRow(srcRow, joined);
        } else {
          console.log("[JOIN TRACE] composeMappedRowsForPtrs join miss", {
            rowNo: r.rowNo,
            join: j,
          });
        }
      }
    }

    const out = applyColumnMappingsToRow({ mappings, sourceRow: srcRow });
    out.row_no = r.rowNo;

    console.log("[JOIN TRACE] composeMappedRowsForPtrs mapped row", {
      rowNo: r.rowNo,
      outputKeys: Object.keys(out),
    });

    composed.push(out);
  }

  const headers = Array.from(
    new Set(composed.flatMap((row) => Object.keys(row)))
  );

  return { rows: composed, headers };
}

/**
 * Stage data for a ptrs. Reuses previewTransform pipeline to project/optionally filter, then
 * (when persist=true) writes rows into tbl_ptrs_stage_row and updates ptrs status.
 * Returns { sample, affectedCount, persistedCount? }.
 */
async function stagePtrs({
  customerId,
  ptrsId,
  steps = [],
  persist = false,
  limit = 50,
  userId,
  profileId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const started = Date.now();

  // 1) Compose mapped rows for this ptrs (import + joins + column map)
  const { rows: baseRows } = await composeMappedRowsForPtrs({
    customerId,
    ptrsId,
    limit,
  });

  // 2) Apply row-level rules (if any) independently of preview
  let rows = baseRows;
  let rulesStats = null;

  try {
    let rowRules = null;
    try {
      const mapRow = await getColumnMap({ customerId, ptrsId });
      rowRules = mapRow && mapRow.rowRules != null ? mapRow.rowRules : null;
      if (typeof rowRules === "string") {
        try {
          rowRules = JSON.parse(rowRules);
        } catch {
          rowRules = null;
        }
      }
    } catch (_) {
      rowRules = null;
    }

    const rulesResult = applyRules(
      rows,
      Array.isArray(rowRules) ? rowRules : []
    );
    rows = rulesResult.rows || rows;
    rulesStats = rulesResult.stats || null;
  } catch (err) {
    slog.warn("PTRS v2 stagePtrs: failed to apply row rules", {
      action: "PtrsV2StagePtrsApplyRules",
      customerId,
      ptrsId,
      error: err.message,
    });
  }

  // 3) Persist into tbl_ptrs_stage_row if requested
  if (persist) {
    const basePayload = rows.map((r) => {
      const rowNoVal = Number(r?.row_no ?? r?.rowNo ?? 0) || 0;
      const dataObj =
        r && typeof r === "object" && Object.keys(r).length
          ? r
          : { _warning: "⚠️ No mapped data for this row" };

      return {
        customerId: String(customerId),
        ptrsId: String(ptrsId),
        rowNo: rowNoVal,
        data: dataObj,
        errors: null,
        standard: null,
        custom: null,
        meta: {
          _stage: "ptrs.v2.stagePtrs",
          at: new Date().toISOString(),
          rules: {
            applied: Array.isArray(r._appliedRules) ? r._appliedRules : [],
            exclude: !!r.exclude,
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
      for (const key of ["data", "errors", "standard", "custom", "meta"]) {
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
          p?.data?._warning ||
            p?.errors?._warning ||
            p?.standard?._warning ||
            p?.custom?._warning ||
            p?.meta?._warning
        );
        const hasEmpty =
          isEmptyPlain(p?.data) ||
          isEmptyPlain(p?.errors) ||
          isEmptyPlain(p?.standard) ||
          isEmptyPlain(p?.custom) ||
          isEmptyPlain(p?.meta);
        return hasWarn || hasEmpty;
      })
      .slice(0, 3)
      .map((p) => ({
        rowNo: p.rowNo,
        dataKeys: p.data ? Object.keys(p.data) : null,
        hasWarning: Boolean(
          p?.data?._warning ||
            p?.errors?._warning ||
            p?.standard?._warning ||
            p?.custom?._warning ||
            p?.meta?._warning
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

    await db.PtrsStageRow.destroy({ where: { customerId, ptrsId } });
    if (safePayload.length) {
      await db.PtrsStageRow.bulkCreate(safePayload, {
        validate: false,
        returning: false,
      });
    }
  }

  const tookMs = Date.now() - started;
  return {
    rowsIn: rows.length,
    rowsOut: rows.length,
    tookMs,
    sample: rows[0] || null,
    stats: { rules: rulesStats },
  };
}

/**
 * List ptrs for a tenant. Always returns full PTRS rows for the customer.
 */
async function listPtrs({ customerId }) {
  if (!customerId) throw new Error("customerId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const ptrs = await db.Ptrs.findAll({
      where: { customerId },
      order: [["createdAt", "DESC"]],
      raw: true,
      transaction: t,
    });

    await t.commit();
    return ptrs;
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch {
        /* ignore rollback errors */
      }
    }
    throw err;
  }
}

/**
 * List ptrs for a tenant that have an associated column map.
 * Returns full PTRS rows filtered to only those with a saved map.
 */
async function listPtrsWithMap({ customerId }) {
  if (!customerId) throw new Error("customerId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // Fetch PTRS runs from tbl_ptrs
    const ptrs = await db.Ptrs.findAll({
      where: { customerId },
      order: [["createdAt", "DESC"]],
      raw: true,
      transaction: t,
    });

    if (!ptrs.length) {
      await t.commit();
      return [];
    }

    const ptrsIds = ptrs.map((r) => r.id);

    const maps = await db.PtrsColumnMap.findAll({
      where: { customerId, ptrsId: ptrsIds },
      attributes: ["ptrsId"],
      raw: true,
      transaction: t,
    });

    const mappedSet = new Set(maps.map((m) => m.ptrsId));
    const filtered = ptrs.filter((r) => mappedSet.has(r.id));

    await t.commit();
    return filtered;
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch {
        /* ignore rollback errors */
      }
    }
    throw err;
  }
}

/**
 * Returns a preview of staged data using the current column map and step pipeline,
 * but previews directly from the staged table using snake_case logical fields.
 */
async function getStagePreview({ customerId, ptrsId, limit = 50 }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const { rows: composed, headers } = await composeMappedRowsForPtrs({
      customerId,
      ptrsId,
      limit,
      transaction: t,
    });

    // Apply row rules (if configured) for preview purposes
    let rowRules = null;
    try {
      const mapRow = await getColumnMap({
        customerId,
        ptrsId,
        transaction: t,
      });
      console.log("mapRow: ", mapRow);
      rowRules = mapRow && mapRow.rowRules != null ? mapRow.rowRules : null;
      if (typeof rowRules === "string") {
        try {
          rowRules = JSON.parse(rowRules);
        } catch {
          rowRules = null;
        }
      }
      console.log("rowRules: ", rowRules);
    } catch (_) {
      rowRules = null;
    }

    const rulesResult = applyRules(
      composed,
      Array.isArray(rowRules) ? rowRules : []
    );
    console.log("rulesResult: ", rulesResult);
    const rowsAfterRules = rulesResult.rows || composed;
    console.log("rowsAfterRules: ", rowsAfterRules);
    const rulesStats = rulesResult.stats || null;

    await t.commit();
    console.log("headers: ", headers);
    console.log("rowsAfterRules: ", rowsAfterRules);
    console.log("rules: ", rulesStats);
    return {
      headers,
      rows: rowsAfterRules,
      stats: { rules: rulesStats },
    };
  } catch (err) {
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

// async function getRulesPreview({
//   customerId,
//   ptrsId,
//   limit = 50,
//   profileId = null,
// }) {
//   if (!customerId) throw new Error("customerId is required");
//   if (!ptrsId) throw new Error("ptrsId is required");
//   const prev = await getStagePreview({
//     customerId,
//     ptrsId,
//     steps: [],
//     limit,
//     profileId,
//   });
//   return {
//     headers: prev.headers || [],
//     rows: prev.rows || [],
//     stats: prev.stats || null,
//   };
// }

// async function applyRulesAndPersist({
//   customerId,
//   ptrsId,
//   profileId = null,
//   limit = 5000,
// }) {
//   if (!customerId) throw new Error("customerId is required");
//   if (!ptrsId) throw new Error("ptrsId is required");
//   const prev = await getStagePreview({
//     customerId,
//     ptrsId,
//     steps: [],
//     limit,
//     profileId,
//   });
//   const rows = prev.rows || [];

//   const payload = rows.map((r) => ({
//     customerId,
//     ptrsId,
//     rowNo: r.row_no || r.rowNo || null,
//     data:
//       r && Object.keys(r || {}).length
//         ? r
//         : { _warning: "⚠️ No mapped data for this row" },
//     meta: {
//       rules: {
//         applied: Array.isArray(r._appliedRules) ? r._appliedRules : [],
//         exclude: !!r.exclude,
//         at: new Date().toISOString(),
//       },
//     },
//   }));

//   await db.PtrsStageRow.destroy({ where: { customerId, ptrsId } });
//   if (payload.length) {
//     await db.PtrsStageRow.bulkCreate(payload, {
//       validate: false,
//       returning: false,
//     });
//   }
//   return { ok: true, stats: prev.stats || null, persisted: payload.length };
// }

// in v2/ptrs/services/ptrs.service.js
async function listProfiles(customerId) {
  console.log("customerId: ", customerId);
  if (!customerId) throw new Error("customerId is required");
  return await db.PtrsProfile.findAll({ where: { customerId } });
}

// /** ---------------------------
//  * Profiles CRUD
//  * ---------------------------*/

// /** Create a PTRS profile (tenant-scoped) */
// async function createProfile({ customerId, payload = {}, userId = null }) {
//   if (!customerId) throw new Error("customerId is required");
//   const data = {
//     customerId,
//     // Only persist known-safe top-level fields; everything else into config
//     profileId: payload.profileId || payload.code || null,
//     name: payload.name || payload.label || null,
//     description: payload.description || null,
//     isDefault: payload.isDefault === true,
//     config: payload.config || null,
//     createdBy: userId || null,
//     updatedBy: userId || null,
//   };
//   // Drop undefined keys
//   Object.keys(data).forEach((k) => data[k] === undefined && delete data[k]);
//   const row = await db.PtrsProfile.create(data);
//   return row.get({ plain: true });
// }

// /** Read a single profile (tenant-scoped) */
// async function getProfile({ customerId, profileId }) {
//   if (!customerId) throw new Error("customerId is required");
//   if (!profileId) throw new Error("profileId is required");
//   const row = await db.PtrsProfile.findOne({
//     where: { customerId, id: profileId },
//     raw: true,
//   });
//   return row || null;
// }

// /** Update a profile (tenant-scoped, partial update) */
// async function updateProfile({
//   customerId,
//   profileId,
//   payload = {},
//   userId = null,
// }) {
//   if (!customerId) throw new Error("customerId is required");
//   if (!profileId) throw new Error("profileId is required");
//   const row = await db.PtrsProfile.findOne({
//     where: { customerId, id: profileId },
//   });
//   if (!row) {
//     const e = new Error("Profile not found");
//     e.statusCode = 404;
//     throw e;
//   }
//   const patch = {
//     profileId: payload.profileId ?? payload.code,
//     name: payload.name ?? payload.label,
//     description: payload.description,
//     isDefault:
//       typeof payload.isDefault === "boolean" ? payload.isDefault : undefined,
//     config: payload.config,
//     updatedBy: userId || row.updatedBy || row.createdBy || null,
//   };
//   Object.keys(patch).forEach((k) => patch[k] === undefined && delete patch[k]);
//   await row.update(patch);
//   return row.get({ plain: true });
// }

// /** Delete a profile (tenant-scoped) */
// async function deleteProfile({ customerId, profileId }) {
//   if (!customerId) throw new Error("customerId is required");
//   if (!profileId) throw new Error("profileId is required");
//   const row = await db.PtrsProfile.findOne({
//     where: { customerId, id: profileId },
//   });
//   if (!row) {
//     const e = new Error("Profile not found");
//     e.statusCode = 404;
//     throw e;
//   }
//   await row.destroy();
//   return { ok: true };
// }

function mergeBlueprintLayers(base, overlay) {
  if (!overlay || typeof overlay !== "object") return base || {};
  const out = { ...(base || {}) };

  // Shallow-merge known hook areas
  if (overlay.synonyms) {
    out.synonyms = { ...(out.synonyms || {}), ...(overlay.synonyms || {}) };
  }
  if (overlay.fallbacks) {
    out.fallbacks = { ...(out.fallbacks || {}), ...(overlay.fallbacks || {}) };
  }
  if (Array.isArray(overlay.rowRules)) {
    out.rowRules = [...(out.rowRules || []), ...overlay.rowRules];
  }
  if (overlay.joins) {
    out.joins = { ...(out.joins || {}), ...(overlay.joins || {}) };
  }

  // Allow overlays to replace other top-level keys if explicitly provided
  for (const [key, value] of Object.entries(overlay)) {
    if (["synonyms", "fallbacks", "rowRules", "joins"].includes(key)) continue;
    if (value !== undefined) {
      out[key] = value;
    }
  }

  return out;
}

/**
 * Load PTRS calculation blueprint from DB, optionally overlaying profile- and customer-level overrides.
 *
 * DB expectations:
 * - Base blueprint row has id "ptrsCalculationBlueprint".
 * - Profile-specific rows use id = profileId (e.g. "veolia").
 * - Customer overrides live in tbl_ptrs_blueprint_override.
 */
async function getBlueprint({ customerId = null, profileId = null } = {}) {
  // No customerId => no RLS txn needed; just read base + optional profile
  if (!customerId) {
    const baseRow = await db.PtrsBlueprint.findByPk(
      "ptrsCalculationBlueprint",
      { raw: true }
    );

    if (!baseRow || !baseRow.json) {
      const err = new Error(
        "PTRS base blueprint not found in DB (id=ptrsCalculationBlueprint). Seed tbl_ptrs_blueprint before calling getBlueprint."
      );
      err.statusCode = 500;
      throw err;
    }

    let merged = baseRow.json || {};

    if (profileId) {
      const profileRow = await db.PtrsBlueprint.findByPk(profileId, {
        raw: true,
      });
      if (profileRow && profileRow.json) {
        merged = mergeBlueprintLayers(merged, profileRow.json);
      }
    }

    // No customer overrides without a customerId
    return merged;
  }

  // 🔐 RLS-safe path: customer-scoped transaction
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // 1) Base blueprint (required)
    const baseRow = await db.PtrsBlueprint.findByPk(
      "ptrsCalculationBlueprint",
      { raw: true, transaction: t }
    );

    if (!baseRow || !baseRow.json) {
      const err = new Error(
        "PTRS base blueprint not found in DB (id=ptrsCalculationBlueprint). Seed tbl_ptrs_blueprint before calling getBlueprint."
      );
      err.statusCode = 500;
      throw err;
    }

    let merged = baseRow.json || {};

    // 2) Profile-level overlay (optional)
    if (profileId) {
      const profileRow = await db.PtrsBlueprint.findByPk(profileId, {
        raw: true,
        transaction: t,
      });
      if (profileRow && profileRow.json) {
        merged = mergeBlueprintLayers(merged, profileRow.json);
      }
    }

    // 3) Customer-level override (optional)
    const overrideRow = await db.PtrsBlueprintOverride.findOne({
      where: {
        customerId,
        profileId: profileId || null,
      },
      raw: true,
      transaction: t,
    });

    if (overrideRow && overrideRow.json) {
      merged = mergeBlueprintLayers(merged, overrideRow.json);
    }

    await t.commit();
    return merged;
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {} // best effort
    }
    throw err;
  }
}

module.exports = {
  createPtrs,
  getUpload,
  importCsvStream,
  getImportSample,
  getUnifiedSample,
  getColumnMap,
  saveColumnMap,
  getBlueprint,
  // getMap,
  //   saveMap,
  //   updateRulesOnly,
  //   previewTransform,
  listPtrs,
  listPtrsWithMap,
  addDataset,
  listDatasets,
  getDatasetSample,
  removeDataset,
  getPtrs,
  updatePtrs,
  getStagePreview,
  stagePtrs,
  listProfiles,
  //   getRulesPreview,
  //   applyRulesAndPersist,
  //   // Profiles CRUD
  //   createProfile,
  //   getProfile,
  //   updateProfile,
  //   deleteProfile,
};

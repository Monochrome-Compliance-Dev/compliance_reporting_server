const db = require("@/db/database");
const csv = require("fast-csv");
const { Readable } = require("stream");
const fs = require("fs");
const crypto = require("crypto");

const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

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

/**
 * Build a stable SHA-256 hash from an arbitrary JS value.
 * - Uses deterministic key ordering to avoid hash drift
 * - Intended for execution/input fingerprinting (NOT passwords)
 */
function buildStableInputHash(input) {
  const seen = new WeakSet();

  const stableStringify = (value) => {
    if (value === null || typeof value !== "object") {
      return JSON.stringify(value);
    }

    if (seen.has(value)) {
      return '"[Circular]"';
    }
    seen.add(value);

    if (Array.isArray(value)) {
      return "[" + value.map((v) => stableStringify(v)).join(",") + "]";
    }

    const keys = Object.keys(value).sort();
    const entries = keys.map(
      (k) => JSON.stringify(k) + ":" + stableStringify(value[k]),
    );
    return "{" + entries.join(",") + "}";
  };

  const canonical = stableStringify(input);
  return crypto.createHash("sha256").update(canonical).digest("hex");
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
  if (v == null) return "";
  const out = String(v).trim().toUpperCase();
  // Existing debug log retained for compatibility
  return out;
}

function mergeJoinedRow(mainRowData, joinedRow) {
  if (!joinedRow) {
    return mainRowData;
  }

  const merged = { ...joinedRow, ...mainRowData };

  return merged;
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

// /** Controller-friendly wrapper: saveMap delegates to saveColumnMap */
// async function saveMap(payload) {
//   return saveColumnMap(payload);
// }

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

    updates.updatedBy = userId || null;

    await ptrs.update(updates, { transaction: t });
    await t.commit();

    return ptrs.get({ plain: true });
  } catch (err) {
    await t.rollback();
    throw err;
  }
}

/**
 * Create a PTRS execution run (tenant-scoped)
 * If `transaction` is supplied, it will be used (and NOT committed/rolled back here).
 *
 * NOTE: Strict by design — execution runs require profileId + inputHash.
 */
async function createExecutionRun({
  customerId,
  ptrsId,
  profileId,
  step,
  inputHash,
  status = "pending",
  startedAt = null,
  createdBy = null,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!step) throw new Error("step is required");
  if (!inputHash) throw new Error("inputHash is required");

  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const ownsTx = !transaction;

  try {
    const row = await db.PtrsExecutionRun.create(
      {
        customerId,
        ptrsId,
        profileId,
        step,
        inputHash,
        status,
        startedAt: startedAt || new Date(),
        createdBy,
        updatedBy: createdBy,
      },
      { transaction: t },
    );

    if (ownsTx) await t.commit();
    return row.get({ plain: true });
  } catch (err) {
    if (ownsTx && !t.finished) await t.rollback();
    throw err;
  }
}

/**
 * Fetch the most recent execution run for a PTRS + step
 * If `transaction` is supplied, it will be used (and NOT committed/rolled back here).
 */
async function getLatestExecutionRun({
  customerId,
  ptrsId,
  step,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!step) throw new Error("step is required");

  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const ownsTx = !transaction;

  try {
    const row = await db.PtrsExecutionRun.findOne({
      where: { customerId, ptrsId, step },
      order: [
        ["startedAt", "DESC"],
        ["id", "DESC"],
      ],
      raw: true,
      transaction: t,
    });

    if (ownsTx) await t.commit();
    return row || null;
  } catch (err) {
    if (ownsTx && !t.finished) await t.rollback();
    throw err;
  }
}

/**
 * Update an execution run (status, metrics, completion)
 * If `transaction` is supplied, it will be used (and NOT committed/rolled back here).
 */
async function updateExecutionRun({
  customerId,
  executionRunId,
  status,
  finishedAt,
  rowsIn,
  rowsOut,
  stats,
  errorMessage,
  updatedBy = null,
  transaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!executionRunId) throw new Error("executionRunId is required");

  const t =
    transaction || (await beginTransactionWithCustomerContext(customerId));
  const ownsTx = !transaction;

  try {
    const row = await db.PtrsExecutionRun.findOne({
      where: { id: executionRunId, customerId },
      transaction: t,
    });

    if (!row) {
      const e = new Error("Execution run not found");
      e.statusCode = 404;
      throw e;
    }

    const patch = {};
    if (status != null) patch.status = status;
    if (finishedAt != null) patch.finishedAt = finishedAt;
    if (rowsIn != null) patch.rowsIn = rowsIn;
    if (rowsOut != null) patch.rowsOut = rowsOut;
    if (stats != null) patch.stats = stats;
    if (errorMessage != null) patch.errorMessage = errorMessage;
    if (updatedBy != null) patch.updatedBy = updatedBy;

    await row.update(patch, { transaction: t });

    if (ownsTx) await t.commit();
    return row.get({ plain: true });
  } catch (err) {
    if (ownsTx && !t.finished) await t.rollback();
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
  datasetId = null,
  sourceType = null,
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
    String(s || "").trim(),
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

  // Ensure there is a dataset row for this main upload so we can scope raw rows.
  // If caller provided datasetId, we trust it (strictly) and do not attempt to infer.
  let effectiveDatasetId = datasetId || null;

  // Begin a customer-scoped transaction for all DB writes (RLS-safe)
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    if (!effectiveDatasetId) {
      const originalName = fileMeta?.originalName || null;
      const displayName = originalName || "Main input";

      const ds = await db.PtrsDataset.create(
        {
          customerId,
          ptrsId,
          role: "main",
          sourceType: sourceType || "csv",
          fileName: displayName,
          storageRef: null,
          rowsCount: null,
          status: "uploaded",
          meta: {
            headers: headersArray,
            source: "csv",
            originalName,
          },
          createdBy: null,
          updatedBy: null,
        },
        { transaction: t },
      );

      effectiveDatasetId = ds.id;
    }
  } catch (e) {
    // If we fail to create a dataset row, abort loudly.
    await t.rollback().catch(() => {});
    throw e;
  }

  const fixedStream = Readable.from(text);

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
      (t.finished ? Promise.resolve() : t.rollback())
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
        batch.push({
          customerId,
          ptrsId,
          datasetId: effectiveDatasetId,
          rowNo,
          data: row,
          errors: null,
        });
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

          // Update dataset row stats for the main upload
          if (effectiveDatasetId) {
            const ds = await db.PtrsDataset.findOne({
              where: { id: effectiveDatasetId, customerId, ptrsId },
              transaction: t,
            });
            if (ds) {
              const currentMeta = ds.get("meta") || {};
              await ds.update(
                {
                  rowsCount: rowsInserted,
                  meta: {
                    ...currentMeta,
                    headers: headersArray,
                    rowsCount: rowsInserted,
                    updatedAt: new Date().toISOString(),
                  },
                },
                { transaction: t },
              );
            }
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
 * Create a new PTRS v2 run record (tbl_ptrs).
 * @param {Object} params
 * @param {string} params.customerId
 * @param {string} [params.profileId]
 * @param {string} [params.label]
 * @param {string} [params.periodStart] - YYYY-MM-DD
 * @param {string} [params.periodEnd] - YYYY-MM-DD
 * @param {string} [params.reportingEntityName]
 * @param {Object} [params.meta]
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
    meta,
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
        meta: meta && typeof meta === "object" ? meta : null,
        createdBy: createdBy || null,
        updatedBy: createdBy || null,
      },
      { transaction: t },
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
// will need beginTransactionWithCustomerContext
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

function hasRuleApplied(meta, ruleId) {
  if (!ruleId) return false;
  if (!meta || !meta.rules || !Array.isArray(meta.rules.applied)) return false;
  return meta.rules.applied.includes(ruleId);
}

function markRuleApplied(meta, ruleId) {
  if (!ruleId) return meta || {};
  const nextMeta = ensureRulesMeta(meta);
  const applied = nextMeta.rules.applied;

  if (!applied.includes(ruleId)) {
    applied.push(ruleId);
  }

  return nextMeta;
}

// in v2/ptrs/services/ptrs.service.js
async function listProfiles(customerId) {
  console.log("customerId: ", customerId);
  if (!customerId) throw new Error("customerId is required");
  // will need beginTransactionWithCustomerContext
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
// will need beginTransactionWithCustomerContext
//   const row = await db.PtrsProfile.create(data);
//   return row.get({ plain: true });
// }

// /** Read a single profile (tenant-scoped) */
// async function getProfile({ customerId, profileId }) {
//   if (!customerId) throw new Error("customerId is required");
//   if (!profileId) throw new Error("profileId is required");
// will need beginTransactionWithCustomerContext
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
// will need beginTransactionWithCustomerContext
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
// will need beginTransactionWithCustomerContext
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
    // will need beginTransactionWithCustomerContext
    const baseRow = await db.PtrsBlueprint.findByPk(
      "ptrsCalculationBlueprint",
      { raw: true },
    );

    if (!baseRow || !baseRow.json) {
      const err = new Error(
        "PTRS base blueprint not found in DB (id=ptrsCalculationBlueprint). Seed tbl_ptrs_blueprint before calling getBlueprint.",
      );
      err.statusCode = 500;
      throw err;
    }

    let merged = baseRow.json || {};

    if (profileId) {
      // will need beginTransactionWithCustomerContext
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
      { raw: true, transaction: t },
    );

    if (!baseRow || !baseRow.json) {
      const err = new Error(
        "PTRS base blueprint not found in DB (id=ptrsCalculationBlueprint). Seed tbl_ptrs_blueprint before calling getBlueprint.",
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
  safeMeta,
  slog,
  toSnake,
  buildStableInputHash,
  mergeJoinedRow,
  normalizeJoinKeyValue,
  createPtrs,
  getUpload,
  importCsvStream,
  getBlueprint,
  //   saveMap,
  //   previewTransform,
  listPtrs,
  listPtrsWithMap,
  getPtrs,
  updatePtrs,
  createExecutionRun,
  getLatestExecutionRun,
  updateExecutionRun,
  listProfiles,
  //   // Profiles CRUD
  //   createProfile,
  //   getProfile,
  //   updateProfile,
  //   deleteProfile,
};

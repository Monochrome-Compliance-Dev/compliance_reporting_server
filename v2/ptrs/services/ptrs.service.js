const { logger } = require("@/helpers/logger");
const db = require("@/db/database");
const csv = require("fast-csv");
const fs = require("fs");
const path = require("path");
const { Readable } = require("stream");

const { Worker } = require("worker_threads");

let XLSX = null;
try {
  XLSX = require("xlsx");
} catch (e) {
  if (logger && logger.warn) {
    logger.warn(
      "XLSX module not found — install 'xlsx' to enable Excel uploads."
    );
  }
}

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

  // 2) Deduplicate/repair headers: "", "New value", "New value" => "column_1","New value","New value_2"
  const seen = new Map();
  const headersArray = rawHeaders.map((h) => {
    let base = h || "column";
    let count = seen.get(base) || 0;
    seen.set(base, count + 1);
    if (count === 0) return base;
    return `${base}_${count + 1}`;
  });

  // 3) Second pass: parse the whole CSV with the fixed headers, skipping the first line
  const fixedStream = Readable.from(text);
  return new Promise((resolve, reject) => {
    let rowsCount = 0;
    fixedStream
      .pipe(
        csv.parse({
          headers: headersArray, // supply our deduped headers
          renameHeaders: false, // not renaming; we're providing the final headers
          ignoreEmpty: true,
          trim: true,
          strictColumnHandling: false,
          skipLines: 1, // skip the original header row we consumed
        })
      )
      .on("error", (err) => reject(err))
      .on("data", () => {
        rowsCount += 1;
      })
      .on("end", () => resolve({ headers: headersArray, rowsCount }));
  });
}

/** Get column map for a run */
async function getColumnMap({ customerId, runId }) {
  return db.PtrsColumnMap.findOne({
    where: { customerId, runId },
    raw: true,
  });
}

/** Upsert column map for a run */
async function saveColumnMap({
  customerId,
  runId,
  mappings,
  extras = null,
  fallbacks = null,
  defaults = null,
  joins = null,
  rowRules = null,
  profileId = null,
  userId,
}) {
  const existing = await db.PtrsColumnMap.findOne({
    where: { customerId, runId },
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
    await existing.update({
      ...payload,
      updatedBy: userId || existing.updatedBy || existing.createdBy || null,
    });
    return existing.get({ plain: true });
  }

  const row = await db.PtrsColumnMap.create({
    customerId,
    runId,
    ...payload,
    createdBy: userId || null,
    updatedBy: userId || null,
  });
  return row.get({ plain: true });
}

/**
 * Return a small window of staged rows plus count and inferred headers.
 */
async function getImportSample({ customerId, runId, limit = 10, offset = 0 }) {
  // rows
  const rows = await db.PtrsImportRaw.findAll({
    where: { customerId, runId },
    order: [["rowNo", "ASC"]],
    limit,
    offset,
    attributes: ["rowNo", "data"],
    raw: true,
  });

  // total
  const total = await db.PtrsImportRaw.count({
    where: { customerId, runId },
  });

  // headers: scan up to 500 earliest rows to reduce noise
  const headerScan = await db.PtrsImportRaw.findAll({
    where: { customerId, runId },
    order: [["rowNo", "ASC"]],
    limit: 500,
    attributes: ["data"],
    raw: true,
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

  return {
    rows,
    total,
    headers,
  };
}

/** Fetch one upload (tenant-scoped) */
async function getUpload({ runId, customerId }) {
  return db.PtrsUpload.findOne({ where: { id: runId, customerId } });
}

/**
 * Stream a CSV into tbl_ptrs_import_raw as JSONB rows
 * - stream: Readable of CSV
 * - returns rowsInserted (int)
 */
async function importCsvStream({ customerId, runId, stream }) {
  let rowNo = 0;
  let rowsInserted = 0;

  const BATCH_SIZE = 1000;
  const batch = [];

  const flush = async () => {
    if (batch.length === 0) return;
    try {
      await db.PtrsImportRaw.bulkCreate(batch, { validate: false });
      rowsInserted += batch.length;
    } finally {
      batch.length = 0;
    }
  };

  return new Promise((resolve, reject) => {
    const parser = csv
      .parse({
        headers: true,
        renameHeaders: true,
        ignoreEmpty: true,
        trim: true,
        strictColumnHandling: false,
      })
      .on("error", (err) => reject(err))
      .on("data", (row) => {
        rowNo += 1;
        batch.push({
          customerId,
          runId,
          rowNo,
          data: row,
          errors: null,
        });
        if (batch.length >= BATCH_SIZE) {
          parser.pause();
          flush()
            .then(() => parser.resume())
            .catch((err) => reject(err));
        }
      })
      .on("end", async () => {
        try {
          await flush();
          await db.PtrsUpload.update(
            { status: "Ingested", rowCount: rowsInserted },
            { where: { id: runId, customerId } }
          );
          resolve(rowsInserted);
        } catch (e) {
          reject(e);
        }
      });

    try {
      stream.pipe(parser);
    } catch (e) {
      reject(e);
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
  runId,
  role,
  sourceName = null,
  fileName = null,
  fileSize = null,
  mimeType = null,
  buffer,
  userId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!runId) throw new Error("runId is required");
  if (!role) throw new Error("role is required");
  if (!buffer || !Buffer.isBuffer(buffer))
    throw new Error("file buffer is required");

  // Ensure run exists for tenant
  const run = await db.PtrsUpload.findOne({ where: { id: runId, customerId } });
  if (!run) {
    const e = new Error("Run not found");
    e.statusCode = 404;
    throw e;
  }

  // Normalize to CSV if an Excel file is uploaded
  let workBuffer = buffer;
  let workMime = mimeType || null;
  let workExt = (fileName && path.extname(fileName)) || ".csv";
  try {
    const MAX_EXCEL_BYTES = 25 * 1024 * 1024; // 25 MB limit for Excel uploads
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
        runId,
        customerId,
        error: convErr.message,
      });
    }
    throw convErr;
  }

  // Create DB row first to get dataset id
  const row = await db.PtrsRawDataset.create({
    customerId,
    runId,
    role,
    sourceName: sourceName || fileName || null,
    fileName: fileName || null,
    fileSize: Number.isFinite(fileSize) ? fileSize : workBuffer.length || null,
    mimeType: workMime || mimeType || null,
    storageRef: null,
    meta: null,
    createdBy: userId || null,
    updatedBy: userId || null,
  });

  const datasetId = row.id;

  // Persist bytes to local storage (can be swapped for S3 later)
  const baseDir = path.resolve(
    process.cwd(),
    "storage",
    "ptrs_datasets",
    String(customerId),
    String(runId)
  );
  fs.mkdirSync(baseDir, { recursive: true });
  const ext = workExt || ".csv";
  const storagePath = path.join(baseDir, `${datasetId}${ext}`);
  fs.writeFileSync(storagePath, workBuffer);

  // Parse headers + count rows (tolerant to duplicate headers)
  const { headers, rowsCount } = await parseCsvMetaFromStream(
    Readable.from(workBuffer)
  );
  const meta = { headers, rowsCount };

  await row.update({ storageRef: storagePath, meta });
  return row.get({ plain: true });
}

/** List datasets attached to a run (tenant-scoped) */
async function listDatasets({ customerId, runId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!runId) throw new Error("runId is required");
  const rows = await db.PtrsRawDataset.findAll({
    where: { customerId, runId },
    order: [["createdAt", "DESC"]],
    raw: true,
  });
  return rows;
}

/** Remove a dataset (deletes DB row and best-effort removes stored file) */
async function removeDataset({ customerId, runId, datasetId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!runId) throw new Error("runId is required");
  if (!datasetId) throw new Error("datasetId is required");

  const row = await db.PtrsRawDataset.findOne({
    where: { id: datasetId, customerId, runId },
    raw: false,
  });
  if (!row) {
    const e = new Error("Dataset not found");
    e.statusCode = 404;
    throw e;
  }

  const storageRef = row.get("storageRef");
  await row.destroy();

  if (storageRef) {
    try {
      fs.unlinkSync(storageRef);
    } catch (e) {
      // ignore unlink errors; file may have been moved/deleted
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
 * Create a new PTRS v2 upload metadata record.
 * @param {Object} params
 * @param {string} params.customerId
 * @param {string} params.fileName
 * @param {number} [params.fileSize]
 * @param {string} [params.mimeType]
 * @param {string} [params.hash]
 * @param {string} [params.createdBy]
 */
async function createUpload(params) {
  const { customerId, fileName, fileSize, mimeType, hash, createdBy } =
    params || {};

  if (!customerId) throw new Error("customerId is required");
  if (!fileName) throw new Error("fileName is required");

  try {
    const row = await db.PtrsUpload.create({
      customerId,
      fileName,
      fileSize: Number.isFinite(fileSize) ? fileSize : null,
      mimeType: mimeType || null,
      hash: hash || null,
      status: "Uploaded",
      createdBy: createdBy || null,
      updatedBy: createdBy || null,
    });

    return row;
  } catch (err) {
    logger.logEvent("error", "Failed to create PTRS v2 upload", {
      action: "PtrsV2CreateUpload",
      customerId,
      fileName,
      error: err.message,
    });
    throw err;
  }
}

/**
 * Compile and run a preview over staged data (+column map).
 * Supports simple step kinds: filter | derive | rename.
 * - filter: { field, op: 'eq|ne|gt|gte|lt|lte|contains|in', value }
 * - derive: { as, sql }   // SQL snippet referencing logical fields
 * - rename: { from, to }
 */
async function previewTransform({ customerId, runId, steps = [], limit = 50 }) {
  // Load column map (required to project JSONB -> columns)
  const mapRow = await getColumnMap({ customerId, runId });
  if (!mapRow || !mapRow.mappings) {
    throw new Error("No column map saved for this run");
  }
  const mappings = mapRow.mappings || {};

  // Build projection list from JSONB to SQL columns using mapping types
  const projections = [];
  const logicalFields = new Set();
  for (const [source, cfg] of Object.entries(mappings)) {
    const field = cfg.field;
    if (!field) continue;
    logicalFields.add(field);
    const ty = (cfg.type || "string").toLowerCase();
    let cast;
    if (ty === "number" || ty === "numeric" || ty === "decimal")
      cast = "::numeric";
    else if (ty === "date" || ty === "datetime" || ty === "timestamp")
      cast = "::timestamptz";
    else cast = ""; // text by default via ->>
    const expr =
      ty === "string"
        ? `(data->>'${source.replace(/'/g, "''")}') AS "${field}"`
        : `((data->>'${source.replace(/'/g, "''")}')${cast}) AS "${field}"`;
    projections.push(expr);
  }
  if (projections.length === 0) {
    throw new Error("Column map has no usable field mappings");
  }

  // Build WHERE clause and parameters from filter steps
  const where = [];
  const params = { customerId, runId, limit };
  let pIndex = 0;
  const param = (val) => {
    const key = `p${pIndex++}`;
    params[key] = val;
    return `:${key}`;
  };

  const renamePairs = []; // [{from,to}]
  const deriveExprs = []; // [`<sql> AS "alias"`]

  for (const step of steps) {
    if (!step || typeof step !== "object") continue;
    const { kind, config = {} } = step;
    if (kind === "filter") {
      const { field, op, value } = config;
      if (!field || !logicalFields.has(field)) continue;
      switch ((op || "eq").toLowerCase()) {
        case "eq":
          where.push(`"${field}" = ${param(value)}`);
          break;
        case "ne":
          where.push(`"${field}" <> ${param(value)}`);
          break;
        case "gt":
          where.push(`"${field}" > ${param(value)}`);
          break;
        case "gte":
          where.push(`"${field}" >= ${param(value)}`);
          break;
        case "lt":
          where.push(`"${field}" < ${param(value)}`);
          break;
        case "lte":
          where.push(`"${field}" <= ${param(value)}`);
          break;
        case "contains":
          where.push(
            `CAST("${field}" AS text) ILIKE '%' || ${param(String(value))} || '%'`
          );
          break;
        case "in": {
          const arr = Array.isArray(value) ? value : [value];
          const placeholders = arr.map((v) => param(v)).join(", ");
          where.push(`"${field}" IN (${placeholders})`);
          break;
        }
        default:
          // unknown op -> skip
          break;
      }
    } else if (kind === "rename") {
      const { from, to } = config || {};
      if (from && to && logicalFields.has(from)) {
        renamePairs.push({ from, to });
      }
    } else if (kind === "derive") {
      const { as, sql } = config || {};
      if (as && sql) {
        // very light safety: restrict to allowed chars
        deriveExprs.push(`${sql} AS "${as}"`);
      }
    }
  }

  // Use a transaction for a temp table scope
  return db.sequelize.transaction(async (t) => {
    await db.sequelize.query(
      `SET LOCAL app.current_customer_id = :customerId;`,
      {
        transaction: t,
        replacements: { customerId },
      }
    );

    // Create temp table with projected columns
    const createTempSql = `
      CREATE TEMP TABLE tmp_ptrs_preview ON COMMIT DROP AS
      SELECT "rowNo",
             ${projections.join(",\n               ")}
      FROM "tbl_ptrs_import_raw"
      WHERE "runId" = :runId AND "customerId" = :customerId;
    `;
    await db.sequelize.query(createTempSql, {
      transaction: t,
      replacements: { runId, customerId },
    });

    // Build filtered CTE
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    // Build final select list (apply renames and derives)
    const baseCols = Array.from(logicalFields).map((f) => `"${f}"`);
    // Apply renames by adding aliased duplicates (non-destructive)
    for (const { from, to } of renamePairs) {
      baseCols.push(`"${from}" AS "${to}"`);
    }
    // Add derives
    for (const expr of deriveExprs) {
      baseCols.push(expr);
    }

    const countSql = `
      WITH filtered AS (
        SELECT "rowNo", ${Array.from(logicalFields)
          .map((f) => `"${f}"`)
          .join(", ")}
        FROM tmp_ptrs_preview
        ${whereSql}
      )
      SELECT COUNT(*)::int AS cnt FROM filtered;
    `;
    const [countRows] = await db.sequelize.query(countSql, {
      type: db.sequelize.QueryTypes.SELECT,
      transaction: t,
      replacements: { ...params },
    });
    const affectedCount = countRows ? countRows.cnt || 0 : 0;

    const sampleSql = `
      WITH filtered AS (
        SELECT * FROM tmp_ptrs_preview
        ${whereSql}
      )
      SELECT ${baseCols.join(", ")}
      FROM filtered
      ORDER BY "rowNo"
      LIMIT :limit;
    `;
    const sample = await db.sequelize.query(sampleSql, {
      type: db.sequelize.QueryTypes.SELECT,
      transaction: t,
      replacements: { ...params },
    });

    return { sample, affectedCount };
  });
}

/**
 * List runs for a tenant. If hasMap=true, only include runs that have a saved column map.
 */
async function listRuns({ customerId, hasMap = false }) {
  const attrs = [
    "id",
    "customerId",
    "fileName",
    "fileSize",
    "mimeType",
    "rowCount",
    "status",
    "createdAt",
    "updatedAt",
  ];

  const runs = await db.PtrsUpload.findAll({
    where: { customerId },
    attributes: attrs,
    order: [["createdAt", "DESC"]],
    raw: true,
  });

  if (!hasMap) return runs;

  if (!runs.length) return [];
  const runIds = runs.map((r) => r.id);
  const maps = await db.PtrsColumnMap.findAll({
    where: { customerId, runId: runIds },
    attributes: ["runId"],
    raw: true,
  });
  const mappedSet = new Set(maps.map((m) => m.runId));
  return runs.filter((r) => mappedSet.has(r.id));
}

module.exports = {
  createUpload,
  getUpload,
  importCsvStream,
  getImportSample,
  getColumnMap,
  saveColumnMap,
  previewTransform,
  listRuns,
  addDataset,
  listDatasets,
  removeDataset,
};

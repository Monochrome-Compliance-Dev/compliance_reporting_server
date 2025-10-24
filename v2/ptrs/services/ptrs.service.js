const { logger } = require("@/helpers/logger");
const db = require("@/db/database");
const csv = require("fast-csv");

/** Get column map for an upload */
async function getColumnMap({ customerId, uploadId }) {
  return db.PtrsColumnMap.findOne({
    where: { customerId, uploadId },
    raw: true,
  });
}

/** Upsert column map for an upload */
async function saveColumnMap({ customerId, uploadId, mappings, userId }) {
  const existing = await db.PtrsColumnMap.findOne({
    where: { customerId, uploadId },
  });
  if (existing) {
    await existing.update({
      mappings,
      createdBy: userId || existing.createdBy || null,
    });
    return existing.get({ plain: true });
  }
  const row = await db.PtrsColumnMap.create({
    customerId,
    uploadId,
    mappings,
    createdBy: userId || null,
  });
  return row.get({ plain: true });
}

/**
 * Return a small window of staged rows plus count and inferred headers.
 */
async function getImportSample({
  customerId,
  uploadId,
  limit = 10,
  offset = 0,
}) {
  // rows
  const rows = await db.PtrsImportRaw.findAll({
    where: { customerId, uploadId },
    order: [["rowNo", "ASC"]],
    limit,
    offset,
    attributes: ["rowNo", "data"],
    raw: true,
  });

  // total
  const total = await db.PtrsImportRaw.count({
    where: { customerId, uploadId },
  });

  // headers: scan up to 500 earliest rows to reduce noise
  const headerScan = await db.PtrsImportRaw.findAll({
    where: { customerId, uploadId },
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
  const headers = Array.from(headerSet.values());

  return {
    rows,
    total,
    headers,
  };
}

/** Fetch one upload (tenant-scoped) */
async function getUpload({ uploadId, customerId }) {
  return db.PtrsUpload.findOne({ where: { id: uploadId, customerId } });
}

/**
 * Stream a CSV into tbl_ptrs_import_raw as JSONB rows
 * - stream: Readable of CSV
 * - returns rowsInserted (int)
 */
async function importCsvStream({ customerId, uploadId, stream }) {
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
      .parse({ headers: true, trim: true })
      .on("error", (err) => reject(err))
      .on("data", (row) => {
        rowNo += 1;
        batch.push({
          customerId,
          uploadId,
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
            { where: { id: uploadId, customerId } }
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
async function previewTransform({
  customerId,
  uploadId,
  steps = [],
  limit = 50,
}) {
  // Load column map (required to project JSONB -> columns)
  const mapRow = await getColumnMap({ customerId, uploadId });
  if (!mapRow || !mapRow.mappings) {
    throw new Error("No column map saved for this upload");
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
  const params = { customerId, uploadId, limit };
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
      WHERE "uploadId" = :uploadId AND "customerId" = :customerId;
    `;
    await db.sequelize.query(createTempSql, {
      transaction: t,
      replacements: { uploadId, customerId },
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

module.exports = {
  createUpload,
  getUpload,
  importCsvStream,
  getImportSample,
  getColumnMap,
  saveColumnMap,
  previewTransform,
};

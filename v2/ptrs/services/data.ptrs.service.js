const db = require("@/db/database");
const path = require("path");
const { Readable } = require("stream");
const csv = require("fast-csv");

const fs = require("fs");
const { Worker } = require("worker_threads");

const { logger } = require("@/helpers/logger");
const { normalizeJoinKeyValue, toSnake } = require("./ptrs.service");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

module.exports = {
  pickFromRowLoose,
  addDataset,
  listDatasets,
  removeDataset,
  getDatasetSample,
  buildDatasetIndexByRole,
  importPaymentTermChangesFromDataset,
  listPaymentTermChanges,
  upsertMainDatasetFromRaw,
};

const PAYMENT_TERM_CHANGE_ROLES = new Set([
  "paymenttermchanges",
  "paymenttermchange",
  "payment_term_changes",
  "payment_term_change",
  "payment-term-changes",
  "payment-term-change",

  // FE currently sends this role (do not rely on filename parsing)
  "termschanges",
  "termchanges",
]);

function isPaymentTermChangeRole(role) {
  const r = String(role || "")
    .trim()
    .toLowerCase();
  if (!r) return false;
  if (PAYMENT_TERM_CHANGE_ROLES.has(r)) return true;

  // Allow minor variations without having to keep extending the set.
  // Example: "terms_changes", "payment_terms_changes", "payment-term-change-file" etc.
  const compact = r.replace(/[^a-z0-9]/g, "");
  return compact.includes("term") && compact.includes("change");
}

function modelHasField(model, field) {
  try {
    return Boolean(model?.rawAttributes && model.rawAttributes[field]);
  } catch {
    return false;
  }
}

function pickModelFields(model, candidate) {
  if (!model?.rawAttributes) return { ...candidate };
  const allowed = new Set(Object.keys(model.rawAttributes));
  const out = {};
  for (const [k, v] of Object.entries(candidate || {})) {
    if (allowed.has(k)) out[k] = v;
  }
  return out;
}

/**
 * Ensure a PTRS run has a \"main\" dataset row in tbl_ptrs_dataset when raw rows exist.
 * This is required for the standard Step 2 FE flow (datasets list) to work for non-file ingests like Xero.
 */
async function upsertMainDatasetFromRaw({
  customerId,
  ptrsId,
  source = "raw",
  userId = null,
  meta = {},
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const rawCount = await db.PtrsImportRaw.count({
      where: { customerId, ptrsId },
      transaction: t,
    });

    if (!rawCount || rawCount <= 0) {
      await t.commit();
      return { ok: true, rowsCount: 0, dataset: null };
    }

    const displayName =
      source === "xero"
        ? "Xero import"
        : source === "csv"
          ? "CSV upload"
          : "Main input";

    const candidate = {
      customerId,
      ptrsId,
      role: "main",
      // PtrsDataset.fileName is NOT NULL. For non-file ingests (e.g. Xero), we still need a label.
      fileName: displayName,
      storageRef: null,
      rowsCount: rawCount,
      status: "uploaded",
      meta: {
        ...(meta || {}),
        source,
        rowsCount: rawCount,
        displayName,
        updatedAt: new Date().toISOString(),
      },
      createdBy: userId || null,
      updatedBy: userId || null,
    };

    const rowToWrite = pickModelFields(db.PtrsDataset, candidate);

    const existing = await db.PtrsDataset.findOne({
      where: { customerId, ptrsId, role: "main" },
      transaction: t,
      raw: false,
    });

    let saved;
    if (existing) {
      saved = await existing.update(rowToWrite, { transaction: t });
    } else {
      saved = await db.PtrsDataset.create(rowToWrite, { transaction: t });
    }

    await t.commit();

    return {
      ok: true,
      rowsCount: rawCount,
      dataset: saved?.get ? saved.get({ plain: true }) : saved,
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

// TODO: future: allow external transaction but only from beginTransactionWithCustomerContext

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

function pickFromRowLoose(row, header) {
  if (!row || !header) return undefined;
  for (const h of headerVariants(header)) {
    if (row[h] != null) {
      return row[h];
    }
    const kh = Object.keys(row).find(
      (k) => String(k).toLowerCase() === String(h).toLowerCase()
    );
    if (kh && row[kh] != null) {
      return row[kh];
    }
  }
  return undefined;
}

function headerVariants(key) {
  const original = String(key || "");
  const snake = toSnake(original);
  const underscored = original.replace(/\s+/g, "_");
  const cased = original.toLowerCase();
  const set = new Set([original, snake, underscored, cased]);
  return Array.from(set.values());
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
    const ptrsProfileId = ptrs?.profileId || ptrs?.profile_id || null;

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

    // Commit the dataset upload first; the subsequent import runs its own txn.
    await t.commit();

    const plain = row.get({ plain: true });

    // Log info about the dataset and whether it will trigger term changes import
    logger?.info?.("PTRS v2 addDataset: uploaded dataset", {
      action: "PtrsV2AddDataset",
      customerId,
      ptrsId,
      datasetId: plain.id,
      role: normalisedRole,
      willImportPaymentTermChanges: isPaymentTermChangeRole(normalisedRole),
    });

    // If this dataset is a payment-term-change file, immediately import it into
    // tbl_ptrs_payment_term_change so Stage can apply it deterministically.
    // We fail loudly here because a silent import failure will cause confusing metrics later.
    if (isPaymentTermChangeRole(normalisedRole)) {
      if (!ptrsProfileId) {
        const e = new Error(
          "PTRS profileId is missing; cannot import payment term changes without a profileId"
        );
        e.statusCode = 400;
        throw e;
      }

      logger?.info?.(
        "PTRS v2 addDataset: starting payment term changes import",
        {
          action: "PtrsV2AddDatasetPaymentTermChangeImportStart",
          customerId,
          ptrsId,
          datasetId: plain.id,
          role: normalisedRole,
          profileId: ptrsProfileId,
        }
      );

      let importResult;
      try {
        importResult = await importPaymentTermChangesFromDataset({
          customerId,
          ptrsId,
          profileId: ptrsProfileId,
          datasetId: plain.id,
          userId: userId || null,
        });
      } catch (e) {
        logger?.error?.(
          "PTRS v2 addDataset: payment term changes import failed",
          {
            action: "PtrsV2AddDatasetPaymentTermChangeImportFailed",
            customerId,
            ptrsId,
            datasetId: plain.id,
            role: normalisedRole,
            profileId: ptrsProfileId,
            error: e?.message,
          }
        );
        throw e;
      }

      // Persist import stats onto the dataset meta for audit/debug.
      const t2 = await beginTransactionWithCustomerContext(customerId);
      try {
        const ds = await db.PtrsDataset.findOne({
          where: { id: plain.id, customerId, ptrsId },
          transaction: t2,
        });

        if (ds) {
          const currentMeta = ds.get("meta") || {};
          await ds.update(
            {
              meta: {
                ...currentMeta,
                paymentTermChangesImport: {
                  at: new Date().toISOString(),
                  datasetId: plain.id,
                  stats: importResult?.stats || null,
                },
              },
              updatedBy: userId || ds.get("updatedBy") || null,
            },
            { transaction: t2 }
          );
        }

        await t2.commit();
      } catch (e) {
        try {
          await t2.rollback();
        } catch (_) {}
        // Non-fatal: import is already done; meta update is best-effort.
        logger?.warn?.(
          "PTRS v2 addDataset: imported payment term changes but failed to update meta",
          {
            action: "PtrsV2AddDatasetPaymentTermChangeMetaUpdateFailed",
            customerId,
            ptrsId,
            datasetId: plain.id,
            error: e?.message,
          }
        );
      }

      return {
        ...plain,
        meta: {
          ...(plain.meta || {}),
          paymentTermChangesImport: {
            at: new Date().toISOString(),
            datasetId: plain.id,
            stats: importResult?.stats || null,
          },
        },
      };
    }

    return plain;
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

    const hasMain = rows.some(
      (r) =>
        String(r.role || "")
          .trim()
          .toLowerCase() === "main"
    );

    if (!hasMain) {
      const rawCount = await db.PtrsImportRaw.count({
        where: { customerId, ptrsId },
        transaction: t,
      });

      if (rawCount > 0) {
        const candidate = {
          customerId,
          ptrsId,
          role: "main",
          // PtrsDataset.fileName is NOT NULL.
          fileName: "Main input",
          storageRef: null,
          rowsCount: rawCount,
          status: "uploaded",
          meta: {
            source: "raw",
            rowsCount: rawCount,
            displayName: "Main input",
          },
          createdBy: null,
          updatedBy: null,
        };

        const rowToWrite = pickModelFields(db.PtrsDataset, candidate);

        const existing = await db.PtrsDataset.findOne({
          where: { customerId, ptrsId, role: "main" },
          transaction: t,
          raw: false,
        });

        if (existing) {
          await existing.update(rowToWrite, { transaction: t });
        } else {
          await db.PtrsDataset.create(rowToWrite, { transaction: t });
        }

        // Refresh list so FE sees it immediately
        const refreshed = await db.PtrsDataset.findAll({
          where: { customerId, ptrsId },
          order: [["createdAt", "DESC"]],
          raw: true,
          transaction: t,
        });

        rows.length = 0;
        rows.push(...refreshed);
      }
    }

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

  // If this is a synthetic "main" dataset (e.g. Xero import), there may be no file.
  // In that case, sample from tbl_ptrs_import_raw instead so the existing FE Step 2 flow works.
  const role = String(row.role || "")
    .trim()
    .toLowerCase();
  const hasStorageRef = Boolean(storageRef);

  const isUsableFile = (() => {
    if (!hasStorageRef) return false;
    try {
      if (!fs.existsSync(storageRef)) return false;
      const st = fs.statSync(storageRef);
      return st.isFile();
    } catch (_) {
      return false;
    }
  })();

  if (!isUsableFile) {
    if (role === "main") {
      const ptrsId = row.ptrsId;
      if (!ptrsId) {
        const e = new Error("Dataset is missing ptrsId");
        e.statusCode = 500;
        throw e;
      }

      // Sample ImportRaw rows (JSON payload) for this PTRS run.
      const tRaw = await beginTransactionWithCustomerContext(customerId);
      try {
        const total = await db.PtrsImportRaw.count({
          where: { customerId, ptrsId },
          transaction: tRaw,
        });

        const items = await db.PtrsImportRaw.findAll({
          where: { customerId, ptrsId },
          order: [["rowNo", "ASC"]],
          offset: Math.max(Number(offset) || 0, 0),
          limit: Math.min(Math.max(Number(limit) || 10, 1), 200),
          raw: true,
          transaction: tRaw,
        });

        await tRaw.commit();

        const rows = (items || []).map((r) => {
          // Support common payload field names.
          return r.data || r.payload || r.rawPayload || r.raw || {};
        });

        // Build headers from the union of keys across sampled rows.
        const headerSet = new Set();
        for (const r of rows) {
          if (r && typeof r === "object" && !Array.isArray(r)) {
            for (const k of Object.keys(r)) headerSet.add(k);
          }
        }

        return {
          headers: Array.from(headerSet.values()),
          rows,
          total,
        };
      } catch (err) {
        try {
          await tRaw.rollback();
        } catch (_) {}
        throw err;
      }
    }

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

async function buildDatasetIndexByRole({
  customerId,
  ptrsId,
  role,
  keyColumn,
}) {
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
    return { map: new Map(), headers: [], rowsIndexed: 0 };
  }

  const storageRef = ds.storageRef;
  if (!storageRef || !fs.existsSync(storageRef)) {
    return { map: new Map(), headers: [], rowsIndexed: 0 };
  }

  // Guard against storageRef pointing at a directory
  try {
    const st = fs.statSync(storageRef);
    if (!st.isFile()) {
      return { map: new Map(), headers: [], rowsIndexed: 0 };
    }
  } catch (_) {
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
        console.error("buildDatasetIndexByRole PARSE ERROR", err.message);
        reject(err);
      })
      .on("data", (row) => {
        if (isFirst) {
          headers = Object.keys(row || {});
          isFirst = false;
        }
        const rawKey = pickFromRowLoose(row, keyColumn);
        const normKey = normalizeJoinKeyValue(rawKey);
        if (!index.has(normKey)) index.set(normKey, row);
      })
      .on("end", () => {
        resolve();
      });

    stream.pipe(parser);
  });

  return { map: index, headers, rowsIndexed: index.size };
}

function parseAuDateTimeDMY(dateStr, timeStr) {
  // Accept a few common exports:
  // - DD/MM/YYYY (+ optional Time column)
  // - DD/MM/YY and MM/DD/YY (some XLSX->CSV conversions output US-style)
  // - DD-MM-YYYY
  // - YYYY-MM-DD
  // - Date column containing both date+time
  // - Excel serial numbers (days), including fractional time

  const rawDate = dateStr == null ? "" : String(dateStr).trim();
  const rawTime = timeStr == null ? "" : String(timeStr).trim();
  if (!rawDate) return null;

  // Excel serial date (common after XLSX conversion). Supports fractional day for time.
  if (/^\d+(\.\d+)?$/.test(rawDate)) {
    const n = Number(rawDate);
    if (Number.isFinite(n) && n > 0) {
      // Excel epoch: 1899-12-30; 25569 = 1970-01-01
      const ms = Math.round((n - 25569) * 86400 * 1000);
      const dt = new Date(ms);
      if (!Number.isNaN(dt.getTime())) return dt;
    }
  }

  // If date cell already contains time, split it.
  let dPart = rawDate;
  let tPart = rawTime;
  if (!tPart && /\d{1,2}:\d{2}/.test(rawDate)) {
    const parts = rawDate.split(/\s+/);
    if (parts.length >= 2) {
      dPart = parts[0];
      tPart = parts.slice(1).join(" ");
    }
  }

  // Parse time.
  // Supports:
  // - 24h: HH:MM or HH:MM:SS
  // - 12h: H:MM(:SS) AM/PM
  // - Excel time serial fraction (0.x)
  let hh = 0;
  let mm = 0;
  let ss = 0;

  const parseTime = (val) => {
    if (val == null) return null;
    const s = String(val).trim();
    if (!s) return null;

    // Excel time as fraction of a day
    if (/^0?\.\d+$/.test(s) || /^\d+(\.\d+)?$/.test(s)) {
      const n = Number(s);
      // A pure integer here is probably not a time, so only treat values between 0 and 1 as time-of-day.
      if (Number.isFinite(n) && n >= 0 && n < 1) {
        const totalSeconds = Math.round(n * 86400);
        const h = Math.floor(totalSeconds / 3600);
        const m = Math.floor((totalSeconds % 3600) / 60);
        const sec = totalSeconds % 60;
        return { hh: h, mm: m, ss: sec };
      }
    }

    // 12-hour time with AM/PM
    let m = s.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)$/i);
    if (m) {
      let h = Number(m[1]);
      const mins = Number(m[2]);
      const secs = Number(m[3] || 0);
      const mer = String(m[4]).toUpperCase();
      if (mer === "AM") {
        if (h === 12) h = 0;
      } else if (mer === "PM") {
        if (h !== 12) h += 12;
      }
      return { hh: h, mm: mins, ss: secs };
    }

    // 24-hour time
    m = s.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?/);
    if (m) {
      return { hh: Number(m[1]), mm: Number(m[2]), ss: Number(m[3] || 0) };
    }

    return null;
  };

  const parsedTime = parseTime(tPart);
  if (parsedTime) {
    hh = parsedTime.hh;
    mm = parsedTime.mm;
    ss = parsedTime.ss;
  }

  // Date parsers
  const tryDMY4 = (sep) => {
    const m = String(dPart).match(
      new RegExp(`^(\\d{1,2})\\${sep}(\\d{1,2})\\${sep}(\\d{4})$`)
    );
    if (!m) return null;
    const day = Number(m[1]);
    const month = Number(m[2]);
    const year = Number(m[3]);
    const dt = new Date(year, month - 1, day, hh, mm, ss, 0);
    return Number.isNaN(dt.getTime()) ? null : dt;
  };

  const tryDMY2orMDY2 = (sep) => {
    const m = String(dPart).match(
      new RegExp(`^(\\d{1,2})\\${sep}(\\d{1,2})\\${sep}(\\d{2})$`)
    );
    if (!m) return null;

    const a = Number(m[1]);
    const b = Number(m[2]);
    const yy = Number(m[3]);

    // Two-digit year: assume 2000-2099 for now (safe for this dataset).
    const year = 2000 + yy;

    // Disambiguate DD/MM/YY vs MM/DD/YY.
    // If one side is >12, it's unambiguous.
    // Otherwise default to DMY (AU) to avoid silently flipping Australian dates.
    let day;
    let month;
    if (a > 12 && b <= 12) {
      day = a;
      month = b;
    } else if (b > 12 && a <= 12) {
      // US-style
      month = a;
      day = b;
    } else {
      // ambiguous (e.g., 01/02/24) -> assume AU DMY
      day = a;
      month = b;
    }

    const dt = new Date(year, month - 1, day, hh, mm, ss, 0);
    return Number.isNaN(dt.getTime()) ? null : dt;
  };

  const tryYMD = () => {
    const m = String(dPart).match(/^\s*(\d{4})-(\d{1,2})-(\d{1,2})\s*$/);
    if (!m) return null;
    const year = Number(m[1]);
    const month = Number(m[2]);
    const day = Number(m[3]);
    const dt = new Date(year, month - 1, day, hh, mm, ss, 0);
    return Number.isNaN(dt.getTime()) ? null : dt;
  };

  return (
    tryDMY4("/") ||
    tryDMY4("-") ||
    tryDMY2orMDY2("/") ||
    tryDMY2orMDY2("-") ||
    tryYMD()
  );
}

async function listPaymentTermChanges({
  customerId,
  profileId,
  companyCode = null,
  limit = 200,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!profileId) throw new Error("profileId is required");

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const where = { customerId, profileId };
    if (companyCode) where.companyCode = String(companyCode);

    const rows = await db.PtrsPaymentTermChange.findAll({
      where,
      order: [
        ["companyCode", "ASC"],
        ["changedAt", "DESC"],
      ],
      limit: Math.min(Number(limit) || 200, 1000),
      raw: true,
      transaction: t,
    });

    await t.commit();
    return rows;
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
 * Import effective-dated payment term changes from an uploaded dataset (stored as CSV) into
 * tbl_ptrs_payment_term_change.
 *
 * Expected headers (loose match):
 * - Date, Time, Supplier, Changed By, Field Name, Company Code, Purch. organization, New value, Old value
 */
async function importPaymentTermChangesFromDataset({
  customerId,
  ptrsId,
  profileId = null,
  datasetId,
  userId = null,
  fieldNameFilter = "Payt terms",
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!datasetId) throw new Error("datasetId is required");

  logger?.info?.("PTRS v2 importPaymentTermChangesFromDataset: starting", {
    action: "PtrsV2ImportPaymentTermChangesFromDataset",
    customerId,
    ptrsId,
    datasetId,
    profileId: profileId || null,
  });

  // If profileId not provided, resolve from ptrs.
  const t0 = await beginTransactionWithCustomerContext(customerId);
  let resolvedProfileId = profileId;
  let dataset;
  try {
    const ptrs = await db.Ptrs.findOne({
      where: { id: ptrsId, customerId },
      raw: true,
      transaction: t0,
    });
    if (!ptrs) {
      const e = new Error("Ptrs not found");
      e.statusCode = 404;
      throw e;
    }

    if (!resolvedProfileId) resolvedProfileId = ptrs.profileId;

    if (!resolvedProfileId) {
      const e = new Error(
        "profileId is required (and could not be resolved from ptrs)"
      );
      e.statusCode = 400;
      throw e;
    }

    dataset = await db.PtrsDataset.findOne({
      where: { id: datasetId, customerId, ptrsId },
      raw: true,
      transaction: t0,
    });

    if (!dataset) {
      const e = new Error("Dataset not found");
      e.statusCode = 404;
      throw e;
    }

    await t0.commit();
  } catch (err) {
    try {
      await t0.rollback();
    } catch (_) {}
    throw err;
  }

  const storageRef = dataset.storageRef;
  if (!storageRef || !fs.existsSync(storageRef)) {
    const e = new Error("Dataset file missing");
    e.statusCode = 404;
    throw e;
  }

  const rowsToInsert = [];
  const stats = {
    parsed: 0,
    inserted: 0,
    skipped: 0,
    skippedMissingCompanyCode: 0,
    skippedMissingNewValue: 0,
    skippedMissingDate: 0,
    skippedFieldNameMismatch: 0,
  };

  // Parse CSV rows
  await new Promise((resolve, reject) => {
    const stream = fs.createReadStream(storageRef);
    const parser = csv
      .parse({ headers: true, trim: true, ignoreEmpty: true })
      .on("error", reject)
      .on("data", (row) => {
        stats.parsed += 1;

        // Log raw CSV row keys for the first parsed row only
        if (stats.parsed === 1) {
          logger?.warn?.(
            "PTRS v2 importPaymentTermChangesFromDataset: first row keys",
            {
              action: "PtrsV2ImportPaymentTermChangesFromDatasetRowKeys",
              datasetId,
              ptrsId,
              profileId: resolvedProfileId,
              keys: Object.keys(row),
              sample: row,
            }
          );
        }

        const fieldName = pickFromRowLoose(row, "Field Name");
        if (fieldNameFilter && fieldName) {
          const lhs = String(fieldName)
            .trim()
            .toLowerCase()
            .replace(/[^a-z0-9]+/g, "");
          const rhs = String(fieldNameFilter)
            .trim()
            .toLowerCase()
            .replace(/[^a-z0-9]+/g, "");

          if (lhs !== rhs) {
            stats.skipped += 1;
            stats.skippedFieldNameMismatch += 1;
            return;
          }
        }

        const dateStr = pickFromRowLoose(row, "Date");
        const timeStr = pickFromRowLoose(row, "Time");
        const changedAt = parseAuDateTimeDMY(dateStr, timeStr);
        if (!changedAt) {
          stats.skipped += 1;
          stats.skippedMissingDate += 1;
          return;
        }

        const companyCode = pickFromRowLoose(row, "Company Code");
        if (!companyCode) {
          stats.skipped += 1;
          stats.skippedMissingCompanyCode += 1;
          return;
        }

        const newRaw = pickFromRowLoose(row, "New value");
        if (newRaw == null || String(newRaw).trim() === "") {
          stats.skipped += 1;
          stats.skippedMissingNewValue += 1;
          return;
        }

        const supplier = pickFromRowLoose(row, "Supplier");
        const changedBy = pickFromRowLoose(row, "Changed By");
        const purchOrganisation = pickFromRowLoose(row, "Purch. organization");
        const oldRaw = pickFromRowLoose(row, "Old value");

        const rec = {
          customerId,
          profileId: resolvedProfileId,
          changedAt,
          supplier: supplier != null ? String(supplier) : null,
          changedBy: changedBy != null ? String(changedBy) : null,
          fieldName: fieldName != null ? String(fieldName) : null,
          companyCode: String(companyCode),
          purchOrganisation:
            purchOrganisation != null ? String(purchOrganisation) : null,
          newRaw: String(newRaw),
          oldRaw:
            oldRaw != null && String(oldRaw).trim() !== ""
              ? String(oldRaw)
              : null,
          note: "Imported from dataset " + datasetId,
          createdBy: userId || null,
          updatedBy: userId || null,
        };

        if (modelHasField(db.PtrsPaymentTermChange, "ptrsId")) {
          rec.ptrsId = ptrsId;
        }
        if (modelHasField(db.PtrsPaymentTermChange, "datasetId")) {
          rec.datasetId = datasetId;
        }

        rowsToInsert.push(rec);
      })
      .on("end", () => resolve());

    stream.pipe(parser);
  });

  if (!rowsToInsert.length) {
    logger?.warn?.(
      "PTRS v2 importPaymentTermChangesFromDataset: nothing to insert (all rows skipped or filtered)",
      {
        action: "PtrsV2ImportPaymentTermChangesFromDatasetNoRows",
        customerId,
        ptrsId,
        datasetId,
        profileId: resolvedProfileId,
        stats,
      }
    );

    return {
      ok: true,
      profileId: resolvedProfileId,
      datasetId,
      stats: { ...stats, inserted: 0 },
    };
  }

  logger?.info?.("PTRS v2 importPaymentTermChangesFromDataset: parsed", {
    action: "PtrsV2ImportPaymentTermChangesFromDatasetParsed",
    customerId,
    ptrsId,
    datasetId,
    profileId: resolvedProfileId,
    parsed: stats.parsed,
    toInsert: rowsToInsert.length,
    skipped: stats.skipped,
    skippedMissingCompanyCode: stats.skippedMissingCompanyCode,
    skippedMissingNewValue: stats.skippedMissingNewValue,
    skippedMissingDate: stats.skippedMissingDate,
    skippedFieldNameMismatch: stats.skippedFieldNameMismatch,
  });

  // Persist in one transaction.
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    await db.PtrsPaymentTermChange.bulkCreate(rowsToInsert, {
      transaction: t,
      validate: true,
      returning: false,
    });

    await t.commit();

    logger?.info?.("PTRS v2 importPaymentTermChangesFromDataset: completed", {
      action: "PtrsV2ImportPaymentTermChangesFromDatasetCompleted",
      customerId,
      ptrsId,
      datasetId,
      profileId: resolvedProfileId,
      inserted: rowsToInsert.length,
    });

    stats.inserted = rowsToInsert.length;
    return {
      ok: true,
      profileId: resolvedProfileId,
      datasetId,
      stats,
    };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    logger?.error?.("PTRS v2 importPaymentTermChangesFromDataset: failed", {
      action: "PtrsV2ImportPaymentTermChangesFromDatasetFailed",
      customerId,
      ptrsId,
      datasetId,
      profileId: resolvedProfileId,
      error: err?.message,
    });
    throw err;
  }
}

const db = require("@/db/database");
const path = require("path");
const { Readable } = require("stream");
const fs = require("fs");

const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

module.exports = {
  addDataset,
  listDatasets,
  removeDataset,
  getDatasetSample,
};

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

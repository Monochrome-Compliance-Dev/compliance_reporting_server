const fs = require("fs");
const path = require("path");
const readline = require("readline");
const csv = require("fast-csv");
const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

function toPlain(row) {
  if (!row) return null;
  return row.get ? row.get({ plain: true }) : row;
}

function normaliseDataset(row) {
  const plain = toPlain(row);
  if (!plain) return null;

  return {
    ...plain,
    id: plain.id,
    datasetId: plain.id,
    runId: plain.runId,
    datasetType: plain.role,
    role: plain.role,
    fileName: plain.originalFileName || plain.sourceName || null,
    sourceName: plain.sourceName || plain.originalFileName || null,
    rowsInserted: Number(plain.rowsCount || 0),
    rowsCount: Number(plain.rowsCount || 0),
    headers: Array.isArray(plain.headers) ? plain.headers : [],
    headersCount: Number(plain.headersCount || 0),
    detectedCoverage: plain.detectedCoverage || {},
    status: plain.status || "uploaded",
  };
}

function rollbackQuietly(t) {
  if (!t || t.finished) return Promise.resolve();
  return t.rollback().catch(() => {});
}

function getDataHubDatasetModel() {
  if (!db.DataHubDataset) {
    throw new Error("DataHubDataset model is not registered on db");
  }
  return db.DataHubDataset;
}

function getDataHubRunModel() {
  if (!db.DataHubRun) {
    throw new Error("DataHubRun model is not registered on db");
  }
  return db.DataHubRun;
}

function emitDatasetUploadStatus(runId, payload) {
  try {
    const io = global.__socketio;
    if (!io || !runId) return;

    io.to(`dataHub:${runId}`).emit("dataHub:datasetUploadStatus", {
      runId,
      ...payload,
      updatedAt: new Date().toISOString(),
    });
  } catch (_) {
    // Never break ingest due to websocket failure
  }
}

function parseCsvLine(line = "") {
  const values = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    const next = line[i + 1];

    if (char === '"' && inQuotes && next === '"') {
      current += '"';
      i += 1;
    } else if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === "," && !inQuotes) {
      values.push(current.trim());
      current = "";
    } else {
      current += char;
    }
  }

  values.push(current.trim());
  return values.map((value) => value.replace(/^"|"$/g, ""));
}

function dedupeHeaders(rawHeaders = []) {
  const seen = new Map();
  return rawHeaders.map((header, index) => {
    const trimmed = String(header || "").trim();
    const label = trimmed.length ? trimmed : `column_${index + 1}`;
    const count = (seen.get(label) || 0) + 1;
    seen.set(label, count);
    return count === 1 ? label : `${label}_${count}`;
  });
}

function readFirstLine(filePath) {
  const fd = fs.openSync(filePath, "r");
  try {
    const chunkSize = 64 * 1024;
    const buffer = Buffer.alloc(chunkSize);
    let output = "";
    let position = 0;

    while (true) {
      const bytes = fs.readSync(fd, buffer, 0, chunkSize, position);
      if (!bytes) break;

      const chunk = buffer.toString("utf8", 0, bytes);
      const newlineIndex = chunk.search(/\r?\n/);
      if (newlineIndex >= 0) {
        output += chunk.slice(0, newlineIndex);
        break;
      }

      output += chunk;
      position += bytes;
      if (position > 1024 * 1024) break;
    }

    return output.replace(/^\uFEFF/, "");
  } finally {
    fs.closeSync(fd);
  }
}

async function inspectCsvFile(filePath) {
  if (!filePath || !fs.existsSync(filePath)) {
    return { headers: [], headersCount: 0, rowsCount: 0 };
  }

  const stream = fs.createReadStream(filePath);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let headers = [];
  let rowsCount = 0;
  let lineNo = 0;

  try {
    for await (const line of rl) {
      if (!line || !line.trim()) continue;

      if (lineNo === 0) {
        headers = parseCsvLine(line);
      } else {
        rowsCount += 1;
      }
      lineNo += 1;
    }
  } finally {
    rl.close();
    stream.destroy();
  }

  return {
    headers,
    headersCount: headers.length,
    rowsCount,
  };
}

async function getRunForCustomer({ customerId, runId, transaction }) {
  const DataHubRun = getDataHubRunModel();
  const run = await DataHubRun.findOne({
    where: { id: runId, customerId },
    transaction,
  });

  if (!run) {
    const err = new Error("Data Hub run not found");
    err.statusCode = 404;
    throw err;
  }

  return run;
}

async function listDatasets({ customerId, runId, role = null } = {}) {
  if (!customerId) throw new Error("customerId is required");
  if (!runId) throw new Error("runId is required");

  const DataHubDataset = getDataHubDatasetModel();
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    await getRunForCustomer({ customerId, runId, transaction: t });

    const where = { customerId, runId };
    if (role) where.role = role;

    const rows = await DataHubDataset.findAll({
      where,
      order: [["createdAt", "DESC"]],
      transaction: t,
    });

    await t.commit();
    return rows.map(normaliseDataset);
  } catch (err) {
    await rollbackQuietly(t);
    logger?.error?.("Failed to list Data Hub datasets", {
      action: "DataHubListDatasets",
      customerId,
      runId,
      role,
      error: err.message,
    });
    throw err;
  }
}

async function createDataset({
  customerId,
  runId,
  profileId = null,
  role,
  sourceType = "csv",
  sourceName = null,
  fileName = null,
  fileSize = null,
  mimeType = null,
  buffer,
  meta = null,
  userId = null,
} = {}) {
  const rawRole = typeof role === "string" ? role.trim() : "";
  const normalisedRole = rawRole.toLowerCase();

  if (!customerId) throw new Error("customerId is required");
  if (!runId) throw new Error("runId is required");
  if (!normalisedRole) throw new Error("role is required");
  if (!buffer || !Buffer.isBuffer(buffer)) {
    throw new Error("file buffer is required");
  }

  const DataHubDataset = getDataHubDatasetModel();
  const t = await beginTransactionWithCustomerContext(customerId);
  let storagePath = null;

  emitDatasetUploadStatus(runId, {
    customerId,
    role: normalisedRole,
    sourceType,
    status: "processing",
    rowsInserted: 0,
    totalRows: 0,
  });

  try {
    const run = await getRunForCustomer({ customerId, runId, transaction: t });
    const effectiveProfileId = profileId || run.profileId || null;
    const originalFileName = fileName || sourceName || null;
    const displayName = sourceName || originalFileName || "Dataset upload";
    const ext = (originalFileName && path.extname(originalFileName)) || ".csv";

    const row = await DataHubDataset.create(
      {
        runId,
        customerId,
        profileId: effectiveProfileId,
        role: normalisedRole,
        sourceType: sourceType || "csv",
        sourceName: displayName,
        originalFileName,
        storedFileName: null,
        storagePath: null,
        mimeType: mimeType || null,
        fileSize: Number.isFinite(fileSize) ? fileSize : buffer.length || null,
        headers: [],
        headersCount: 0,
        rowsCount: 0,
        status: "uploaded",
        detectedCoverage: {},
        meta: meta && typeof meta === "object" ? meta : null,
        uploadedBy: userId || null,
        createdBy: userId || null,
        updatedBy: userId || null,
      },
      { transaction: t },
    );

    const datasetId = row.id;
    const baseDir = path.resolve(
      process.cwd(),
      "storage",
      "data_hub",
      String(customerId),
      String(runId),
    );
    fs.mkdirSync(baseDir, { recursive: true });

    const storedFileName = `${datasetId}${ext}`;
    storagePath = path.join(baseDir, storedFileName);
    fs.writeFileSync(storagePath, buffer);

    const fileInspection = await inspectCsvFile(storagePath);
    emitDatasetUploadStatus(runId, {
      customerId,
      datasetId,
      role: normalisedRole,
      sourceType,
      status: "processing",
      rowsInserted: 0,
      totalRows: fileInspection.rowsCount,
    });

    await row.update(
      {
        storedFileName,
        storagePath,
        headers: fileInspection.headers,
        headersCount: fileInspection.headersCount,
        rowsCount: fileInspection.rowsCount,
        meta: {
          ...(meta && typeof meta === "object" ? meta : {}),
          headers: fileInspection.headers,
          rowsCount: fileInspection.rowsCount,
          uploadedAt: new Date().toISOString(),
        },
        updatedBy: userId || null,
      },
      { transaction: t },
    );

    await run.update(
      {
        status: "uploaded",
        currentStep: "link",
        updatedBy: userId || null,
      },
      { transaction: t },
    );

    emitDatasetUploadStatus(runId, {
      customerId,
      datasetId,
      role: normalisedRole,
      sourceType,
      status: "complete",
      rowsInserted: fileInspection.rowsCount,
      totalRows: fileInspection.rowsCount,
    });

    await t.commit();
    return normaliseDataset(row);
  } catch (err) {
    emitDatasetUploadStatus(runId, {
      customerId,
      role: normalisedRole,
      sourceType,
      status: "failed",
      rowsInserted: 0,
      totalRows: 0,
      error: err?.message || "Dataset upload failed",
    });

    await rollbackQuietly(t);

    if (storagePath) {
      try {
        fs.unlinkSync(storagePath);
      } catch (_) {}
    }

    logger?.error?.("Failed to create Data Hub dataset", {
      action: "DataHubCreateDataset",
      customerId,
      runId,
      role: normalisedRole,
      error: err.message,
    });
    throw err;
  }
}

async function getDataset({ customerId, runId, datasetId } = {}) {
  if (!customerId) throw new Error("customerId is required");
  if (!runId) throw new Error("runId is required");
  if (!datasetId) throw new Error("datasetId is required");

  const DataHubDataset = getDataHubDatasetModel();
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    await getRunForCustomer({ customerId, runId, transaction: t });

    const row = await DataHubDataset.findOne({
      where: { id: datasetId, runId, customerId },
      transaction: t,
    });

    await t.commit();
    return normaliseDataset(row);
  } catch (err) {
    await rollbackQuietly(t);
    logger?.error?.("Failed to get Data Hub dataset", {
      action: "DataHubGetDataset",
      customerId,
      runId,
      datasetId,
      error: err.message,
    });
    throw err;
  }
}

async function getDatasetSample({
  customerId,
  runId,
  datasetId = null,
  role = null,
  limit = 10,
  offset = 0,
} = {}) {
  if (!customerId) throw new Error("customerId is required");
  if (!runId) throw new Error("runId is required");

  const DataHubDataset = getDataHubDatasetModel();
  const t = await beginTransactionWithCustomerContext(customerId);

  let dataset;
  try {
    await getRunForCustomer({ customerId, runId, transaction: t });

    const where = { customerId, runId };
    if (datasetId) where.id = datasetId;
    if (!datasetId && role) where.role = role;

    dataset = await DataHubDataset.findOne({
      where,
      order: [["createdAt", "DESC"]],
      raw: true,
      transaction: t,
    });

    await t.commit();
  } catch (err) {
    await rollbackQuietly(t);
    logger?.error?.("Failed to resolve Data Hub dataset sample source", {
      action: "DataHubGetDatasetSampleResolve",
      customerId,
      runId,
      datasetId,
      role,
      error: err.message,
    });
    throw err;
  }

  if (!dataset) {
    const err = new Error("Data Hub dataset not found");
    err.statusCode = 404;
    throw err;
  }

  const storagePath = dataset.storagePath;
  if (!storagePath || !fs.existsSync(storagePath)) {
    const err = new Error("Dataset file missing");
    err.statusCode = 404;
    throw err;
  }

  try {
    const stat = fs.statSync(storagePath);
    if (!stat.isFile()) {
      const err = new Error("Dataset storage path is not a file");
      err.statusCode = 404;
      throw err;
    }
  } catch (err) {
    err.statusCode = err.statusCode || 404;
    throw err;
  }

  const rawHeaders =
    Array.isArray(dataset.headers) && dataset.headers.length
      ? dataset.headers
      : parseCsvLine(readFirstLine(storagePath));
  const headers = dedupeHeaders(rawHeaders);
  const safeLimit = Math.min(Math.max(Number(limit) || 10, 1), 200);
  const safeOffset = Math.max(Number(offset) || 0, 0);

  const rows = [];
  let total = 0;

  await new Promise((resolve, reject) => {
    const stream = fs.createReadStream(storagePath);
    const parser = csv
      .parse({
        headers,
        renameHeaders: false,
        trim: true,
        skipLines: 1,
        ignoreEmpty: true,
        strictColumnHandling: false,
        discardUnmappedColumns: true,
      })
      .on("error", reject)
      .on("data", (row) => {
        if (total >= safeOffset && rows.length < safeLimit) rows.push(row);
        total += 1;
      })
      .on("end", () => resolve());

    stream.pipe(parser);
  });

  return {
    dataset: normaliseDataset(dataset),
    headers,
    rows,
    total,
  };
}

async function deleteDataset({
  customerId,
  runId,
  datasetId,
  userId = null,
} = {}) {
  if (!customerId) throw new Error("customerId is required");
  if (!runId) throw new Error("runId is required");
  if (!datasetId) throw new Error("datasetId is required");

  const DataHubDataset = getDataHubDatasetModel();
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    await getRunForCustomer({ customerId, runId, transaction: t });

    const row = await DataHubDataset.findOne({
      where: { id: datasetId, runId, customerId },
      transaction: t,
    });

    if (!row) {
      const err = new Error("Data Hub dataset not found");
      err.statusCode = 404;
      throw err;
    }

    if (userId) {
      await row.update({ updatedBy: userId }, { transaction: t });
    }
    await row.destroy({ transaction: t });

    await t.commit();
    return { ok: true, runId, datasetId };
  } catch (err) {
    await rollbackQuietly(t);
    logger?.error?.("Failed to delete Data Hub dataset", {
      action: "DataHubDeleteDataset",
      customerId,
      runId,
      datasetId,
      error: err.message,
    });
    throw err;
  }
}

module.exports = {
  normaliseDataset,
  inspectCsvFile,
  emitDatasetUploadStatus,
  listDatasets,
  createDataset,
  getDataset,
  getDatasetSample,
  deleteDataset,
};

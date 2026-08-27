const db = require("@/db/database");
const path = require("path");
const csv = require("fast-csv");

const fs = require("fs");
const crypto = require("crypto");
const { pipeline } = require("stream/promises");

const { logger } = require("@/helpers/logger");

const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  DEFAULT_BATCH_SIZE,
  ingestCsvFileInBatches,
} = require("@/v2/ptrs/services/csv-ingestion.ptrs.service");
const {
  validateDatasetClassification,
} = require("@/v2/ptrs/services/datasets.ptrs.service");

module.exports = {
  pickFromRowLoose,
  addDataset,
  listDatasets,
  removeDataset,
  getDatasetSample,
  importPaymentTermChangesFromDataset,
  listPaymentTermChanges,
  emitCsvUploadStatus,
  importCsvStream,
  importDatasetCsvStreamToImportRaw,
};

function emitCsvUploadStatus(ptrsId, payload) {
  try {
    const io = global.__socketio;
    if (!io || !ptrsId) return;

    io.to(`ptrs:${ptrsId}`).emit("ptrs:csvUploadStatus", {
      ptrsId,
      ...payload,
      updatedAt: new Date().toISOString(),
    });
  } catch (_) {
    // Never break ingest due to websocket failure
  }
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

async function importDatasetCsvStreamToImportRaw({
  customerId,
  ptrsId,
  datasetId,
  role = null,
  filePath,
  sourceType = null,
  fileSize = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!datasetId) throw new Error("datasetId is required");
  if (!filePath) throw new Error("filePath is required");

  emitCsvUploadStatus(ptrsId, {
    customerId,
    datasetId,
    role,
    sourceType,
    status: "uploading",
    rowsInserted: 0,
    totalRows: null,
    bytesProcessed: 0,
    totalBytes: Number.isFinite(fileSize) ? fileSize : null,
  });

  let rowsInserted = 0;
  try {
    const result = await ingestCsvFileInBatches({
      filePath,
      batchSize: DEFAULT_BATCH_SIZE,
      createRow: (data, rowNo) => ({
        customerId,
        ptrsId,
        datasetId,
        rowNo,
        data,
      }),
      persistBatch: async (batch) => {
        const transaction = await beginTransactionWithCustomerContext(customerId);
        try {
          await db.PtrsImportRaw.bulkCreate(batch, {
            validate: false,
            transaction,
          });
          await transaction.commit();
        } catch (error) {
          if (!transaction.finished) await transaction.rollback();
          throw error;
        }
      },
      onProgress: (progress) => {
        rowsInserted = progress.rowsInserted;
        emitCsvUploadStatus(ptrsId, {
          customerId,
          datasetId,
          role,
          sourceType,
          status: "uploading",
          rowsInserted,
          totalRows: null,
          bytesProcessed: progress.bytesProcessed,
          totalBytes: Number.isFinite(fileSize) ? fileSize : null,
        });
      },
    });

    rowsInserted = result.rowsInserted;
    const transaction = await beginTransactionWithCustomerContext(customerId);
    try {
      const dataset = await db.PtrsDataset.findOne({
        where: { id: datasetId, customerId, ptrsId },
        transaction,
      });
      if (!dataset) throw new Error("Dataset not found during CSV finalisation");
      const currentMeta = dataset.get("meta") || {};
      await dataset.update(
        {
          status: "parsed",
          rowsCount: rowsInserted,
          meta: {
            ...currentMeta,
            headers: result.headers,
            rowsCount: rowsInserted,
            sourceType: sourceType || dataset.get("sourceType") || null,
            role: role || dataset.get("role") || null,
            importedToRawAt: new Date().toISOString(),
          },
        },
        { transaction },
      );
      await transaction.commit();
    } catch (error) {
      if (!transaction.finished) await transaction.rollback();
      throw error;
    }

    emitCsvUploadStatus(ptrsId, {
      customerId,
      datasetId,
      role,
      sourceType,
      status: "complete",
      rowsInserted,
      totalRows: rowsInserted,
      bytesProcessed: result.bytesProcessed,
      totalBytes: Number.isFinite(fileSize) ? fileSize : null,
    });

    logger?.info?.("PTRS v2 dataset CSV ingestion complete", {
      action: "PtrsV2ImportDatasetToRaw",
      customerId,
      ptrsId,
      datasetId,
      role: role || null,
      rowsInserted,
      batchSize: DEFAULT_BATCH_SIZE,
    });

    return { ok: true, ...result };
  } catch (error) {
    let cleanupTransaction = null;
    try {
      cleanupTransaction = await beginTransactionWithCustomerContext(customerId);
      await db.PtrsImportRaw.destroy({
        where: { customerId, ptrsId, datasetId },
        transaction: cleanupTransaction,
      });
      const dataset = await db.PtrsDataset.findOne({
        where: { id: datasetId, customerId, ptrsId },
        transaction: cleanupTransaction,
      });
      if (dataset) {
        const currentMeta = dataset.get("meta") || {};
        await dataset.update(
          {
            status: "failed",
            rowsCount: null,
            storageRef: null,
            meta: {
              ...currentMeta,
              rowsCount: null,
              ingestionFailedAt: new Date().toISOString(),
              ingestionError: error.message,
            },
          },
          { transaction: cleanupTransaction },
        );
      }
      await cleanupTransaction.commit();
      await fs.promises.unlink(filePath).catch(() => {});
    } catch (cleanupError) {
      if (cleanupTransaction && !cleanupTransaction.finished) {
        await cleanupTransaction.rollback().catch(() => {});
      }
      logger?.error?.("PTRS v2 failed ingestion cleanup failed", {
        action: "PtrsV2ImportDatasetCleanupFailed",
        customerId,
        ptrsId,
        datasetId,
        error: cleanupError.message,
      });
    }

    emitCsvUploadStatus(ptrsId, {
      customerId,
      datasetId,
      role,
      sourceType,
      status: "failed",
      rowsInserted,
      totalRows: null,
      error: error.message || "CSV import failed",
    });
    throw error;
  }
}

async function importCsvStream({
  customerId,
  ptrsId,
  stream,
  filePath = null,
  fileMeta = null,
  sourceType = null,
  adapterType = null,
  adapterVersion = null,
  sourceGroupScope = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!filePath && !stream) throw new Error("filePath or stream is required");

  const ownsTemporaryUpload = !filePath;
  let uploadPath = filePath;
  if (!uploadPath) {
    uploadPath = await persistCsvStreamToTemporaryFile(stream);
  }

  try {
    const stat = await fs.promises.stat(uploadPath);
    const originalName = fileMeta?.originalName || `ptrs-${ptrsId}.csv`;
    const dataset = await addDataset({
      customerId,
      ptrsId,
      purpose: "transaction",
      sourceFormat: "csv",
      adapterType,
      adapterVersion,
      sourceGroupScope,
      sourceType: sourceType || "csv",
      sourceName: originalName,
      fileName: originalName,
      fileSize: fileMeta?.sizeBytes ?? stat.size,
      mimeType: fileMeta?.mimeType || "text/csv",
      uploadPath,
      userId: null,
    });

    const transaction = await beginTransactionWithCustomerContext(customerId);
    try {
      const where = { customerId, ptrsId };
      const values = pickModelFields(db.PtrsUpload, {
        customerId,
        ptrsId,
        originalName,
        mimeType: fileMeta?.mimeType || "text/csv",
        sizeBytes: fileMeta?.sizeBytes ?? stat.size,
        storagePath: dataset.storageRef || null,
      });
      const [upload, created] = await db.PtrsUpload.findOrCreate({
        where,
        defaults: values,
        transaction,
      });
      if (!created) await upload.update(values, { transaction });
      await transaction.commit();
    } catch (error) {
      if (!transaction.finished) await transaction.rollback();
      throw error;
    }

    return Number(dataset.rowsCount || dataset.meta?.rowsCount || 0);
  } finally {
    if (ownsTemporaryUpload && uploadPath) {
      await fs.promises.unlink(uploadPath).catch(() => {});
    }
  }
}

const PTRS_UPLOAD_TEMP_DIR = path.resolve(
  process.cwd(),
  "storage",
  "ptrs_uploads",
  "tmp",
);

async function persistCsvStreamToTemporaryFile(stream) {
  if (!stream) throw new Error("stream is required");
  await fs.promises.mkdir(PTRS_UPLOAD_TEMP_DIR, { recursive: true });
  const temporaryPath = path.join(
    PTRS_UPLOAD_TEMP_DIR,
    `${Date.now()}-${crypto.randomUUID()}.csv`,
  );
  try {
    await pipeline(stream, fs.createWriteStream(temporaryPath, { flags: "wx" }));
    return temporaryPath;
  } catch (error) {
    await fs.promises.unlink(temporaryPath).catch(() => {});
    throw error;
  }
}

async function moveFileToDurableStorage(sourcePath, destinationPath) {
  await fs.promises.mkdir(path.dirname(destinationPath), { recursive: true });
  try {
    await fs.promises.rename(sourcePath, destinationPath);
  } catch (error) {
    if (error?.code !== "EXDEV") throw error;
    await fs.promises.copyFile(sourcePath, destinationPath);
    await fs.promises.unlink(sourcePath);
  }
}

function pickFromRowLoose(row, header) {
  if (!row || !header) return undefined;
  for (const h of headerVariants(header)) {
    if (row[h] != null) {
      return row[h];
    }
    const kh = Object.keys(row).find(
      (k) => String(k).toLowerCase() === String(h).toLowerCase(),
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

/**
 * Create a dataset and materialise its durable CSV source into raw rows.
 */
async function addDataset({
  customerId,
  ptrsId,
  purpose,
  sourceFormat = "csv",
  adapterType = null,
  adapterVersion = null,
  referenceKind = null,
  sourceGroupScope = null,
  sourceType = null,
  sourceName = null,
  fileName = null,
  fileSize = null,
  mimeType = null,
  uploadPath,
  userId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!uploadPath) throw new Error("uploadPath is required");
  const classification = validateDatasetClassification({
    purpose,
    sourceFormat,
    adapterType,
    adapterVersion,
    referenceKind,
    sourceGroupScope,
  });
  if (classification.sourceFormat !== "csv") {
    const error = new Error("Dataset file uploads currently support CSV only");
    error.statusCode = 400;
    throw error;
  }

  const sourceStat = await fs.promises.stat(uploadPath);
  if (!sourceStat.isFile()) throw new Error("uploadPath must reference a file");

  const transaction = await beginTransactionWithCustomerContext(customerId);
  let storagePath = null;
  let ptrsProfileId = null;
  let datasetId = null;
  let ingestionComplete = false;

  try {
    const ptrs = await db.Ptrs.findOne({
      where: { id: ptrsId, customerId },
      transaction,
    });
    if (!ptrs) {
      const error = new Error("Ptrs not found");
      error.statusCode = 404;
      throw error;
    }
    ptrsProfileId = ptrs?.profileId || ptrs?.profile_id || null;
    const displayFileName = fileName || "dataset.csv";
    const dsCandidate = {
      customerId,
      ptrsId,
      ...classification,
      sourceType: sourceType || "csv",
      sourceName: sourceName || displayFileName,
      fileName: displayFileName,
      fileSize: Number.isFinite(fileSize) ? fileSize : sourceStat.size,
      mimeType: mimeType || "text/csv",
      storageRef: null,
      rowsCount: null,
      status: "uploading",
      meta: {
        source: "csv",
        originalName: displayFileName,
        sourceName: sourceName || displayFileName,
        purpose: classification.purpose,
        referenceKind: classification.referenceKind,
        sourceFormat: classification.sourceFormat,
        fileSize: Number.isFinite(fileSize) ? fileSize : sourceStat.size,
        mimeType: mimeType || "text/csv",
      },
      createdBy: userId || null,
      updatedBy: userId || null,
    };

    const row = await db.PtrsDataset.create(
      pickModelFields(db.PtrsDataset, dsCandidate),
      { transaction },
    );
    datasetId = row.id;
    const baseDir = path.resolve(
      process.cwd(),
      "storage",
      "ptrs_datasets",
      String(customerId),
      String(ptrsId),
    );
    storagePath = path.join(baseDir, `${datasetId}.csv`);
    await transaction.commit();

    await moveFileToDurableStorage(uploadPath, storagePath);
    const storageTransaction =
      await beginTransactionWithCustomerContext(customerId);
    try {
      const storedDataset = await db.PtrsDataset.findOne({
        where: { id: datasetId, customerId, ptrsId },
        transaction: storageTransaction,
      });
      if (!storedDataset) {
        throw new Error("Dataset not found after source storage");
      }
      await storedDataset.update(
        { storageRef: storagePath },
        { transaction: storageTransaction },
      );
      await storageTransaction.commit();
    } catch (error) {
      if (!storageTransaction.finished) await storageTransaction.rollback();
      throw error;
    }

    await importDatasetCsvStreamToImportRaw({
      customerId,
      ptrsId,
      datasetId,
      role: classification.role,
      sourceType: dsCandidate.sourceType,
      filePath: storagePath,
      fileSize: dsCandidate.fileSize,
    });
    ingestionComplete = true;

    logger?.info?.("PTRS v2 addDataset: uploaded dataset", {
      action: "PtrsV2AddDataset",
      customerId,
      ptrsId,
      datasetId,
      purpose: classification.purpose,
      referenceKind: classification.referenceKind,
      willImportPaymentTermChanges:
        classification.referenceKind === "termschanges",
    });

    // If this dataset is a payment-term-change file, immediately import it into
    // tbl_ptrs_payment_term_change so Stage can apply it deterministically.
    // We fail loudly here because a silent import failure will cause confusing metrics later.
    if (classification.referenceKind === "termschanges") {
      if (!ptrsProfileId) {
        const e = new Error(
          "PTRS profileId is missing; cannot import payment term changes without a profileId",
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
          datasetId,
          referenceKind: classification.referenceKind,
          profileId: ptrsProfileId,
        },
      );

      let importResult;
      try {
        importResult = await importPaymentTermChangesFromDataset({
          customerId,
          ptrsId,
          profileId: ptrsProfileId,
          datasetId,
          userId: userId || null,
        });
      } catch (e) {
        logger?.error?.(
          "PTRS v2 addDataset: payment term changes import failed",
          {
            action: "PtrsV2AddDatasetPaymentTermChangeImportFailed",
            customerId,
            ptrsId,
            datasetId,
            referenceKind: classification.referenceKind,
            profileId: ptrsProfileId,
            error: e?.message,
          },
        );
        throw e;
      }

      // Persist import stats onto the dataset meta for audit/debug.
      const t2 = await beginTransactionWithCustomerContext(customerId);
      try {
        const ds = await db.PtrsDataset.findOne({
          where: { id: datasetId, customerId, ptrsId },
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
                  datasetId,
                  stats: importResult?.stats || null,
                },
              },
              updatedBy: userId || ds.get("updatedBy") || null,
            },
            { transaction: t2 },
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
            datasetId,
            error: e?.message,
          },
        );
      }

    }

    const finalTransaction = await beginTransactionWithCustomerContext(customerId);
    try {
      const finalDataset = await db.PtrsDataset.findOne({
        where: { id: datasetId, customerId, ptrsId },
        transaction: finalTransaction,
      });
      if (!finalDataset) throw new Error("Dataset not found after CSV ingestion");
      const plain = finalDataset.get({ plain: true });
      await finalTransaction.commit();
      return plain;
    } catch (error) {
      if (!finalTransaction.finished) await finalTransaction.rollback();
      throw error;
    }
  } catch (err) {
    if (!transaction.finished) await transaction.rollback().catch(() => {});
    if (datasetId && !ingestionComplete) {
      let failureTransaction = null;
      try {
        failureTransaction =
          await beginTransactionWithCustomerContext(customerId);
        await db.PtrsImportRaw.destroy({
          where: { customerId, ptrsId, datasetId },
          transaction: failureTransaction,
        });
        const failedDataset = await db.PtrsDataset.findOne({
          where: { id: datasetId, customerId, ptrsId },
          transaction: failureTransaction,
        });
        if (failedDataset) {
          const currentMeta = failedDataset.get("meta") || {};
          await failedDataset.update(
            {
              status: "failed",
              storageRef: null,
              rowsCount: null,
              meta: {
                ...currentMeta,
                rowsCount: null,
                ingestionFailedAt: new Date().toISOString(),
                ingestionError: err.message,
              },
            },
            { transaction: failureTransaction },
          );
        }
        await failureTransaction.commit();
      } catch (cleanupError) {
        if (failureTransaction && !failureTransaction.finished) {
          await failureTransaction.rollback().catch(() => {});
        }
        logger?.error?.("PTRS v2 dataset failure state update failed", {
          action: "PtrsV2AddDatasetFailureStateFailed",
          customerId,
          ptrsId,
          datasetId,
          error: cleanupError.message,
        });
      }
    }
    if (storagePath && !ingestionComplete) {
      await fs.promises.unlink(storagePath).catch(() => {});
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
    const where = {
      customerId,
      ptrsId,
      ...(db.PtrsDataset?.rawAttributes?.deletedAt ? { deletedAt: null } : {}),
    };

    const rows = await db.PtrsDataset.findAll({
      where,
      order: [
        ["purpose", "ASC"],
        ["createdAt", "ASC"],
        ["id", "ASC"],
      ],
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

    // Stage is one combined population; removing any source invalidates it all.
    await db.PtrsStageRow.destroy({
      where: { customerId, ptrsId },
      force: true,
      transaction: t,
    });
    await db.PtrsCanonicalRevision.destroy({
      where: { customerId, ptrsId, datasetId },
      transaction: t,
    });
    await db.PtrsFieldMap.destroy({
      where: { customerId, ptrsId, datasetId },
      force: true,
      transaction: t,
    });
    await db.PtrsImportRaw.destroy({
      where: { customerId, ptrsId, datasetId },
      transaction: t,
    });
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

/** Return a bounded sample from the dataset-scoped persisted raw rows. */
async function getDatasetSample({
  customerId,
  datasetId,
  limit = 10,
  offset = 0,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!datasetId) throw new Error("datasetId is required");

  const transaction = await beginTransactionWithCustomerContext(customerId);
  try {
    const dataset = await db.PtrsDataset.findOne({
      where: { id: datasetId, customerId },
      raw: true,
      transaction,
    });
    if (!dataset) {
      const error = new Error("Dataset not found");
      error.statusCode = 404;
      throw error;
    }
    if (!dataset.ptrsId) throw new Error("Dataset is missing ptrsId");

    const where = {
      customerId,
      ptrsId: dataset.ptrsId,
      datasetId,
    };
    const total = Number.isInteger(dataset.rowsCount)
      ? dataset.rowsCount
      : await db.PtrsImportRaw.count({ where, transaction });
    const items = await db.PtrsImportRaw.findAll({
      where,
      order: [["rowNo", "ASC"]],
      offset: Math.max(Number(offset) || 0, 0),
      limit: Math.min(Math.max(Number(limit) || 10, 1), 200),
      raw: true,
      transaction,
    });
    const rows = (items || []).map((item) => item.data || {});
    let headers = Array.isArray(dataset.meta?.headers)
      ? dataset.meta.headers.map(String)
      : [];
    if (headers.length === 0) {
      headers = Array.from(
        new Set(rows.flatMap((sampleRow) => Object.keys(sampleRow || {}))),
      );
    }
    await transaction.commit();
    return { headers, rows, total };
  } catch (error) {
    if (!transaction.finished) await transaction.rollback();
    throw error;
  }
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
      new RegExp(`^(\\d{1,2})\\${sep}(\\d{1,2})\\${sep}(\\d{4})$`),
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
      new RegExp(`^(\\d{1,2})\\${sep}(\\d{1,2})\\${sep}(\\d{2})$`),
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
        "profileId is required (and could not be resolved from ptrs)",
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
            },
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
      },
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

function toSnake(str) {
  if (str == null) return "";
  return String(str)
    .replace(/[^a-zA-Z0-9]+/g, "_")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .toLowerCase()
    .replace(/^_+|_+$/g, "")
    .replace(/_{2,}/g, "_");
}

function applyJoinTransform(value, transform) {
  const v = value == null ? "" : String(value);

  const op =
    transform && typeof transform === "object"
      ? String(transform.op || "")
      : "";
  const arg =
    transform && typeof transform === "object" ? transform.arg : undefined;

  if (!op || op === "trim_upper") return v.trim().toUpperCase();

  switch (op) {
    case "digits_only":
      return v.replace(/[^0-9]/g, "");
    case "remove_spaces_punct":
      return v
        .replace(/[^a-zA-Z0-9]/g, "")
        .trim()
        .toUpperCase();
    case "strip_prefix": {
      const p = arg == null ? "" : String(arg);
      if (!p) return v.trim().toUpperCase();
      const t = v.trim();
      return t.startsWith(p)
        ? t.slice(p.length).trim().toUpperCase()
        : t.toUpperCase();
    }
    case "lpad": {
      const len = Number(arg);
      const t = v.trim();
      if (!Number.isFinite(len) || len <= 0) return t.toUpperCase();
      return t.padStart(len, "0").toUpperCase();
    }
    default:
      return v.trim().toUpperCase();
  }
}

function normalizeJoinKeyValue(v, transform = null) {
  return applyJoinTransform(v, transform);
}

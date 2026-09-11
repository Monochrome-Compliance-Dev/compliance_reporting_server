const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const xlsx = require("xlsx");

const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { validateDatasetClassification } = require("./datasets.ptrs.service");

const RAW_BATCH_SIZE = 500;

function workbookError(code, message, details = null) {
  const error = new Error(message);
  error.statusCode = 400;
  error.code = code;
  if (details) error.details = details;
  return error;
}

function normaliseHeading(value) {
  return String(value == null ? "" : value)
    .trim()
    .toLowerCase();
}

function validateSheetConfig(sheetConfig, index) {
  if (!sheetConfig || typeof sheetConfig !== "object") {
    throw workbookError(
      "WORKBOOK_PROFILE_INVALID",
      `Workbook sheet configuration ${index + 1} must be an object`,
    );
  }
  const sheetName = String(sheetConfig.sheetName || "").trim();
  if (!sheetName) {
    throw workbookError(
      "WORKBOOK_PROFILE_INVALID",
      `Workbook sheet configuration ${index + 1} is missing sheetName`,
    );
  }
  const classification = validateDatasetClassification({
    purpose: sheetConfig.purpose,
    sourceFormat: "xlsx",
    referenceKind: sheetConfig.referenceKind,
    adapterType: sheetConfig.adapterType,
    adapterVersion: sheetConfig.adapterVersion,
    sourceGroupScope: sheetConfig.sourceGroupScope,
  });
  if (classification.purpose === "transaction" && !classification.adapterType) {
    throw workbookError(
      "WORKBOOK_PROFILE_INVALID",
      `Transaction sheet ${sheetName} is missing adapterType`,
    );
  }
  if (
    classification.purpose === "transaction" &&
    !classification.sourceGroupScope
  ) {
    throw workbookError(
      "WORKBOOK_PROFILE_INVALID",
      `Transaction sheet ${sheetName} is missing sourceGroupScope`,
    );
  }
  const requiredColumns = Array.isArray(sheetConfig.requiredColumns)
    ? sheetConfig.requiredColumns
        .map((column) => String(column).trim())
        .filter(Boolean)
    : [];
  if (!requiredColumns.length) {
    throw workbookError(
      "WORKBOOK_PROFILE_INVALID",
      `Sheet ${sheetName} must declare requiredColumns`,
    );
  }
  const headerRow = Number(sheetConfig.headerRow || 1);
  if (!Number.isInteger(headerRow) || headerRow < 1) {
    throw workbookError(
      "WORKBOOK_PROFILE_INVALID",
      `Sheet ${sheetName} has an invalid headerRow`,
    );
  }
  const range = String(sheetConfig.range || "").trim() || null;
  let rangeStartRow = 1;
  if (range) {
    try {
      const decodedRange = xlsx.utils.decode_range(range);
      if (
        !Number.isInteger(decodedRange?.s?.r) ||
        !Number.isInteger(decodedRange?.e?.r) ||
        decodedRange.e.r < decodedRange.s.r
      ) {
        throw new Error("invalid range");
      }
      rangeStartRow = decodedRange.s.r + 1;
    } catch (_) {
      throw workbookError(
        "WORKBOOK_PROFILE_INVALID",
        `Sheet ${sheetName} has an invalid range`,
      );
    }
  }
  if (headerRow < rangeStartRow) {
    throw workbookError(
      "WORKBOOK_PROFILE_INVALID",
      `Sheet ${sheetName} headerRow is before its configured range`,
    );
  }
  return {
    ...classification,
    sheetName,
    headerRow,
    range,
    rangeStartRow,
    requiredColumns,
    sourceName: String(sheetConfig.sourceName || sheetName).trim(),
  };
}

function parseConfiguredWorkbook({
  workbook,
  sheetConfigs,
  ignoredSheetNames = [],
}) {
  if (!workbook || !Array.isArray(workbook.SheetNames)) {
    throw new Error("workbook is required");
  }
  if (!Array.isArray(sheetConfigs) || !sheetConfigs.length) {
    throw workbookError(
      "WORKBOOK_PROFILE_MISSING",
      "The selected PTRS profile has no workbookImport.sheets configuration",
    );
  }

  const sheetsByName = new Map();
  for (const actualName of workbook.SheetNames) {
    const key = normaliseHeading(actualName);
    const matches = sheetsByName.get(key) || [];
    matches.push(actualName);
    sheetsByName.set(key, matches);
  }

  const configuredNames = new Set();
  const parsed = sheetConfigs.map((rawConfig, index) => {
    const config = validateSheetConfig(rawConfig, index);
    const sheetKey = normaliseHeading(config.sheetName);
    if (configuredNames.has(sheetKey)) {
      throw workbookError(
        "WORKBOOK_SHEET_AMBIGUOUS",
        `Sheet ${config.sheetName} is assigned more than once by the profile`,
      );
    }
    configuredNames.add(sheetKey);

    const matches = sheetsByName.get(sheetKey) || [];
    if (matches.length !== 1) {
      throw workbookError(
        matches.length ? "WORKBOOK_SHEET_AMBIGUOUS" : "WORKBOOK_SHEET_MISSING",
        matches.length
          ? `Workbook sheet ${config.sheetName} is ambiguous`
          : `Workbook is missing configured sheet ${config.sheetName}`,
      );
    }
    const actualSheetName = matches[0];
    const matrix = xlsx.utils.sheet_to_json(workbook.Sheets[actualSheetName], {
      header: 1,
      defval: null,
      raw: false,
      blankrows: true,
      range: config.range || 0,
    });
    const headerIndex = config.headerRow - config.rangeStartRow;
    const headerCells = Array.isArray(matrix[headerIndex])
      ? matrix[headerIndex]
      : [];
    const headers = headerCells.map((value) =>
      String(value == null ? "" : value).trim(),
    );
    const duplicateHeaders = headers.filter(
      (header, position) =>
        header &&
        headers.findIndex(
          (candidate) =>
            normaliseHeading(candidate) === normaliseHeading(header),
        ) !== position,
    );
    if (duplicateHeaders.length) {
      throw workbookError(
        "WORKBOOK_COLUMNS_AMBIGUOUS",
        `Sheet ${actualSheetName} has ambiguous duplicate columns`,
        { columns: Array.from(new Set(duplicateHeaders)) },
      );
    }
    const available = new Map(
      headers
        .filter(Boolean)
        .map((header) => [normaliseHeading(header), header]),
    );
    const missingColumns = config.requiredColumns.filter(
      (column) => !available.has(normaliseHeading(column)),
    );
    if (missingColumns.length) {
      throw workbookError(
        "WORKBOOK_COLUMNS_MISSING",
        `Sheet ${actualSheetName} is missing required columns`,
        { missingColumns },
      );
    }

    const invalidRowOffset = matrix
      .slice(headerIndex + 1)
      .findIndex((values) =>
        values.some(
          (value, columnIndex) =>
            !headers[columnIndex] &&
            value != null &&
            String(value).trim() !== "",
        ),
      );
    if (invalidRowOffset >= 0) {
      throw workbookError(
        "WORKBOOK_ROW_SHAPE_INVALID",
        `Sheet ${actualSheetName} contains data under an unnamed column`,
        { rowNo: config.headerRow + invalidRowOffset + 1 },
      );
    }

    const rows = matrix
      .slice(headerIndex + 1)
      .map((values, offset) => {
        const data = {};
        headers.forEach((header, columnIndex) => {
          if (header) data[header] = values[columnIndex] ?? null;
        });
        return { rowNo: config.headerRow + offset + 1, data };
      })
      .filter((row) =>
        Object.values(row.data).some(
          (value) => value != null && String(value).trim() !== "",
        ),
      );

    return { config, actualSheetName, headers: headers.filter(Boolean), rows };
  });
  const ignoredNames = new Set(
    (Array.isArray(ignoredSheetNames) ? ignoredSheetNames : [])
      .map(normaliseHeading)
      .filter(Boolean),
  );
  const unrecognisedSheets = workbook.SheetNames.filter((sheetName) => {
    const key = normaliseHeading(sheetName);
    return !configuredNames.has(key) && !ignoredNames.has(key);
  });
  if (unrecognisedSheets.length) {
    throw workbookError(
      "WORKBOOK_STRUCTURE_UNRECOGNISED",
      "Workbook contains worksheets that the profile does not classify or explicitly ignore",
      { sheetNames: unrecognisedSheets },
    );
  }
  return parsed;
}

async function importConfiguredWorkbook({
  customerId,
  ptrsId,
  uploadPath,
  fileName,
  fileSize,
  mimeType,
  userId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!uploadPath) throw new Error("uploadPath is required");

  const workbook = xlsx.readFile(uploadPath, { cellDates: false });
  const transaction = await beginTransactionWithCustomerContext(customerId);
  const copiedFiles = [];
  let committed = false;
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
    const profileId = ptrs.get("profileId");
    if (!profileId) {
      throw workbookError(
        "WORKBOOK_PROFILE_MISSING",
        "PTRS report has no profile for workbook classification",
      );
    }
    const profile = await db.PtrsProfile.findOne({
      where: { id: profileId, customerId },
      transaction,
    });
    const workbookConfig = profile?.get("meta")?.workbookImport;
    const parsedSheets = parseConfiguredWorkbook({
      workbook,
      sheetConfigs: workbookConfig?.sheets,
      ignoredSheetNames: workbookConfig?.ignoredSheets,
    });
    const sourceStat = await fs.promises.stat(uploadPath);
    const originalName = fileName || path.basename(uploadPath);
    const baseDir = path.resolve(
      process.cwd(),
      "storage",
      "ptrs_datasets",
      String(customerId),
      String(ptrsId),
    );
    await fs.promises.mkdir(baseDir, { recursive: true });
    const extension = path.extname(originalName).toLowerCase() || ".xlsx";
    const storageRef = path.join(
      baseDir,
      `workbook-${crypto.randomUUID()}${extension}`,
    );
    await fs.promises.copyFile(uploadPath, storageRef);
    copiedFiles.push(storageRef);

    const datasets = [];
    for (const parsed of parsedSheets) {
      const { config, actualSheetName, headers, rows } = parsed;
      const dataset = await db.PtrsDataset.create(
        {
          customerId,
          ptrsId,
          role: config.role,
          purpose: config.purpose,
          sourceFormat: "xlsx",
          adapterType: config.adapterType,
          adapterVersion: config.adapterVersion,
          referenceKind: config.referenceKind,
          sourceGroupScope: config.sourceGroupScope,
          sourceType: "workbook",
          fileName: originalName,
          storageRef: null,
          rowsCount: rows.length,
          status: "uploading",
          meta: {
            source: "workbook",
            originalName,
            fileSize: Number.isFinite(fileSize) ? fileSize : sourceStat.size,
            mimeType: mimeType || null,
            sourceName: config.sourceName,
            headers,
            rowsCount: rows.length,
            role: config.role,
            workbook: {
              sheetName: actualSheetName,
              configuredSheetName: config.sheetName,
              headerRow: config.headerRow,
              range: config.range,
            },
          },
          createdBy: userId,
          updatedBy: userId,
        },
        { transaction },
      );
      for (let offset = 0; offset < rows.length; offset += RAW_BATCH_SIZE) {
        await db.PtrsImportRaw.bulkCreate(
          rows.slice(offset, offset + RAW_BATCH_SIZE).map((row) => ({
            customerId,
            ptrsId,
            datasetId: dataset.id,
            rowNo: row.rowNo,
            data: row.data,
          })),
          { validate: false, transaction },
        );
      }
      await dataset.update(
        {
          storageRef,
          status: "parsed",
          meta: {
            ...dataset.get("meta"),
            importedToRawAt: new Date().toISOString(),
          },
        },
        { transaction },
      );
      datasets.push(dataset.get({ plain: true }));
    }

    await transaction.commit();
    committed = true;
    const termChangeDatasets = datasets.filter(
      (dataset) => dataset.referenceKind === "termschanges",
    );
    if (termChangeDatasets.length) {
      // Lazy import avoids creating a second ingestion engine while retaining
      // the existing reference-dataset materialisation path.
      const {
        importPaymentTermChangesFromDataset,
      } = require("./data.ptrs.service");
      for (const dataset of termChangeDatasets) {
        await importPaymentTermChangesFromDataset({
          customerId,
          ptrsId,
          profileId,
          datasetId: dataset.id,
          userId,
        });
      }
    }
    return { profileId, datasets };
  } catch (error) {
    if (!transaction.finished) await transaction.rollback();
    if (!committed) {
      await Promise.all(
        copiedFiles.map((file) => fs.promises.unlink(file).catch(() => {})),
      );
    }
    throw error;
  }
}

module.exports = {
  importConfiguredWorkbook,
  parseConfiguredWorkbook,
  validateSheetConfig,
};

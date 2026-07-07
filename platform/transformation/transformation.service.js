const fs = require("fs/promises");
const path = require("path");
const datasetRepository = require("@/platform/data/dataset.repository");

function createError(message, status = 500) {
  const error = new Error(message);
  error.status = status;
  return error;
}

function hasValue(value) {
  return value !== undefined && value !== null && value !== "";
}

function requireValue(value, message) {
  if (!hasValue(value)) {
    throw createError(message);
  }
}

function buildActor(executionContext) {
  return {
    id: executionContext.actorId,
    role: executionContext.role,
    customerId: executionContext.customerId,
  };
}

function getLeaseExpiry(workingDataset) {
  if (!workingDataset.activeEditor?.expiresAt) {
    return null;
  }

  return new Date(workingDataset.activeEditor.expiresAt);
}

function isFinalWorkingDataset(workingDataset) {
  return workingDataset.status === "final";
}

function isLeaseActive(workingDataset, now) {
  const expiresAt = getLeaseExpiry(workingDataset);
  return Boolean(expiresAt && expiresAt > now);
}

function isLeaseOwnedByActor({ workingDataset, actor, editorSessionId }) {
  return (
    workingDataset.activeEditor?.userId === actor.id &&
    workingDataset.activeEditor?.sessionId === editorSessionId
  );
}

function requireEditableWorkingDataset(workingDataset) {
  if (isFinalWorkingDataset(workingDataset)) {
    throw createError("final working datasets cannot be materialised.", 409);
  }
}

function requireOwnedActiveLease({
  workingDataset,
  actor,
  editorSessionId,
  now,
}) {
  requireEditableWorkingDataset(workingDataset);

  if (!isLeaseActive(workingDataset, now)) {
    throw createError("active editor lease has expired.", 409);
  }

  if (!isLeaseOwnedByActor({ workingDataset, actor, editorSessionId })) {
    throw createError("active editor lease belongs to another session.", 409);
  }
}

function normaliseProjectionFields(fields) {
  if (!Array.isArray(fields) || fields.length === 0) {
    throw createError("fields must include at least one projection field.");
  }

  return fields.map((field, index) => {
    requireValue(
      field?.sourceField,
      `fields[${index}].sourceField is required for materialisation.`,
    );
    requireValue(
      field?.targetField,
      `fields[${index}].targetField is required for materialisation.`,
    );

    return {
      sourceField: String(field.sourceField).trim(),
      targetField: String(field.targetField).trim(),
    };
  });
}

function normaliseCustomFields(customFields = []) {
  if (!Array.isArray(customFields)) {
    throw createError("customFields must be an array when provided.");
  }

  return customFields.map((field, index) => {
    requireValue(
      field?.targetField,
      `customFields[${index}].targetField is required for materialisation.`,
    );

    return {
      targetField: String(field.targetField).trim(),
      value: hasValue(field.value) ? field.value : null,
    };
  });
}

function ensureUniqueTargetFields({ fields, customFields }) {
  const targetFields = [
    ...fields.map((field) => field.targetField),
    ...customFields.map((field) => field.targetField),
  ];
  const duplicates = targetFields.filter(
    (field, index) => targetFields.indexOf(field) !== index,
  );

  if (duplicates.length > 0) {
    throw createError("materialisation target fields must be unique.");
  }
}

function parseCsvLine(line) {
  const values = [];
  let current = "";
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const character = line[index];
    const nextCharacter = line[index + 1];

    if (character === '"' && inQuotes && nextCharacter === '"') {
      current += '"';
      index += 1;
    } else if (character === '"') {
      inQuotes = !inQuotes;
    } else if (character === "," && !inQuotes) {
      values.push(current);
      current = "";
    } else {
      current += character;
    }
  }

  values.push(current);

  return values;
}

function parseCsv(content) {
  const lines = content
    .replace(/^\uFEFF/, "")
    .split(/\r?\n/)
    .filter((line) => line.length > 0);

  if (lines.length === 0) {
    throw createError("working dataset CSV is empty.");
  }

  const headers = parseCsvLine(lines[0]);
  const rows = lines.slice(1).map((line) => {
    const values = parseCsvLine(line);

    return headers.reduce((row, header, index) => {
      row[header] = values[index] ?? "";
      return row;
    }, {});
  });

  return {
    headers,
    rows,
  };
}

function escapeCsvValue(value) {
  const stringValue =
    value === undefined || value === null ? "" : String(value);

  if (
    stringValue.includes(",") ||
    stringValue.includes('"') ||
    stringValue.includes("\n") ||
    stringValue.includes("\r")
  ) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }

  return stringValue;
}

function serialiseCsv({ headers, rows }) {
  const outputRows = [
    headers.map(escapeCsvValue).join(","),
    ...rows.map((row) =>
      headers.map((header) => escapeCsvValue(row[header])).join(","),
    ),
  ];

  return `${outputRows.join("\n")}\n`;
}

function buildMaterialisedRows({ sourceRows, fields, customFields }) {
  return sourceRows.map((sourceRow) => {
    const materialisedRow = {};

    fields.forEach((field) => {
      materialisedRow[field.targetField] = sourceRow[field.sourceField] ?? "";
    });

    customFields.forEach((field) => {
      materialisedRow[field.targetField] = field.value;
    });

    return materialisedRow;
  });
}

function buildMaterialisedFileName({ workingDatasetId, now }) {
  const timestamp = now.toISOString().replace(/[:.]/g, "-");
  return `${workingDatasetId}-materialised-${timestamp}.csv`;
}

function buildMaterialisedStoragePath({ workingDataset, storedFileName }) {
  const currentDirectory = path.dirname(workingDataset.storagePath);
  return path.join(currentDirectory, storedFileName);
}

async function writeMaterialisedCsv({
  workingDataset,
  fields,
  customFields,
  now,
}) {
  const sourceContent = await fs.readFile(workingDataset.storagePath, "utf8");
  const sourceCsv = parseCsv(sourceContent);
  const materialisedHeaders = [
    ...fields.map((field) => field.targetField),
    ...customFields.map((field) => field.targetField),
  ];
  const materialisedRows = buildMaterialisedRows({
    sourceRows: sourceCsv.rows,
    fields,
    customFields,
  });
  const materialisedContent = serialiseCsv({
    headers: materialisedHeaders,
    rows: materialisedRows,
  });
  const storedFileName = buildMaterialisedFileName({
    workingDatasetId: workingDataset.workingDatasetId,
    now,
  });
  const storagePath = buildMaterialisedStoragePath({
    workingDataset,
    storedFileName,
  });

  await fs.mkdir(path.dirname(storagePath), { recursive: true });
  await fs.writeFile(storagePath, materialisedContent, "utf8");

  const fileStats = await fs.stat(storagePath);

  return {
    storagePath,
    storedFileName,
    mimeType: "text/csv",
    fileSize: fileStats.size,
    headers: materialisedHeaders,
    headersCount: materialisedHeaders.length,
    rowsCount: materialisedRows.length,
    meta: {
      materialisedFrom: "projection_config",
      sourceStoragePath: workingDataset.storagePath,
      materialisedAt: now.toISOString(),
    },
  };
}

async function materialiseWorkingDataset({
  executionContext,
  params,
  body,
  PlatformDataWorkingDataset,
  PlatformDataWorkingDatasetActivity,
}) {
  requireValue(
    executionContext,
    "executionContext is required for working dataset materialisation.",
  );
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required for working dataset materialisation.",
  );
  requireValue(
    PlatformDataWorkingDatasetActivity,
    "PlatformDataWorkingDatasetActivity model is required for working dataset materialisation.",
  );
  requireValue(
    params,
    "params are required for working dataset materialisation.",
  );
  requireValue(
    params.workingDatasetId,
    "workingDatasetId is required for working dataset materialisation.",
  );
  requireValue(body, "body is required for working dataset materialisation.");
  requireValue(
    body.profileId,
    "profileId is required for working dataset materialisation.",
  );
  requireValue(
    body.editorSessionId,
    "editorSessionId is required for working dataset materialisation.",
  );
  requireValue(
    executionContext.customerId,
    "customerId is required for working dataset materialisation.",
  );

  const fields = normaliseProjectionFields(body.fields);
  const customFields = normaliseCustomFields(body.customFields);
  ensureUniqueTargetFields({ fields, customFields });

  const actor = buildActor(executionContext);
  const now = new Date();

  const workingDataset = await datasetRepository.getWorkingDatasetRecordById({
    PlatformDataWorkingDataset,
    workingDatasetId: params.workingDatasetId,
    customerId: executionContext.customerId,
    profileId: body.profileId,
  });

  requireOwnedActiveLease({
    workingDataset,
    actor,
    editorSessionId: body.editorSessionId,
    now,
  });

  const storage = await writeMaterialisedCsv({
    workingDataset,
    fields,
    customFields,
    now,
  });

  const materialisedWorkingDataset =
    await datasetRepository.updateWorkingDatasetStorageRecord({
      PlatformDataWorkingDataset,
      workingDatasetId: params.workingDatasetId,
      customerId: executionContext.customerId,
      profileId: body.profileId,
      storage,
      actor,
    });

  const activity = await datasetRepository.createWorkingDatasetActivityRecord({
    PlatformDataWorkingDatasetActivity,
    activity: {
      customerId: executionContext.customerId,
      profileId: body.profileId,
      workingDatasetId: params.workingDatasetId,
      activityType: "working_dataset_materialised",
      stepNumber: body.stepNumber || workingDataset.currentStepNumber,
      summary: "Materialised working dataset from projection configuration",
      details: {
        editorSessionId: body.editorSessionId,
        fields,
        customFields,
        storagePath: storage.storagePath,
        storedFileName: storage.storedFileName,
        rowsCount: storage.rowsCount,
        headersCount: storage.headersCount,
      },
      relatedCapability: "transformation",
      relatedRecordId: params.workingDatasetId,
      actor,
    },
  });

  return {
    success: true,
    workingDataset: materialisedWorkingDataset,
    activity,
  };
}

module.exports = {
  materialiseWorkingDataset,
};

const { withCustomerTransaction } = require("@/helpers/customerTransaction");

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

function normaliseInteger(value, fieldName) {
  const numberValue = Number(value);

  if (!Number.isInteger(numberValue) || numberValue < 0) {
    throw createError(`${fieldName} must be a non-negative integer.`);
  }

  return numberValue;
}

function normaliseTimestamp(value) {
  return value instanceof Date ? value.toISOString() : value;
}

function normaliseActiveEditor(plainRecord) {
  if (!plainRecord.activeEditorSessionId) {
    return null;
  }

  const expiresAt = normaliseTimestamp(plainRecord.activeEditorExpiresAt);

  if (expiresAt && new Date(expiresAt).getTime() <= Date.now()) {
    return null;
  }

  return {
    userId: plainRecord.activeEditorUserId,
    sessionId: plainRecord.activeEditorSessionId,
    startedAt: normaliseTimestamp(plainRecord.activeEditorStartedAt),
    lastSeenAt: normaliseTimestamp(plainRecord.activeEditorLastSeenAt),
    expiresAt,
  };
}

function normaliseDatasetRecord(record) {
  const plainRecord =
    typeof record?.get === "function" ? record.get({ plain: true }) : record;

  requireValue(plainRecord, "dataset record is required.");

  return {
    datasetId: plainRecord.id,
    customerId: plainRecord.customerId,
    profileId: plainRecord.profileId,
    datasetType: plainRecord.datasetType,
    sourceType: plainRecord.sourceType,
    sourceName: plainRecord.sourceName,
    originalFileName: plainRecord.originalFileName,
    storedFileName: plainRecord.storedFileName,
    storagePath: plainRecord.storagePath,
    mimeType: plainRecord.mimeType,
    fileSize: normaliseInteger(plainRecord.fileSize, "fileSize"),
    headers: plainRecord.headers,
    headersCount: normaliseInteger(plainRecord.headersCount, "headersCount"),
    rowsCount: normaliseInteger(plainRecord.rowsCount, "rowsCount"),
    status: plainRecord.status,
    isImmutable: true,
    createdAt:
      plainRecord.createdAt instanceof Date
        ? plainRecord.createdAt.toISOString()
        : plainRecord.createdAt,
  };
}

function normaliseWorkingDatasetRecord(record) {
  const plainRecord =
    typeof record?.get === "function" ? record.get({ plain: true }) : record;

  requireValue(plainRecord, "working dataset record is required.");
  requireValue(
    plainRecord.sourceDatasetId,
    "working dataset sourceDatasetId is required.",
  );
  requireValue(plainRecord.lineage, "working dataset lineage is required.");

  return {
    workingDatasetId: plainRecord.id,
    sourceDatasetId: plainRecord.sourceDatasetId,
    customerId: plainRecord.customerId,
    profileId: plainRecord.profileId,
    workingName: plainRecord.workingName,
    datasetType: plainRecord.datasetType,
    status: plainRecord.status,
    currentStepNumber: plainRecord.currentStepNumber,
    storagePath: plainRecord.storagePath,
    storedFileName: plainRecord.storedFileName,
    mimeType: plainRecord.mimeType,
    fileSize: normaliseInteger(plainRecord.fileSize, "fileSize"),
    headers: plainRecord.headers,
    headersCount: normaliseInteger(plainRecord.headersCount, "headersCount"),
    rowsCount: normaliseInteger(plainRecord.rowsCount, "rowsCount"),
    lineage: plainRecord.lineage,
    meta: plainRecord.meta,
    activeEditor: normaliseActiveEditor(plainRecord),
    finalisedAt:
      plainRecord.finalisedAt instanceof Date
        ? plainRecord.finalisedAt.toISOString()
        : plainRecord.finalisedAt,
    finalisedBy: plainRecord.finalisedBy,
    createdAt:
      plainRecord.createdAt instanceof Date
        ? plainRecord.createdAt.toISOString()
        : plainRecord.createdAt,
    updatedAt:
      plainRecord.updatedAt instanceof Date
        ? plainRecord.updatedAt.toISOString()
        : plainRecord.updatedAt,
  };
}

function normaliseWorkingDatasetActivityRecord(record) {
  const plainRecord =
    typeof record?.get === "function" ? record.get({ plain: true }) : record;

  requireValue(plainRecord, "working dataset activity record is required.");

  return {
    activityId: plainRecord.id,
    customerId: plainRecord.customerId,
    profileId: plainRecord.profileId,
    workingDatasetId: plainRecord.workingDatasetId,
    activityType: plainRecord.activityType,
    stepNumber: plainRecord.stepNumber,
    summary: plainRecord.summary,
    details: plainRecord.details,
    relatedCapability: plainRecord.relatedCapability,
    relatedRecordId: plainRecord.relatedRecordId,
    createdBy: plainRecord.createdBy,
    createdAt:
      plainRecord.createdAt instanceof Date
        ? plainRecord.createdAt.toISOString()
        : plainRecord.createdAt,
  };
}

async function createDatasetRecord({ PlatformDataDataset, dataset }) {
  requireValue(PlatformDataDataset, "PlatformDataDataset model is required.");
  requireValue(dataset, "dataset is required for persistence.");
  requireValue(dataset.datasetId, "datasetId is required for persistence.");
  requireValue(dataset.customerId, "customerId is required for persistence.");
  requireValue(dataset.profileId, "profileId is required for persistence.");
  requireValue(dataset.datasetType, "datasetType is required for persistence.");
  requireValue(dataset.sourceType, "sourceType is required for persistence.");
  requireValue(dataset.sourceName, "sourceName is required for persistence.");
  requireValue(
    dataset.originalFileName,
    "originalFileName is required for persistence.",
  );
  requireValue(dataset.storagePath, "storagePath is required for persistence.");
  requireValue(dataset.mimeType, "mimeType is required for persistence.");
  requireValue(dataset.fileSize, "fileSize is required for persistence.");
  requireValue(dataset.headers, "headers is required for persistence.");
  requireValue(
    dataset.headersCount,
    "headersCount is required for persistence.",
  );
  requireValue(dataset.rowsCount, "rowsCount is required for persistence.");
  requireValue(dataset.status, "status is required for persistence.");
  requireValue(dataset.actor?.id, "actor id is required for persistence.");

  const storedFileName = `${dataset.datasetId}.csv`;

  const payload = {
    id: dataset.datasetId,
    customerId: dataset.customerId,
    profileId: dataset.profileId,
    datasetType: dataset.datasetType,
    sourceType: dataset.sourceType,
    sourceName: dataset.sourceName,
    originalFileName: dataset.originalFileName,
    storedFileName,
    storagePath: dataset.storagePath,
    mimeType: dataset.mimeType,
    fileSize: dataset.fileSize,
    headers: dataset.headers,
    headersCount: dataset.headersCount,
    rowsCount: dataset.rowsCount,
    status: dataset.status,
    detectedCoverage: dataset.detectedCoverage || {},
    meta: {
      ...(dataset.meta || {}),
      headers: dataset.headers,
      rowsCount: dataset.rowsCount,
      uploadedAt: dataset.createdAt,
    },
    uploadedBy: dataset.actor.id,
    createdBy: dataset.actor.id,
    updatedBy: dataset.actor.id,
  };

  return withCustomerTransaction(dataset.customerId, async (transaction) => {
    const record = await PlatformDataDataset.create(payload, { transaction });
    return normaliseDatasetRecord(record);
  });
}

async function getDatasetRecordById({
  PlatformDataDataset,
  datasetId,
  customerId,
  profileId,
}) {
  requireValue(PlatformDataDataset, "PlatformDataDataset model is required.");
  requireValue(datasetId, "datasetId is required.");
  requireValue(customerId, "customerId is required.");
  requireValue(profileId, "profileId is required.");

  return withCustomerTransaction(customerId, async (transaction) => {
    const record = await PlatformDataDataset.findOne({
      where: {
        id: datasetId,
        customerId,
        profileId,
      },
      transaction,
    });

    if (!record) {
      throw createError(
        "source dataset was not found for working data creation.",
        404,
      );
    }

    return normaliseDatasetRecord(record);
  });
}

async function getWorkingDatasetRecordById({
  PlatformDataWorkingDataset,
  workingDatasetId,
  customerId,
  profileId,
}) {
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required.",
  );
  requireValue(workingDatasetId, "workingDatasetId is required.");
  requireValue(customerId, "customerId is required.");
  requireValue(profileId, "profileId is required.");

  return withCustomerTransaction(customerId, async (transaction) => {
    const record = await PlatformDataWorkingDataset.findOne({
      where: {
        id: workingDatasetId,
        customerId,
        profileId,
      },
      transaction,
    });

    if (!record) {
      throw createError("working dataset was not found.", 404);
    }

    return normaliseWorkingDatasetRecord(record);
  });
}

async function listWorkingDatasetRecords({
  PlatformDataWorkingDataset,
  customerId,
  profileId,
}) {
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required.",
  );
  requireValue(customerId, "customerId is required.");
  requireValue(profileId, "profileId is required.");

  return withCustomerTransaction(customerId, async (transaction) => {
    const records = await PlatformDataWorkingDataset.findAll({
      where: {
        customerId,
        profileId,
      },
      order: [["updatedAt", "DESC"]],
      transaction,
    });

    return records.map(normaliseWorkingDatasetRecord);
  });
}

async function listWorkingDatasetActivityRecords({
  PlatformDataWorkingDatasetActivity,
  workingDatasetId,
  customerId,
  profileId,
}) {
  requireValue(
    PlatformDataWorkingDatasetActivity,
    "PlatformDataWorkingDatasetActivity model is required.",
  );
  requireValue(workingDatasetId, "workingDatasetId is required.");
  requireValue(customerId, "customerId is required.");
  requireValue(profileId, "profileId is required.");

  return withCustomerTransaction(customerId, async (transaction) => {
    const records = await PlatformDataWorkingDatasetActivity.findAll({
      where: {
        workingDatasetId,
        customerId,
        profileId,
      },
      order: [["createdAt", "ASC"]],
      transaction,
    });

    return records.map(normaliseWorkingDatasetActivityRecord);
  });
}

async function createWorkingDatasetRecord({
  PlatformDataWorkingDataset,
  sourceDataset,
  workingDataset,
}) {
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required.",
  );
  requireValue(sourceDataset, "sourceDataset is required.");
  requireValue(workingDataset, "workingDataset is required.");
  requireValue(
    workingDataset.workingDatasetId,
    "workingDatasetId is required for persistence.",
  );
  requireValue(
    workingDataset.sourceDatasetId,
    "sourceDatasetId is required for persistence.",
  );
  requireValue(
    workingDataset.customerId,
    "customerId is required for persistence.",
  );
  requireValue(
    workingDataset.profileId,
    "profileId is required for persistence.",
  );
  requireValue(
    workingDataset.workingName,
    "workingName is required for persistence.",
  );
  requireValue(
    workingDataset.actor?.id,
    "actor id is required for persistence.",
  );

  const lineage = {
    sourceDatasetId: workingDataset.sourceDatasetId,
    createdFrom: "immutable_dataset",
  };

  const payload = {
    id: workingDataset.workingDatasetId,
    customerId: workingDataset.customerId,
    profileId: workingDataset.profileId,
    sourceDatasetId: workingDataset.sourceDatasetId,
    workingName: workingDataset.workingName,
    datasetType: sourceDataset.datasetType,
    status: "in_progress",
    currentStepNumber: workingDataset.currentStepNumber,
    storedFileName: sourceDataset.storedFileName,
    storagePath: sourceDataset.storagePath,
    mimeType: sourceDataset.mimeType,
    fileSize: sourceDataset.fileSize,
    headers: sourceDataset.headers,
    headersCount: sourceDataset.headersCount,
    rowsCount: sourceDataset.rowsCount,
    lineage,
    meta: {
      sourceDatasetId: workingDataset.sourceDatasetId,
      sourceOriginalFileName: sourceDataset.originalFileName,
    },
    createdBy: workingDataset.actor.id,
    updatedBy: workingDataset.actor.id,
  };

  return withCustomerTransaction(
    workingDataset.customerId,
    async (transaction) => {
      const record = await PlatformDataWorkingDataset.create(payload, {
        transaction,
      });
      return normaliseWorkingDatasetRecord(record);
    },
  );
}

async function createWorkingDatasetActivityRecord({
  PlatformDataWorkingDatasetActivity,
  activity,
}) {
  requireValue(
    PlatformDataWorkingDatasetActivity,
    "PlatformDataWorkingDatasetActivity model is required.",
  );
  requireValue(activity, "activity is required for persistence.");
  requireValue(activity.customerId, "customerId is required for persistence.");
  requireValue(activity.profileId, "profileId is required for persistence.");
  requireValue(
    activity.workingDatasetId,
    "workingDatasetId is required for persistence.",
  );
  requireValue(
    activity.activityType,
    "activityType is required for persistence.",
  );
  requireValue(activity.summary, "summary is required for persistence.");
  requireValue(activity.actor?.id, "actor id is required for persistence.");

  const payload = {
    customerId: activity.customerId,
    profileId: activity.profileId,
    workingDatasetId: activity.workingDatasetId,
    activityType: activity.activityType,
    stepNumber: activity.stepNumber,
    summary: activity.summary,
    details: activity.details || {},
    relatedCapability: activity.relatedCapability,
    relatedRecordId: activity.relatedRecordId,
    createdBy: activity.actor.id,
  };

  return withCustomerTransaction(activity.customerId, async (transaction) => {
    const record = await PlatformDataWorkingDatasetActivity.create(payload, {
      transaction,
    });
    return normaliseWorkingDatasetActivityRecord(record);
  });
}

async function updateWorkingDatasetEditLease({
  PlatformDataWorkingDataset,
  workingDatasetId,
  customerId,
  profileId,
  lease,
}) {
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required.",
  );
  requireValue(workingDatasetId, "workingDatasetId is required.");
  requireValue(customerId, "customerId is required.");
  requireValue(profileId, "profileId is required.");
  requireValue(lease, "lease is required.");
  requireValue(
    lease.activeEditorUserId,
    "activeEditorUserId is required for edit lease persistence.",
  );
  requireValue(
    lease.activeEditorSessionId,
    "activeEditorSessionId is required for edit lease persistence.",
  );
  requireValue(
    lease.activeEditorStartedAt,
    "activeEditorStartedAt is required for edit lease persistence.",
  );
  requireValue(
    lease.activeEditorLastSeenAt,
    "activeEditorLastSeenAt is required for edit lease persistence.",
  );
  requireValue(
    lease.activeEditorExpiresAt,
    "activeEditorExpiresAt is required for edit lease persistence.",
  );
  requireValue(
    lease.updatedBy,
    "updatedBy is required for edit lease persistence.",
  );

  return withCustomerTransaction(customerId, async (transaction) => {
    const record = await PlatformDataWorkingDataset.findOne({
      where: {
        id: workingDatasetId,
        customerId,
        profileId,
      },
      transaction,
    });

    if (!record) {
      throw createError("working dataset was not found.", 404);
    }

    const updatedRecord = await record.update(
      {
        activeEditorUserId: lease.activeEditorUserId,
        activeEditorSessionId: lease.activeEditorSessionId,
        activeEditorStartedAt: lease.activeEditorStartedAt,
        activeEditorLastSeenAt: lease.activeEditorLastSeenAt,
        activeEditorExpiresAt: lease.activeEditorExpiresAt,
        updatedBy: lease.updatedBy,
      },
      { transaction },
    );

    return normaliseWorkingDatasetRecord(updatedRecord);
  });
}

async function clearWorkingDatasetEditLease({
  PlatformDataWorkingDataset,
  workingDatasetId,
  customerId,
  profileId,
  actor,
}) {
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required.",
  );
  requireValue(workingDatasetId, "workingDatasetId is required.");
  requireValue(customerId, "customerId is required.");
  requireValue(profileId, "profileId is required.");
  requireValue(actor?.id, "actor id is required for edit lease release.");

  return withCustomerTransaction(customerId, async (transaction) => {
    const record = await PlatformDataWorkingDataset.findOne({
      where: {
        id: workingDatasetId,
        customerId,
        profileId,
      },
      transaction,
    });

    if (!record) {
      throw createError("working dataset was not found.", 404);
    }

    const updatedRecord = await record.update(
      {
        activeEditorUserId: null,
        activeEditorSessionId: null,
        activeEditorStartedAt: null,
        activeEditorLastSeenAt: null,
        activeEditorExpiresAt: null,
        updatedBy: actor.id,
      },
      { transaction },
    );

    return normaliseWorkingDatasetRecord(updatedRecord);
  });
}

async function finaliseWorkingDatasetRecord({
  PlatformDataWorkingDataset,
  workingDatasetId,
  customerId,
  profileId,
  actor,
  finalisedAt,
}) {
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required.",
  );
  requireValue(workingDatasetId, "workingDatasetId is required.");
  requireValue(customerId, "customerId is required.");
  requireValue(profileId, "profileId is required.");
  requireValue(
    actor?.id,
    "actor id is required for working dataset finalisation.",
  );
  requireValue(
    finalisedAt,
    "finalisedAt is required for working dataset finalisation.",
  );

  return withCustomerTransaction(customerId, async (transaction) => {
    const record = await PlatformDataWorkingDataset.findOne({
      where: {
        id: workingDatasetId,
        customerId,
        profileId,
      },
      transaction,
    });

    if (!record) {
      throw createError("working dataset was not found.", 404);
    }

    const updatedRecord = await record.update(
      {
        status: "final",
        finalisedAt,
        finalisedBy: actor.id,
        activeEditorUserId: null,
        activeEditorSessionId: null,
        activeEditorStartedAt: null,
        activeEditorLastSeenAt: null,
        activeEditorExpiresAt: null,
        updatedBy: actor.id,
      },
      { transaction },
    );

    return normaliseWorkingDatasetRecord(updatedRecord);
  });
}

async function updateWorkingDatasetStorageRecord({
  PlatformDataWorkingDataset,
  workingDatasetId,
  customerId,
  profileId,
  storage,
  actor,
}) {
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required.",
  );
  requireValue(workingDatasetId, "workingDatasetId is required.");
  requireValue(customerId, "customerId is required.");
  requireValue(profileId, "profileId is required.");
  requireValue(storage, "storage is required for working dataset persistence.");
  requireValue(
    storage.storagePath,
    "storagePath is required for working dataset persistence.",
  );
  requireValue(
    storage.storedFileName,
    "storedFileName is required for working dataset persistence.",
  );
  requireValue(
    storage.mimeType,
    "mimeType is required for working dataset persistence.",
  );
  requireValue(
    storage.fileSize,
    "fileSize is required for working dataset persistence.",
  );
  requireValue(
    storage.headers,
    "headers is required for working dataset persistence.",
  );
  requireValue(
    storage.headersCount,
    "headersCount is required for working dataset persistence.",
  );
  requireValue(
    storage.rowsCount,
    "rowsCount is required for working dataset persistence.",
  );
  requireValue(
    actor?.id,
    "actor id is required for working dataset persistence.",
  );

  return withCustomerTransaction(customerId, async (transaction) => {
    const record = await PlatformDataWorkingDataset.findOne({
      where: {
        id: workingDatasetId,
        customerId,
        profileId,
      },
      transaction,
    });

    if (!record) {
      throw createError("working dataset was not found.", 404);
    }

    const updatedRecord = await record.update(
      {
        storagePath: storage.storagePath,
        storedFileName: storage.storedFileName,
        mimeType: storage.mimeType,
        fileSize: storage.fileSize,
        headers: storage.headers,
        headersCount: storage.headersCount,
        rowsCount: storage.rowsCount,
        meta: storage.meta || {},
        updatedBy: actor.id,
      },
      { transaction },
    );

    return normaliseWorkingDatasetRecord(updatedRecord);
  });
}

module.exports = {
  clearWorkingDatasetEditLease,
  createDatasetRecord,
  createWorkingDatasetActivityRecord,
  createWorkingDatasetRecord,
  finaliseWorkingDatasetRecord,
  getDatasetRecordById,
  getWorkingDatasetRecordById,
  listWorkingDatasetActivityRecords,
  listWorkingDatasetRecords,
  normaliseDatasetRecord,
  normaliseWorkingDatasetActivityRecord,
  normaliseWorkingDatasetRecord,
  updateWorkingDatasetEditLease,
  updateWorkingDatasetStorageRecord,
};

const { getNanoid } = require("@/helpers/nanoid_helper");
const { scanFile } = require("@/middleware/virus-scan");

const acquisitionService = require("@/platform/data/acquisition.service");
const auditService = require("@/platform/audit/audit.service");
const datasetRepository = require("@/platform/data/dataset.repository");
const datasetService = require("@/platform/data/dataset.service");
const fileStorageService = require("@/platform/data/file-storage.service");
const securityService = require("@/platform/security/security.service");

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

// --- Working dataset edit lease helpers ---
const EDIT_LEASE_DURATION_MINUTES = Number(
  process.env.PLATFORM_DATA_EDIT_LEASE_DURATION_MINUTES || 30,
);

function buildActor(executionContext) {
  return {
    id: executionContext.actorId,
    role: executionContext.role,
    customerId: executionContext.customerId,
  };
}

function addMinutes(date, minutes) {
  return new Date(date.getTime() + minutes * 60 * 1000);
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
    throw createError("final working datasets cannot be edited.", 409);
  }
}

function requireLeaseAvailableForAcquire({
  workingDataset,
  actor,
  editorSessionId,
  now,
}) {
  requireEditableWorkingDataset(workingDataset);

  if (
    isLeaseActive(workingDataset, now) &&
    !isLeaseOwnedByActor({ workingDataset, actor, editorSessionId })
  ) {
    throw createError("working dataset is currently being edited.", 409);
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

async function createDataset({
  executionContext,
  body,
  file,
  PlatformDataDataset,
}) {
  requireValue(
    executionContext,
    "executionContext is required for Data dataset creation.",
  );
  requireValue(
    PlatformDataDataset,
    "PlatformDataDataset model is required for Data dataset creation.",
  );

  const command = acquisitionService.buildDatasetCreationCommand({
    executionContext,
    body,
    file,
  });

  const datasetId = getNanoid(10);

  let securityObservation;

  try {
    securityObservation = securityService.enforceDataDatasetCreation({
      datasetId,
      actor: command.actor,
      customerId: command.customerId,
    });
  } catch (error) {
    await auditService.recordDataDatasetAudit({
      datasetId,
      outcome: "denied",
      actor: command.actor,
      securityObservation: error.securityObservation,
      error,
    });

    throw error;
  }

  try {
    await scanFile(command.file.path, command.file.originalFileName);
  } catch (error) {
    await auditService.recordDataDatasetAudit({
      datasetId,
      outcome: "denied",
      actor: command.actor,
      securityObservation,
      error,
    });

    throw error;
  }

  const storageResult = await fileStorageService.storeDatasetFile({
    customerId: command.customerId,
    datasetId,
    sourceFilePath: command.file.path,
  });

  const datasetResponse = datasetService.createImmutableDatasetFromCommand({
    command,
    datasetId,
    storageResult,
  });

  const persistedDataset = await datasetRepository.createDatasetRecord({
    PlatformDataDataset,
    dataset: {
      ...datasetResponse.dataset,
      actor: command.actor,
    },
  });

  await auditService.recordDataDatasetAudit({
    datasetId,
    outcome: "success",
    actor: command.actor,
    securityObservation,
  });

  return {
    success: true,
    dataset: persistedDataset,
  };
}

async function createWorkingDataset({
  executionContext,
  body,
  PlatformDataDataset,
  PlatformDataWorkingDataset,
  PlatformDataWorkingDatasetActivity,
}) {
  requireValue(
    executionContext,
    "executionContext is required for working dataset creation.",
  );
  requireValue(
    PlatformDataDataset,
    "PlatformDataDataset model is required for working dataset creation.",
  );
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required for working dataset creation.",
  );
  requireValue(
    PlatformDataWorkingDatasetActivity,
    "PlatformDataWorkingDatasetActivity model is required for working dataset creation.",
  );
  requireValue(body, "body is required for working dataset creation.");
  requireValue(
    body.sourceDatasetId,
    "sourceDatasetId is required for working dataset creation.",
  );
  requireValue(
    body.profileId,
    "profileId is required for working dataset creation.",
  );
  requireValue(
    body.workingName,
    "workingName is required for working dataset creation.",
  );
  requireValue(
    executionContext.customerId,
    "customerId is required for working dataset creation.",
  );

  const workingDatasetId = getNanoid(10);
  const actor = buildActor(executionContext);

  let securityObservation;

  try {
    securityObservation = securityService.enforceDataDatasetCreation({
      datasetId: workingDatasetId,
      actor,
      customerId: executionContext.customerId,
    });
  } catch (error) {
    await auditService.recordDataDatasetAudit({
      datasetId: workingDatasetId,
      outcome: "denied",
      actor,
      securityObservation: error.securityObservation,
      error,
    });

    throw error;
  }

  const sourceDataset = await datasetRepository.getDatasetRecordById({
    PlatformDataDataset,
    datasetId: body.sourceDatasetId,
    customerId: executionContext.customerId,
    profileId: body.profileId,
  });

  if (sourceDataset.status !== "available") {
    throw createError(
      "source dataset is not available for working data creation.",
      409,
    );
  }

  const workingDataset = await datasetRepository.createWorkingDatasetRecord({
    PlatformDataWorkingDataset,
    sourceDataset,
    workingDataset: {
      workingDatasetId,
      sourceDatasetId: sourceDataset.datasetId,
      customerId: executionContext.customerId,
      profileId: body.profileId,
      workingName: body.workingName,
      currentStepNumber: body.currentStepNumber || 1,
      actor,
    },
  });

  const activity = await datasetRepository.createWorkingDatasetActivityRecord({
    PlatformDataWorkingDatasetActivity,
    activity: {
      customerId: executionContext.customerId,
      profileId: body.profileId,
      workingDatasetId,
      activityType: "working_dataset_created",
      stepNumber: workingDataset.currentStepNumber,
      summary: `Created working dataset ${body.workingName}`,
      details: {
        sourceDatasetId: sourceDataset.datasetId,
      },
      relatedCapability: "data",
      relatedRecordId: workingDatasetId,
      actor,
    },
  });

  await auditService.recordDataDatasetAudit({
    datasetId: workingDatasetId,
    outcome: "success",
    actor,
    securityObservation,
  });

  return {
    success: true,
    workingDataset,
    activity,
  };
}

async function listWorkingDatasets({
  executionContext,
  query,
  PlatformDataWorkingDataset,
}) {
  requireValue(
    executionContext,
    "executionContext is required for working dataset listing.",
  );
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required for working dataset listing.",
  );
  requireValue(query, "query is required for working dataset listing.");
  requireValue(
    query.profileId,
    "profileId is required for working dataset listing.",
  );
  requireValue(
    executionContext.customerId,
    "customerId is required for working dataset listing.",
  );

  const workingDatasets = await datasetRepository.listWorkingDatasetRecords({
    PlatformDataWorkingDataset,
    customerId: executionContext.customerId,
    profileId: query.profileId,
  });

  return {
    success: true,
    workingDatasets,
  };
}

async function getWorkingDataset({
  executionContext,
  params,
  query,
  PlatformDataWorkingDataset,
}) {
  requireValue(
    executionContext,
    "executionContext is required for working dataset detail retrieval.",
  );
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required for working dataset detail retrieval.",
  );
  requireValue(
    params,
    "params are required for working dataset detail retrieval.",
  );
  requireValue(
    params.workingDatasetId,
    "workingDatasetId is required for working dataset detail retrieval.",
  );
  requireValue(
    query,
    "query is required for working dataset detail retrieval.",
  );
  requireValue(
    query.profileId,
    "profileId is required for working dataset detail retrieval.",
  );
  requireValue(
    executionContext.customerId,
    "customerId is required for working dataset detail retrieval.",
  );

  const workingDataset = await datasetRepository.getWorkingDatasetRecordById({
    PlatformDataWorkingDataset,
    workingDatasetId: params.workingDatasetId,
    customerId: executionContext.customerId,
    profileId: query.profileId,
  });

  return {
    success: true,
    workingDataset,
  };
}

async function listWorkingDatasetActivity({
  executionContext,
  params,
  query,
  PlatformDataWorkingDatasetActivity,
}) {
  requireValue(
    executionContext,
    "executionContext is required for working dataset activity listing.",
  );
  requireValue(
    PlatformDataWorkingDatasetActivity,
    "PlatformDataWorkingDatasetActivity model is required for working dataset activity listing.",
  );
  requireValue(
    params,
    "params are required for working dataset activity listing.",
  );
  requireValue(
    params.workingDatasetId,
    "workingDatasetId is required for working dataset activity listing.",
  );
  requireValue(
    query,
    "query is required for working dataset activity listing.",
  );
  requireValue(
    query.profileId,
    "profileId is required for working dataset activity listing.",
  );
  requireValue(
    executionContext.customerId,
    "customerId is required for working dataset activity listing.",
  );

  const activities = await datasetRepository.listWorkingDatasetActivityRecords({
    PlatformDataWorkingDatasetActivity,
    workingDatasetId: params.workingDatasetId,
    customerId: executionContext.customerId,
    profileId: query.profileId,
  });

  return {
    success: true,
    activities,
  };
}

async function acquireWorkingDatasetEditLease({
  executionContext,
  params,
  body,
  PlatformDataWorkingDataset,
  PlatformDataWorkingDatasetActivity,
}) {
  requireValue(
    executionContext,
    "executionContext is required for working dataset edit lease acquisition.",
  );
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required for working dataset edit lease acquisition.",
  );
  requireValue(
    PlatformDataWorkingDatasetActivity,
    "PlatformDataWorkingDatasetActivity model is required for working dataset edit lease acquisition.",
  );
  requireValue(
    params,
    "params are required for working dataset edit lease acquisition.",
  );
  requireValue(
    params.workingDatasetId,
    "workingDatasetId is required for working dataset edit lease acquisition.",
  );
  requireValue(
    body,
    "body is required for working dataset edit lease acquisition.",
  );
  requireValue(
    body.profileId,
    "profileId is required for working dataset edit lease acquisition.",
  );
  requireValue(
    body.editorSessionId,
    "editorSessionId is required for working dataset edit lease acquisition.",
  );
  requireValue(
    executionContext.customerId,
    "customerId is required for working dataset edit lease acquisition.",
  );

  const actor = buildActor(executionContext);
  const now = new Date();
  const expiresAt = addMinutes(now, EDIT_LEASE_DURATION_MINUTES);

  const existingWorkingDataset =
    await datasetRepository.getWorkingDatasetRecordById({
      PlatformDataWorkingDataset,
      workingDatasetId: params.workingDatasetId,
      customerId: executionContext.customerId,
      profileId: body.profileId,
    });

  requireLeaseAvailableForAcquire({
    workingDataset: existingWorkingDataset,
    actor,
    editorSessionId: body.editorSessionId,
    now,
  });

  const workingDataset = await datasetRepository.updateWorkingDatasetEditLease({
    PlatformDataWorkingDataset,
    workingDatasetId: params.workingDatasetId,
    customerId: executionContext.customerId,
    profileId: body.profileId,
    lease: {
      activeEditorUserId: actor.id,
      activeEditorSessionId: body.editorSessionId,
      activeEditorStartedAt: now,
      activeEditorLastSeenAt: now,
      activeEditorExpiresAt: expiresAt,
      updatedBy: actor.id,
    },
  });

  const activity = await datasetRepository.createWorkingDatasetActivityRecord({
    PlatformDataWorkingDatasetActivity,
    activity: {
      customerId: executionContext.customerId,
      profileId: body.profileId,
      workingDatasetId: params.workingDatasetId,
      activityType: "edit_lease_acquired",
      stepNumber: workingDataset.currentStepNumber,
      summary: "Acquired working dataset edit lease",
      details: {
        editorSessionId: body.editorSessionId,
        expiresAt: expiresAt.toISOString(),
      },
      relatedCapability: "data",
      relatedRecordId: params.workingDatasetId,
      actor,
    },
  });

  return {
    success: true,
    workingDataset,
    activity,
  };
}

async function renewWorkingDatasetEditLease({
  executionContext,
  params,
  body,
  PlatformDataWorkingDataset,
  PlatformDataWorkingDatasetActivity,
}) {
  requireValue(
    executionContext,
    "executionContext is required for working dataset edit lease renewal.",
  );
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required for working dataset edit lease renewal.",
  );
  requireValue(
    PlatformDataWorkingDatasetActivity,
    "PlatformDataWorkingDatasetActivity model is required for working dataset edit lease renewal.",
  );
  requireValue(
    params,
    "params are required for working dataset edit lease renewal.",
  );
  requireValue(
    params.workingDatasetId,
    "workingDatasetId is required for working dataset edit lease renewal.",
  );
  requireValue(
    body,
    "body is required for working dataset edit lease renewal.",
  );
  requireValue(
    body.profileId,
    "profileId is required for working dataset edit lease renewal.",
  );
  requireValue(
    body.editorSessionId,
    "editorSessionId is required for working dataset edit lease renewal.",
  );
  requireValue(
    executionContext.customerId,
    "customerId is required for working dataset edit lease renewal.",
  );

  const actor = buildActor(executionContext);
  const now = new Date();
  const expiresAt = addMinutes(now, EDIT_LEASE_DURATION_MINUTES);

  const existingWorkingDataset =
    await datasetRepository.getWorkingDatasetRecordById({
      PlatformDataWorkingDataset,
      workingDatasetId: params.workingDatasetId,
      customerId: executionContext.customerId,
      profileId: body.profileId,
    });

  requireOwnedActiveLease({
    workingDataset: existingWorkingDataset,
    actor,
    editorSessionId: body.editorSessionId,
    now,
  });

  const workingDataset = await datasetRepository.updateWorkingDatasetEditLease({
    PlatformDataWorkingDataset,
    workingDatasetId: params.workingDatasetId,
    customerId: executionContext.customerId,
    profileId: body.profileId,
    lease: {
      activeEditorUserId: actor.id,
      activeEditorSessionId: body.editorSessionId,
      activeEditorStartedAt: existingWorkingDataset.activeEditor.startedAt,
      activeEditorLastSeenAt: now,
      activeEditorExpiresAt: expiresAt,
      updatedBy: actor.id,
    },
  });

  const activity = await datasetRepository.createWorkingDatasetActivityRecord({
    PlatformDataWorkingDatasetActivity,
    activity: {
      customerId: executionContext.customerId,
      profileId: body.profileId,
      workingDatasetId: params.workingDatasetId,
      activityType: "edit_lease_renewed",
      stepNumber: workingDataset.currentStepNumber,
      summary: "Renewed working dataset edit lease",
      details: {
        editorSessionId: body.editorSessionId,
        expiresAt: expiresAt.toISOString(),
      },
      relatedCapability: "data",
      relatedRecordId: params.workingDatasetId,
      actor,
    },
  });

  return {
    success: true,
    workingDataset,
    activity,
  };
}

async function releaseWorkingDatasetEditLease({
  executionContext,
  params,
  body,
  PlatformDataWorkingDataset,
  PlatformDataWorkingDatasetActivity,
}) {
  requireValue(
    executionContext,
    "executionContext is required for working dataset edit lease release.",
  );
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required for working dataset edit lease release.",
  );
  requireValue(
    PlatformDataWorkingDatasetActivity,
    "PlatformDataWorkingDatasetActivity model is required for working dataset edit lease release.",
  );
  requireValue(
    params,
    "params are required for working dataset edit lease release.",
  );
  requireValue(
    params.workingDatasetId,
    "workingDatasetId is required for working dataset edit lease release.",
  );
  requireValue(
    body,
    "body is required for working dataset edit lease release.",
  );
  requireValue(
    body.profileId,
    "profileId is required for working dataset edit lease release.",
  );
  requireValue(
    body.editorSessionId,
    "editorSessionId is required for working dataset edit lease release.",
  );
  requireValue(
    executionContext.customerId,
    "customerId is required for working dataset edit lease release.",
  );

  const actor = buildActor(executionContext);
  const now = new Date();

  const existingWorkingDataset =
    await datasetRepository.getWorkingDatasetRecordById({
      PlatformDataWorkingDataset,
      workingDatasetId: params.workingDatasetId,
      customerId: executionContext.customerId,
      profileId: body.profileId,
    });

  requireOwnedActiveLease({
    workingDataset: existingWorkingDataset,
    actor,
    editorSessionId: body.editorSessionId,
    now,
  });

  const workingDataset = await datasetRepository.clearWorkingDatasetEditLease({
    PlatformDataWorkingDataset,
    workingDatasetId: params.workingDatasetId,
    customerId: executionContext.customerId,
    profileId: body.profileId,
    actor,
  });

  const activity = await datasetRepository.createWorkingDatasetActivityRecord({
    PlatformDataWorkingDatasetActivity,
    activity: {
      customerId: executionContext.customerId,
      profileId: body.profileId,
      workingDatasetId: params.workingDatasetId,
      activityType: "edit_lease_released",
      stepNumber: workingDataset.currentStepNumber,
      summary: "Released working dataset edit lease",
      details: {
        editorSessionId: body.editorSessionId,
      },
      relatedCapability: "data",
      relatedRecordId: params.workingDatasetId,
      actor,
    },
  });

  return {
    success: true,
    workingDataset,
    activity,
  };
}

async function finaliseWorkingDataset({
  executionContext,
  params,
  body,
  PlatformDataWorkingDataset,
  PlatformDataWorkingDatasetActivity,
}) {
  requireValue(
    executionContext,
    "executionContext is required for working dataset finalisation.",
  );
  requireValue(
    PlatformDataWorkingDataset,
    "PlatformDataWorkingDataset model is required for working dataset finalisation.",
  );
  requireValue(
    PlatformDataWorkingDatasetActivity,
    "PlatformDataWorkingDatasetActivity model is required for working dataset finalisation.",
  );
  requireValue(params, "params are required for working dataset finalisation.");
  requireValue(
    params.workingDatasetId,
    "workingDatasetId is required for working dataset finalisation.",
  );
  requireValue(body, "body is required for working dataset finalisation.");
  requireValue(
    body.profileId,
    "profileId is required for working dataset finalisation.",
  );
  requireValue(
    body.editorSessionId,
    "editorSessionId is required for working dataset finalisation.",
  );
  requireValue(
    executionContext.customerId,
    "customerId is required for working dataset finalisation.",
  );

  const actor = buildActor(executionContext);
  const now = new Date();

  const existingWorkingDataset =
    await datasetRepository.getWorkingDatasetRecordById({
      PlatformDataWorkingDataset,
      workingDatasetId: params.workingDatasetId,
      customerId: executionContext.customerId,
      profileId: body.profileId,
    });

  requireOwnedActiveLease({
    workingDataset: existingWorkingDataset,
    actor,
    editorSessionId: body.editorSessionId,
    now,
  });

  const workingDataset = await datasetRepository.finaliseWorkingDatasetRecord({
    PlatformDataWorkingDataset,
    workingDatasetId: params.workingDatasetId,
    customerId: executionContext.customerId,
    profileId: body.profileId,
    actor,
    finalisedAt: now,
  });

  const activity = await datasetRepository.createWorkingDatasetActivityRecord({
    PlatformDataWorkingDatasetActivity,
    activity: {
      customerId: executionContext.customerId,
      profileId: body.profileId,
      workingDatasetId: params.workingDatasetId,
      activityType: "working_dataset_finalised",
      stepNumber: workingDataset.currentStepNumber,
      summary: "Finalised working dataset",
      details: {
        editorSessionId: body.editorSessionId,
        finalisedAt: now.toISOString(),
      },
      relatedCapability: "data",
      relatedRecordId: params.workingDatasetId,
      actor,
    },
  });

  return {
    success: true,
    workingDataset,
    activity,
  };
}

module.exports = {
  acquireWorkingDatasetEditLease,
  createDataset,
  createWorkingDataset,
  finaliseWorkingDataset,
  getWorkingDataset,
  listWorkingDatasetActivity,
  listWorkingDatasets,
  releaseWorkingDatasetEditLease,
  renewWorkingDatasetEditLease,
};

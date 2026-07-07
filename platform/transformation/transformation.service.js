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
      },
      relatedCapability: "transformation",
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
  materialiseWorkingDataset,
};

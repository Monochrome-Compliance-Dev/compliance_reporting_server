jest.mock("@/helpers/nanoid_helper", () => ({
  getNanoid: jest.fn(),
}));

jest.mock("@/middleware/virus-scan", () => ({
  scanFile: jest.fn(),
}));

jest.mock("@/platform/audit/audit.service", () => ({
  recordDataDatasetAudit: jest.fn(),
}));

jest.mock("@/platform/data/acquisition.service", () => ({
  buildDatasetCreationCommand: jest.fn(),
}));

jest.mock("@/platform/data/dataset.repository", () => ({
  createDatasetRecord: jest.fn(),
  createWorkingDatasetRecord: jest.fn(),
  createWorkingDatasetActivityRecord: jest.fn(),
  getDatasetRecordById: jest.fn(),
  getWorkingDatasetRecordById: jest.fn(),
  updateWorkingDatasetEditLease: jest.fn(),
  clearWorkingDatasetEditLease: jest.fn(),
  finaliseWorkingDatasetRecord: jest.fn(),
}));

jest.mock("@/platform/data/dataset.service", () => ({
  createImmutableDatasetFromCommand: jest.fn(),
}));

jest.mock("@/platform/data/file-storage.service", () => ({
  storeDatasetFile: jest.fn(),
}));

jest.mock("@/platform/security/security.service", () => ({
  enforceDataDatasetCreation: jest.fn(),
}));

const { getNanoid } = require("@/helpers/nanoid_helper");
const { scanFile } = require("@/middleware/virus-scan");
const auditService = require("@/platform/audit/audit.service");
const acquisitionService = require("@/platform/data/acquisition.service");
const datasetRepository = require("@/platform/data/dataset.repository");
const datasetService = require("@/platform/data/dataset.service");
const fileStorageService = require("@/platform/data/file-storage.service");
const securityService = require("@/platform/security/security.service");
const dataService = require("@/platform/data/data.service");

function createExecutionContext(overrides = {}) {
  return {
    actorId: "user-123",
    role: "Admin",
    customerId: "customer-123",
    ...overrides,
  };
}

function createBody(overrides = {}) {
  return {
    sourceName: "July payments",
    datasetType: "payment",
    profileId: "profile-123",
    ...overrides,
  };
}

function createFile(overrides = {}) {
  return {
    originalname: "payments.csv",
    mimetype: "text/csv",
    size: 12345,
    path: "/tmp/mc-platform-data-uploads/payments.csv",
    ...overrides,
  };
}

function createCommand(overrides = {}) {
  return {
    actor: {
      id: "user-123",
      role: "Admin",
      customerId: "customer-123",
    },
    customerId: "customer-123",
    profileId: "profile-123",
    datasetType: "payment",
    sourceType: "csv_upload",
    sourceName: "July payments",
    file: {
      originalFileName: "payments.csv",
      mimeType: "text/csv",
      fileSize: 12345,
      path: "/tmp/mc-platform-data-uploads/payments.csv",
    },
    ...overrides,
  };
}

function createStorageResult(overrides = {}) {
  return {
    storedFileName: "dataset123.csv",
    storagePath: "/tmp/storage/data_hub/customer-123/datasets/dataset123.csv",
    ...overrides,
  };
}

function createDatasetResponse(overrides = {}) {
  return {
    success: true,
    dataset: {
      datasetId: "dataset123",
      customerId: "customer-123",
      profileId: "profile-123",
      datasetType: "payment",
      sourceType: "csv_upload",
      sourceName: "July payments",
      originalFileName: "payments.csv",
      storedFileName: "dataset123.csv",
      storagePath: "/tmp/storage/data_hub/customer-123/datasets/dataset123.csv",
      mimeType: "text/csv",
      fileSize: 12345,
      headers: ["Supplier", "Invoice"],
      headersCount: 2,
      rowsCount: 1,
      status: "available",
      isImmutable: true,
      createdAt: "2026-07-05T00:00:00.000Z",
      ...overrides,
    },
  };
}

function createWorkingDatasetBody(overrides = {}) {
  return {
    sourceDatasetId: "source-dataset-123",
    profileId: "profile-123",
    workingName: "July payments working data",
    ...overrides,
  };
}

function createSourceDataset(overrides = {}) {
  return {
    datasetId: "source-dataset-123",
    customerId: "customer-123",
    profileId: "profile-123",
    datasetType: "payment",
    sourceType: "csv_upload",
    sourceName: "July payments",
    originalFileName: "payments.csv",
    storedFileName: "source-dataset-123.csv",
    storagePath:
      "/tmp/storage/data_hub/customer-123/datasets/source-dataset-123.csv",
    mimeType: "text/csv",
    fileSize: 12345,
    headers: ["Supplier", "Invoice"],
    headersCount: 2,
    rowsCount: 1,
    status: "available",
    isImmutable: true,
    createdAt: "2026-07-05T00:00:00.000Z",
    ...overrides,
  };
}

function createWorkingDataset(overrides = {}) {
  return {
    workingDatasetId: "working-dataset-123",
    sourceDatasetId: "source-dataset-123",
    customerId: "customer-123",
    profileId: "profile-123",
    workingName: "July payments working data",
    datasetType: "payment",
    status: "in_progress",
    currentStepNumber: 1,
    storagePath:
      "/tmp/storage/data_hub/customer-123/datasets/source-dataset-123.csv",
    storedFileName: "source-dataset-123.csv",
    mimeType: "text/csv",
    fileSize: 12345,
    headers: ["Supplier", "Invoice"],
    headersCount: 2,
    rowsCount: 1,
    lineage: {
      sourceDatasetId: "source-dataset-123",
      createdFrom: "immutable_dataset",
    },
    createdAt: "2026-07-06T00:00:00.000Z",
    ...overrides,
  };
}

function createWorkingDatasetActivity(overrides = {}) {
  return {
    activityId: "activity-123",
    customerId: "customer-123",
    profileId: "profile-123",
    workingDatasetId: "working-dataset-123",
    activityType: "working_dataset_created",
    stepNumber: 1,
    summary: "Created working dataset July payments working data",
    details: {
      sourceDatasetId: "source-dataset-123",
    },
    relatedCapability: "data",
    relatedRecordId: "working-dataset-123",
    createdBy: "user-123",
    createdAt: "2026-07-06T00:00:00.000Z",
    ...overrides,
  };
}

function createLeaseBody(overrides = {}) {
  return {
    profileId: "profile-123",
    editorSessionId: "session-123",
    ...overrides,
  };
}

function createLeaseParams(overrides = {}) {
  return {
    workingDatasetId: "working-dataset-123",
    ...overrides,
  };
}

function createLeasedWorkingDataset(overrides = {}) {
  return createWorkingDataset({
    activeEditor: {
      userId: "user-123",
      sessionId: "session-123",
      startedAt: "2026-07-06T00:00:00.000Z",
      lastSeenAt: "2026-07-06T00:00:00.000Z",
      expiresAt: "2099-07-06T00:30:00.000Z",
    },
    ...overrides,
  });
}

function createFinalisedWorkingDataset(overrides = {}) {
  return createWorkingDataset({
    status: "final",
    activeEditor: {
      userId: null,
      sessionId: null,
      startedAt: null,
      lastSeenAt: null,
      expiresAt: null,
    },
    finalisedAt: "2026-07-06T00:30:00.000Z",
    finalisedBy: "user-123",
    ...overrides,
  });
}

describe("data.service", () => {
  beforeEach(() => {
    getNanoid.mockReturnValue("dataset123");
    scanFile.mockResolvedValue(undefined);
    securityService.enforceDataDatasetCreation.mockReturnValue({
      eventType: "platform.security.data_dataset_observed",
      outcome: "allowed",
    });
    auditService.recordDataDatasetAudit.mockResolvedValue({
      eventType: "platform.data.dataset.created",
      outcome: "success",
    });
    acquisitionService.buildDatasetCreationCommand.mockReturnValue(
      createCommand(),
    );
    fileStorageService.storeDatasetFile.mockResolvedValue(
      createStorageResult(),
    );
    datasetService.createImmutableDatasetFromCommand.mockReturnValue(
      createDatasetResponse(),
    );
    datasetRepository.createDatasetRecord.mockResolvedValue(
      createDatasetResponse().dataset,
    );
    datasetRepository.getDatasetRecordById.mockResolvedValue(
      createSourceDataset(),
    );
    datasetRepository.createWorkingDatasetRecord.mockResolvedValue(
      createWorkingDataset(),
    );
    datasetRepository.createWorkingDatasetActivityRecord.mockResolvedValue(
      createWorkingDatasetActivity(),
    );
    datasetRepository.getWorkingDatasetRecordById.mockResolvedValue(
      createWorkingDataset(),
    );
    datasetRepository.updateWorkingDatasetEditLease.mockResolvedValue(
      createLeasedWorkingDataset(),
    );
    datasetRepository.clearWorkingDatasetEditLease.mockResolvedValue(
      createWorkingDataset(),
    );
    datasetRepository.finaliseWorkingDatasetRecord.mockResolvedValue(
      createFinalisedWorkingDataset(),
    );
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("createDataset", () => {
    it("creates, stores and persists a Data-owned CSV dataset", async () => {
      const executionContext = createExecutionContext();
      const body = createBody();
      const file = createFile();
      const PlatformDataDataset = "PlatformDataDatasetModel";

      const result = await dataService.createDataset({
        executionContext,
        body,
        file,
        PlatformDataDataset,
      });

      expect(
        acquisitionService.buildDatasetCreationCommand,
      ).toHaveBeenCalledWith({
        executionContext,
        body,
        file,
      });
      expect(getNanoid).toHaveBeenCalledWith(10);
      expect(securityService.enforceDataDatasetCreation).toHaveBeenCalledWith({
        datasetId: "dataset123",
        actor: createCommand().actor,
        customerId: "customer-123",
      });
      expect(scanFile).toHaveBeenCalledWith(
        "/tmp/mc-platform-data-uploads/payments.csv",
        "payments.csv",
      );
      expect(fileStorageService.storeDatasetFile).toHaveBeenCalledWith({
        customerId: "customer-123",
        datasetId: "dataset123",
        sourceFilePath: "/tmp/mc-platform-data-uploads/payments.csv",
      });
      expect(
        datasetService.createImmutableDatasetFromCommand,
      ).toHaveBeenCalledWith({
        command: createCommand(),
        datasetId: "dataset123",
        storageResult: createStorageResult(),
      });
      expect(datasetRepository.createDatasetRecord).toHaveBeenCalledWith({
        PlatformDataDataset,
        dataset: {
          ...createDatasetResponse().dataset,
          actor: createCommand().actor,
        },
      });
      expect(auditService.recordDataDatasetAudit).toHaveBeenCalledWith({
        datasetId: "dataset123",
        outcome: "success",
        actor: createCommand().actor,
        securityObservation: {
          eventType: "platform.security.data_dataset_observed",
          outcome: "allowed",
        },
      });
      expect(result).toEqual(createDatasetResponse());
    });

    it("throws when executionContext is missing", async () => {
      await expect(
        dataService.createDataset({
          body: createBody(),
          file: createFile(),
          PlatformDataDataset: "PlatformDataDatasetModel",
        }),
      ).rejects.toThrow(
        "executionContext is required for Data dataset creation.",
      );
    });

    it("throws when PlatformDataDataset model is missing", async () => {
      await expect(
        dataService.createDataset({
          executionContext: createExecutionContext(),
          body: createBody(),
          file: createFile(),
        }),
      ).rejects.toThrow(
        "PlatformDataDataset model is required for Data dataset creation.",
      );
    });

    it("fails loudly when command creation fails", async () => {
      acquisitionService.buildDatasetCreationCommand.mockImplementation(() => {
        throw new Error("command failed");
      });

      await expect(
        dataService.createDataset({
          executionContext: createExecutionContext(),
          body: createBody(),
          file: createFile(),
          PlatformDataDataset: "PlatformDataDatasetModel",
        }),
      ).rejects.toThrow("command failed");
    });

    it("fails loudly when Security denies dataset creation", async () => {
      securityService.enforceDataDatasetCreation.mockImplementation(() => {
        const error = new Error("Role is not allowed for governed execution.");
        error.status = 403;
        error.securityObservation = {
          outcome: "denied",
          reason: "role_not_allowed",
        };
        throw error;
      });

      await expect(
        dataService.createDataset({
          executionContext: createExecutionContext(),
          body: createBody(),
          file: createFile(),
          PlatformDataDataset: "PlatformDataDatasetModel",
        }),
      ).rejects.toThrow("Role is not allowed for governed execution.");

      expect(fileStorageService.storeDatasetFile).not.toHaveBeenCalled();
      expect(scanFile).not.toHaveBeenCalled();
      expect(datasetRepository.createDatasetRecord).not.toHaveBeenCalled();
      expect(auditService.recordDataDatasetAudit).toHaveBeenCalledWith({
        datasetId: "dataset123",
        outcome: "denied",
        actor: createCommand().actor,
        securityObservation: {
          outcome: "denied",
          reason: "role_not_allowed",
        },
        error: expect.objectContaining({
          message: "Role is not allowed for governed execution.",
          status: 403,
        }),
      });
    });

    it("fails loudly and records denied audit evidence when antivirus scanning fails", async () => {
      const scanError = new Error("Antivirus scan failed.");
      scanError.status = 400;
      scanFile.mockRejectedValue(scanError);

      await expect(
        dataService.createDataset({
          executionContext: createExecutionContext(),
          body: createBody(),
          file: createFile(),
          PlatformDataDataset: "PlatformDataDatasetModel",
        }),
      ).rejects.toThrow("Antivirus scan failed.");

      expect(scanFile).toHaveBeenCalledWith(
        "/tmp/mc-platform-data-uploads/payments.csv",
        "payments.csv",
      );
      expect(fileStorageService.storeDatasetFile).not.toHaveBeenCalled();
      expect(
        datasetService.createImmutableDatasetFromCommand,
      ).not.toHaveBeenCalled();
      expect(datasetRepository.createDatasetRecord).not.toHaveBeenCalled();
      expect(auditService.recordDataDatasetAudit).toHaveBeenCalledWith({
        datasetId: "dataset123",
        outcome: "denied",
        actor: createCommand().actor,
        securityObservation: {
          eventType: "platform.security.data_dataset_observed",
          outcome: "allowed",
        },
        error: scanError,
      });
    });

    it("fails loudly when file storage fails", async () => {
      fileStorageService.storeDatasetFile.mockRejectedValue(
        new Error("storage failed"),
      );

      await expect(
        dataService.createDataset({
          executionContext: createExecutionContext(),
          body: createBody(),
          file: createFile(),
          PlatformDataDataset: "PlatformDataDatasetModel",
        }),
      ).rejects.toThrow("storage failed");
    });

    it("fails loudly when dataset creation fails", async () => {
      datasetService.createImmutableDatasetFromCommand.mockImplementation(
        () => {
          throw new Error("dataset failed");
        },
      );

      await expect(
        dataService.createDataset({
          executionContext: createExecutionContext(),
          body: createBody(),
          file: createFile(),
          PlatformDataDataset: "PlatformDataDatasetModel",
        }),
      ).rejects.toThrow("dataset failed");
    });

    it("fails loudly when repository persistence fails", async () => {
      datasetRepository.createDatasetRecord.mockRejectedValue(
        new Error("repository failed"),
      );

      await expect(
        dataService.createDataset({
          executionContext: createExecutionContext(),
          body: createBody(),
          file: createFile(),
          PlatformDataDataset: "PlatformDataDatasetModel",
        }),
      ).rejects.toThrow("repository failed");
    });
  });

  describe("createWorkingDataset", () => {
    it("creates a metadata and lineage working dataset from an available source dataset", async () => {
      getNanoid.mockReturnValue("working-dataset-123");
      const executionContext = createExecutionContext();
      const body = createWorkingDatasetBody();
      const PlatformDataDataset = "PlatformDataDatasetModel";
      const PlatformDataWorkingDataset = "PlatformDataWorkingDatasetModel";
      const PlatformDataWorkingDatasetActivity =
        "PlatformDataWorkingDatasetActivityModel";

      const result = await dataService.createWorkingDataset({
        executionContext,
        body,
        PlatformDataDataset,
        PlatformDataWorkingDataset,
        PlatformDataWorkingDatasetActivity,
      });

      expect(getNanoid).toHaveBeenCalledWith(10);
      expect(securityService.enforceDataDatasetCreation).toHaveBeenCalledWith({
        datasetId: "working-dataset-123",
        actor: createCommand().actor,
        customerId: "customer-123",
      });
      expect(datasetRepository.getDatasetRecordById).toHaveBeenCalledWith({
        PlatformDataDataset,
        datasetId: "source-dataset-123",
        customerId: "customer-123",
        profileId: "profile-123",
      });
      expect(datasetRepository.createWorkingDatasetRecord).toHaveBeenCalledWith(
        {
          PlatformDataWorkingDataset,
          sourceDataset: createSourceDataset(),
          workingDataset: {
            workingDatasetId: "working-dataset-123",
            sourceDatasetId: "source-dataset-123",
            customerId: "customer-123",
            profileId: "profile-123",
            workingName: "July payments working data",
            currentStepNumber: 1,
            actor: createCommand().actor,
          },
        },
      );
      expect(
        datasetRepository.createWorkingDatasetActivityRecord,
      ).toHaveBeenCalledWith({
        PlatformDataWorkingDatasetActivity,
        activity: {
          customerId: "customer-123",
          profileId: "profile-123",
          workingDatasetId: "working-dataset-123",
          activityType: "working_dataset_created",
          stepNumber: 1,
          summary: "Created working dataset July payments working data",
          details: {
            sourceDatasetId: "source-dataset-123",
          },
          relatedCapability: "data",
          relatedRecordId: "working-dataset-123",
          actor: createCommand().actor,
        },
      });
      expect(auditService.recordDataDatasetAudit).toHaveBeenCalledWith({
        datasetId: "working-dataset-123",
        outcome: "success",
        actor: createCommand().actor,
        securityObservation: {
          eventType: "platform.security.data_dataset_observed",
          outcome: "allowed",
        },
      });
      expect(result).toEqual({
        success: true,
        workingDataset: createWorkingDataset(),
        activity: createWorkingDatasetActivity(),
      });
    });

    it("throws when sourceDatasetId is missing", async () => {
      await expect(
        dataService.createWorkingDataset({
          executionContext: createExecutionContext(),
          body: createWorkingDatasetBody({ sourceDatasetId: null }),
          PlatformDataDataset: "PlatformDataDatasetModel",
          PlatformDataWorkingDataset: "PlatformDataWorkingDatasetModel",
          PlatformDataWorkingDatasetActivity:
            "PlatformDataWorkingDatasetActivityModel",
        }),
      ).rejects.toThrow(
        "sourceDatasetId is required for working dataset creation.",
      );
    });

    it("records denied audit evidence when Security denies working dataset creation", async () => {
      getNanoid.mockReturnValue("working-dataset-123");
      securityService.enforceDataDatasetCreation.mockImplementation(() => {
        const error = new Error("Role is not allowed for governed execution.");
        error.status = 403;
        error.securityObservation = {
          outcome: "denied",
          reason: "role_not_allowed",
        };
        throw error;
      });

      await expect(
        dataService.createWorkingDataset({
          executionContext: createExecutionContext(),
          body: createWorkingDatasetBody(),
          PlatformDataDataset: "PlatformDataDatasetModel",
          PlatformDataWorkingDataset: "PlatformDataWorkingDatasetModel",
          PlatformDataWorkingDatasetActivity:
            "PlatformDataWorkingDatasetActivityModel",
        }),
      ).rejects.toThrow("Role is not allowed for governed execution.");

      expect(datasetRepository.getDatasetRecordById).not.toHaveBeenCalled();
      expect(
        datasetRepository.createWorkingDatasetRecord,
      ).not.toHaveBeenCalled();
      expect(auditService.recordDataDatasetAudit).toHaveBeenCalledWith({
        datasetId: "working-dataset-123",
        outcome: "denied",
        actor: createCommand().actor,
        securityObservation: {
          outcome: "denied",
          reason: "role_not_allowed",
        },
        error: expect.objectContaining({
          message: "Role is not allowed for governed execution.",
          status: 403,
        }),
      });
    });

    it("fails loudly when source dataset lookup fails and does not create working dataset", async () => {
      datasetRepository.getDatasetRecordById.mockRejectedValue(
        new Error("source dataset was not found for working data creation."),
      );

      await expect(
        dataService.createWorkingDataset({
          executionContext: createExecutionContext(),
          body: createWorkingDatasetBody(),
          PlatformDataDataset: "PlatformDataDatasetModel",
          PlatformDataWorkingDataset: "PlatformDataWorkingDatasetModel",
          PlatformDataWorkingDatasetActivity:
            "PlatformDataWorkingDatasetActivityModel",
        }),
      ).rejects.toThrow(
        "source dataset was not found for working data creation.",
      );

      expect(
        datasetRepository.createWorkingDatasetRecord,
      ).not.toHaveBeenCalled();
    });

    it("fails loudly when source dataset is unavailable", async () => {
      datasetRepository.getDatasetRecordById.mockResolvedValue(
        createSourceDataset({ status: "processing" }),
      );

      await expect(
        dataService.createWorkingDataset({
          executionContext: createExecutionContext(),
          body: createWorkingDatasetBody(),
          PlatformDataDataset: "PlatformDataDatasetModel",
          PlatformDataWorkingDataset: "PlatformDataWorkingDatasetModel",
          PlatformDataWorkingDatasetActivity:
            "PlatformDataWorkingDatasetActivityModel",
        }),
      ).rejects.toMatchObject({
        message: "source dataset is not available for working data creation.",
        status: 409,
      });

      expect(
        datasetRepository.createWorkingDatasetRecord,
      ).not.toHaveBeenCalled();
    });
  });
});

jest.mock("@/helpers/nanoid_helper", () => ({
  getNanoid: jest.fn(),
}));

jest.mock("@/platform/audit/audit.service", () => ({
  recordDataDatasetAudit: jest.fn(),
}));

jest.mock("@/platform/data/acquisition.service", () => ({
  buildDatasetCreationCommand: jest.fn(),
}));

jest.mock("@/platform/data/dataset.repository", () => ({
  createDatasetRecord: jest.fn(),
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
    buffer: Buffer.from("Supplier,Invoice\nABC,INV-001\n"),
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
      buffer: Buffer.from("Supplier,Invoice\nABC,INV-001\n"),
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

describe("data.service", () => {
  beforeEach(() => {
    getNanoid.mockReturnValue("dataset123");
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
      expect(fileStorageService.storeDatasetFile).toHaveBeenCalledWith({
        customerId: "customer-123",
        datasetId: "dataset123",
        buffer: Buffer.from("Supplier,Invoice\nABC,INV-001\n"),
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
});

jest.mock("fs/promises", () => ({
  mkdir: jest.fn(),
  readFile: jest.fn(),
  stat: jest.fn(),
  writeFile: jest.fn(),
}));

jest.mock("@/platform/data/dataset.repository", () => ({
  createWorkingDatasetActivityRecord: jest.fn(),
  getWorkingDatasetRecordById: jest.fn(),
  updateWorkingDatasetStorageRecord: jest.fn(),
}));

const fs = require("fs/promises");
const datasetRepository = require("@/platform/data/dataset.repository");
const transformationService = require("@/platform/transformation/transformation.service");

function createExecutionContext(overrides = {}) {
  return {
    actorId: "user-123",
    role: "Admin",
    customerId: "customer-123",
    ...overrides,
  };
}

function createParams(overrides = {}) {
  return {
    workingDatasetId: "working-dataset-123",
    ...overrides,
  };
}

function createBody(overrides = {}) {
  return {
    profileId: "profile-123",
    editorSessionId: "session-123",
    stepNumber: 2,
    fields: [
      {
        sourceField: "Invoice No",
        targetField: "invoice_reference_number",
      },
      {
        sourceField: "Supplier",
        targetField: "supplier_name",
      },
    ],
    customFields: [
      {
        targetField: "source_file_type",
        value: "payments",
      },
    ],
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
    headers: ["Supplier", "Invoice No"],
    headersCount: 2,
    rowsCount: 1,
    lineage: {
      sourceDatasetId: "source-dataset-123",
      createdFrom: "immutable_dataset",
    },
    activeEditor: {
      userId: "user-123",
      sessionId: "session-123",
      startedAt: "2026-07-07T00:00:00.000Z",
      lastSeenAt: "2026-07-07T00:00:00.000Z",
      expiresAt: "2099-07-07T00:30:00.000Z",
    },
    createdAt: "2026-07-07T00:00:00.000Z",
    updatedAt: "2026-07-07T00:00:00.000Z",
    ...overrides,
  };
}

function createMaterialisedWorkingDataset(overrides = {}) {
  return createWorkingDataset({
    storagePath:
      "/tmp/storage/data_hub/customer-123/datasets/working-dataset-123-materialised.csv",
    storedFileName: "working-dataset-123-materialised.csv",
    fileSize: 82,
    headers: ["invoice_reference_number", "supplier_name", "source_file_type"],
    headersCount: 3,
    rowsCount: 1,
    meta: {
      materialisedFrom: "projection_config",
    },
    ...overrides,
  });
}

function createActivity(overrides = {}) {
  return {
    activityId: "activity-123",
    customerId: "customer-123",
    profileId: "profile-123",
    workingDatasetId: "working-dataset-123",
    activityType: "working_dataset_materialised",
    stepNumber: 2,
    summary: "Materialised working dataset from projection configuration",
    details: {
      editorSessionId: "session-123",
    },
    relatedCapability: "transformation",
    relatedRecordId: "working-dataset-123",
    createdBy: "user-123",
    createdAt: "2026-07-07T00:00:00.000Z",
    ...overrides,
  };
}

describe("transformation.service", () => {
  beforeEach(() => {
    fs.readFile.mockResolvedValue(
      "Supplier,Invoice No\nAcme Pty Ltd,INV-001\n",
    );
    fs.mkdir.mockResolvedValue(undefined);
    fs.writeFile.mockResolvedValue(undefined);
    fs.stat.mockResolvedValue({ size: 82 });

    datasetRepository.getWorkingDatasetRecordById.mockResolvedValue(
      createWorkingDataset(),
    );
    datasetRepository.updateWorkingDatasetStorageRecord.mockResolvedValue(
      createMaterialisedWorkingDataset(),
    );
    datasetRepository.createWorkingDatasetActivityRecord.mockResolvedValue(
      createActivity(),
    );
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("materialiseWorkingDataset", () => {
    it("materialises a CSV, updates working dataset storage and records activity for an editable working dataset with an owned active lease", async () => {
      const PlatformDataWorkingDataset = "PlatformDataWorkingDatasetModel";
      const PlatformDataWorkingDatasetActivity =
        "PlatformDataWorkingDatasetActivityModel";

      const result = await transformationService.materialiseWorkingDataset({
        executionContext: createExecutionContext(),
        params: createParams(),
        body: createBody(),
        PlatformDataWorkingDataset,
        PlatformDataWorkingDatasetActivity,
      });

      expect(
        datasetRepository.getWorkingDatasetRecordById,
      ).toHaveBeenCalledWith({
        PlatformDataWorkingDataset,
        workingDatasetId: "working-dataset-123",
        customerId: "customer-123",
        profileId: "profile-123",
      });
      expect(fs.readFile).toHaveBeenCalledWith(
        "/tmp/storage/data_hub/customer-123/datasets/source-dataset-123.csv",
        "utf8",
      );
      expect(fs.mkdir).toHaveBeenCalledWith(
        "/tmp/storage/data_hub/customer-123/datasets",
        { recursive: true },
      );
      expect(fs.writeFile).toHaveBeenCalledWith(
        expect.stringMatching(
          /\/tmp\/storage\/data_hub\/customer-123\/datasets\/working-dataset-123-materialised-.+\.csv$/,
        ),
        "invoice_reference_number,supplier_name,source_file_type\nINV-001,Acme Pty Ltd,payments\n",
        "utf8",
      );
      expect(fs.stat).toHaveBeenCalledWith(
        expect.stringMatching(
          /\/tmp\/storage\/data_hub\/customer-123\/datasets\/working-dataset-123-materialised-.+\.csv$/,
        ),
      );
      expect(
        datasetRepository.updateWorkingDatasetStorageRecord,
      ).toHaveBeenCalledWith({
        PlatformDataWorkingDataset,
        workingDatasetId: "working-dataset-123",
        customerId: "customer-123",
        profileId: "profile-123",
        storage: {
          storagePath: expect.stringMatching(
            /\/tmp\/storage\/data_hub\/customer-123\/datasets\/working-dataset-123-materialised-.+\.csv$/,
          ),
          storedFileName: expect.stringMatching(
            /^working-dataset-123-materialised-.+\.csv$/,
          ),
          mimeType: "text/csv",
          fileSize: 82,
          headers: [
            "invoice_reference_number",
            "supplier_name",
            "source_file_type",
          ],
          headersCount: 3,
          rowsCount: 1,
          meta: {
            materialisedFrom: "projection_config",
            sourceStoragePath:
              "/tmp/storage/data_hub/customer-123/datasets/source-dataset-123.csv",
            materialisedAt: expect.any(String),
          },
        },
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
      });
      expect(
        datasetRepository.createWorkingDatasetActivityRecord,
      ).toHaveBeenCalledWith({
        PlatformDataWorkingDatasetActivity,
        activity: {
          customerId: "customer-123",
          profileId: "profile-123",
          workingDatasetId: "working-dataset-123",
          activityType: "working_dataset_materialised",
          stepNumber: 2,
          summary: "Materialised working dataset from projection configuration",
          details: {
            editorSessionId: "session-123",
            fields: [
              {
                sourceField: "Invoice No",
                targetField: "invoice_reference_number",
              },
              {
                sourceField: "Supplier",
                targetField: "supplier_name",
              },
            ],
            customFields: [
              {
                targetField: "source_file_type",
                value: "payments",
              },
            ],
            storagePath: expect.stringMatching(
              /\/tmp\/storage\/data_hub\/customer-123\/datasets\/working-dataset-123-materialised-.+\.csv$/,
            ),
            storedFileName: expect.stringMatching(
              /^working-dataset-123-materialised-.+\.csv$/,
            ),
            rowsCount: 1,
            headersCount: 3,
          },
          relatedCapability: "transformation",
          relatedRecordId: "working-dataset-123",
          actor: {
            id: "user-123",
            role: "Admin",
            customerId: "customer-123",
          },
        },
      });
      expect(result).toEqual({
        success: true,
        workingDataset: createMaterialisedWorkingDataset(),
        activity: createActivity(),
      });
    });

    it("throws when fields are missing", async () => {
      await expect(
        transformationService.materialiseWorkingDataset({
          executionContext: createExecutionContext(),
          params: createParams(),
          body: createBody({ fields: [] }),
          PlatformDataWorkingDataset: "PlatformDataWorkingDatasetModel",
          PlatformDataWorkingDatasetActivity:
            "PlatformDataWorkingDatasetActivityModel",
        }),
      ).rejects.toThrow("fields must include at least one projection field.");

      expect(
        datasetRepository.getWorkingDatasetRecordById,
      ).not.toHaveBeenCalled();
      expect(
        datasetRepository.updateWorkingDatasetStorageRecord,
      ).not.toHaveBeenCalled();
      expect(
        datasetRepository.createWorkingDatasetActivityRecord,
      ).not.toHaveBeenCalled();
    });

    it("throws when target fields are duplicated", async () => {
      await expect(
        transformationService.materialiseWorkingDataset({
          executionContext: createExecutionContext(),
          params: createParams(),
          body: createBody({
            fields: [
              {
                sourceField: "Invoice No",
                targetField: "invoice_reference_number",
              },
            ],
            customFields: [
              {
                targetField: "invoice_reference_number",
                value: "payments",
              },
            ],
          }),
          PlatformDataWorkingDataset: "PlatformDataWorkingDatasetModel",
          PlatformDataWorkingDatasetActivity:
            "PlatformDataWorkingDatasetActivityModel",
        }),
      ).rejects.toThrow("materialisation target fields must be unique.");

      expect(
        datasetRepository.getWorkingDatasetRecordById,
      ).not.toHaveBeenCalled();
      expect(
        datasetRepository.updateWorkingDatasetStorageRecord,
      ).not.toHaveBeenCalled();
      expect(
        datasetRepository.createWorkingDatasetActivityRecord,
      ).not.toHaveBeenCalled();
    });

    it("throws when a projection source field is missing from the working dataset CSV", async () => {
      await expect(
        transformationService.materialiseWorkingDataset({
          executionContext: createExecutionContext(),
          params: createParams(),
          body: createBody({
            fields: [
              {
                sourceField: "Missing Invoice No",
                targetField: "invoice_reference_number",
              },
            ],
            customFields: [],
          }),
          PlatformDataWorkingDataset: "PlatformDataWorkingDatasetModel",
          PlatformDataWorkingDatasetActivity:
            "PlatformDataWorkingDatasetActivityModel",
        }),
      ).rejects.toThrow(
        "materialisation source fields were not found in the working dataset: Missing Invoice No.",
      );

      expect(fs.readFile).toHaveBeenCalledWith(
        "/tmp/storage/data_hub/customer-123/datasets/source-dataset-123.csv",
        "utf8",
      );
      expect(fs.writeFile).not.toHaveBeenCalled();
      expect(
        datasetRepository.updateWorkingDatasetStorageRecord,
      ).not.toHaveBeenCalled();
      expect(
        datasetRepository.createWorkingDatasetActivityRecord,
      ).not.toHaveBeenCalled();
    });

    it("throws when the working dataset is final", async () => {
      datasetRepository.getWorkingDatasetRecordById.mockResolvedValue(
        createWorkingDataset({ status: "final" }),
      );

      await expect(
        transformationService.materialiseWorkingDataset({
          executionContext: createExecutionContext(),
          params: createParams(),
          body: createBody(),
          PlatformDataWorkingDataset: "PlatformDataWorkingDatasetModel",
          PlatformDataWorkingDatasetActivity:
            "PlatformDataWorkingDatasetActivityModel",
        }),
      ).rejects.toThrow("final working datasets cannot be materialised.");

      expect(
        datasetRepository.updateWorkingDatasetStorageRecord,
      ).not.toHaveBeenCalled();
      expect(
        datasetRepository.createWorkingDatasetActivityRecord,
      ).not.toHaveBeenCalled();
    });

    it("throws when the active editor lease belongs to another session", async () => {
      datasetRepository.getWorkingDatasetRecordById.mockResolvedValue(
        createWorkingDataset({
          activeEditor: {
            userId: "user-123",
            sessionId: "other-session",
            startedAt: "2026-07-07T00:00:00.000Z",
            lastSeenAt: "2026-07-07T00:00:00.000Z",
            expiresAt: "2099-07-07T00:30:00.000Z",
          },
        }),
      );

      await expect(
        transformationService.materialiseWorkingDataset({
          executionContext: createExecutionContext(),
          params: createParams(),
          body: createBody(),
          PlatformDataWorkingDataset: "PlatformDataWorkingDatasetModel",
          PlatformDataWorkingDatasetActivity:
            "PlatformDataWorkingDatasetActivityModel",
        }),
      ).rejects.toThrow("active editor lease belongs to another session.");

      expect(
        datasetRepository.updateWorkingDatasetStorageRecord,
      ).not.toHaveBeenCalled();
      expect(
        datasetRepository.createWorkingDatasetActivityRecord,
      ).not.toHaveBeenCalled();
    });

    it("throws when the active editor lease has expired", async () => {
      datasetRepository.getWorkingDatasetRecordById.mockResolvedValue(
        createWorkingDataset({
          activeEditor: {
            userId: "user-123",
            sessionId: "session-123",
            startedAt: "2026-07-07T00:00:00.000Z",
            lastSeenAt: "2026-07-07T00:00:00.000Z",
            expiresAt: "2000-07-07T00:30:00.000Z",
          },
        }),
      );

      await expect(
        transformationService.materialiseWorkingDataset({
          executionContext: createExecutionContext(),
          params: createParams(),
          body: createBody(),
          PlatformDataWorkingDataset: "PlatformDataWorkingDatasetModel",
          PlatformDataWorkingDatasetActivity:
            "PlatformDataWorkingDatasetActivityModel",
        }),
      ).rejects.toThrow("active editor lease has expired.");

      expect(
        datasetRepository.updateWorkingDatasetStorageRecord,
      ).not.toHaveBeenCalled();
      expect(
        datasetRepository.createWorkingDatasetActivityRecord,
      ).not.toHaveBeenCalled();
    });
  });
});

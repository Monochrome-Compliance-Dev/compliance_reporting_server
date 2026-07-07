const datasetRepository = require("@/platform/data/dataset.repository");

jest.mock("@/helpers/customerTransaction", () => ({
  withCustomerTransaction: jest.fn(async (_customerId, work) =>
    work("mock-transaction"),
  ),
}));

const { withCustomerTransaction } = require("@/helpers/customerTransaction");

function createDataset(overrides = {}) {
  return {
    datasetId: "dataset123",
    customerId: "customer-1",
    profileId: "profile-1",
    datasetType: "payment",
    sourceType: "csv_upload",
    sourceName: "July payments",
    originalFileName: "payments.csv",
    storagePath: "platform/data/customer-1/datasets/dataset123/payments.csv",
    mimeType: "text/csv",
    fileSize: 12345,
    headers: ["Supplier", "Invoice"],
    headersCount: 2,
    rowsCount: 1,
    status: "available",
    isImmutable: true,
    createdAt: "2026-07-05T00:00:00.000Z",
    actor: {
      id: "user-123",
      role: "Admin",
      customerId: "customer-1",
    },
    ...overrides,
  };
}

function createModelRecord(overrides = {}) {
  return {
    get: jest.fn(() => ({
      id: "dataset123",
      customerId: "customer-1",
      profileId: "profile-1",
      datasetType: "payment",
      sourceType: "csv_upload",
      sourceName: "July payments",
      originalFileName: "payments.csv",
      storedFileName: "dataset123.csv",
      storagePath: "platform/data/customer-1/datasets/dataset123/payments.csv",
      mimeType: "text/csv",
      fileSize: 12345,
      headers: ["Supplier", "Invoice"],
      headersCount: 2,
      rowsCount: 1,
      status: "available",
      createdAt: new Date("2026-07-05T00:00:00.000Z"),
      ...overrides,
    })),
  };
}

function createSourceDataset(overrides = {}) {
  return {
    datasetId: "source-dataset-123",
    customerId: "customer-1",
    profileId: "profile-1",
    datasetType: "payment",
    sourceType: "csv_upload",
    sourceName: "July payments",
    originalFileName: "payments.csv",
    storedFileName: "source-dataset-123.csv",
    storagePath:
      "platform/data/customer-1/datasets/source-dataset-123/payments.csv",
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

function createWorkingDatasetCommand(overrides = {}) {
  return {
    workingDatasetId: "working-dataset-123",
    sourceDatasetId: "source-dataset-123",
    customerId: "customer-1",
    profileId: "profile-1",
    workingName: "July payments working data",
    currentStepNumber: 1,
    actor: {
      id: "user-123",
      role: "Admin",
      customerId: "customer-1",
    },
    ...overrides,
  };
}

function createWorkingModelRecord(overrides = {}) {
  return {
    get: jest.fn(() => ({
      id: "working-dataset-123",
      customerId: "customer-1",
      profileId: "profile-1",
      sourceDatasetId: "source-dataset-123",
      workingName: "July payments working data",
      datasetType: "payment",
      status: "in_progress",
      currentStepNumber: 1,
      storedFileName: "source-dataset-123.csv",
      storagePath:
        "platform/data/customer-1/datasets/source-dataset-123/payments.csv",
      mimeType: "text/csv",
      fileSize: 12345,
      headers: ["Supplier", "Invoice"],
      headersCount: 2,
      rowsCount: 1,
      lineage: {
        sourceDatasetId: "source-dataset-123",
        createdFrom: "immutable_dataset",
      },
      meta: {
        sourceDatasetId: "source-dataset-123",
        sourceOriginalFileName: "payments.csv",
      },
      activeEditorUserId: null,
      activeEditorSessionId: null,
      activeEditorStartedAt: null,
      activeEditorLastSeenAt: null,
      activeEditorExpiresAt: null,
      finalisedAt: null,
      finalisedBy: null,
      createdAt: new Date("2026-07-06T00:00:00.000Z"),
      updatedAt: new Date("2026-07-06T00:00:00.000Z"),
      ...overrides,
    })),
  };
}

function createUpdatableWorkingModelRecord(overrides = {}) {
  const baseRecord = createWorkingModelRecord(overrides);

  return {
    ...baseRecord,
    update: jest.fn(async (changes) =>
      createWorkingModelRecord({
        ...overrides,
        ...changes,
        updatedAt: new Date("2026-07-06T00:30:00.000Z"),
      }),
    ),
  };
}

function createWorkingActivityModelRecord(overrides = {}) {
  return {
    get: jest.fn(() => ({
      id: "activity-123",
      customerId: "customer-1",
      profileId: "profile-1",
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
      createdAt: new Date("2026-07-06T00:00:00.000Z"),
      ...overrides,
    })),
  };
}

describe("dataset.repository", () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("createDatasetRecord", () => {
    it("persists a platform Data dataset record", async () => {
      const PlatformDataDataset = {
        create: jest.fn().mockResolvedValue(createModelRecord()),
      };

      const result = await datasetRepository.createDatasetRecord({
        PlatformDataDataset,
        dataset: createDataset(),
      });

      expect(withCustomerTransaction).toHaveBeenCalledWith(
        "customer-1",
        expect.any(Function),
      );

      expect(PlatformDataDataset.create).toHaveBeenCalledWith(
        {
          id: "dataset123",
          customerId: "customer-1",
          profileId: "profile-1",
          datasetType: "payment",
          sourceType: "csv_upload",
          sourceName: "July payments",
          originalFileName: "payments.csv",
          storedFileName: "dataset123.csv",
          storagePath:
            "platform/data/customer-1/datasets/dataset123/payments.csv",
          mimeType: "text/csv",
          fileSize: 12345,
          headers: ["Supplier", "Invoice"],
          headersCount: 2,
          rowsCount: 1,
          status: "available",
          detectedCoverage: {},
          meta: {
            headers: ["Supplier", "Invoice"],
            rowsCount: 1,
            uploadedAt: "2026-07-05T00:00:00.000Z",
          },
          uploadedBy: "user-123",
          createdBy: "user-123",
          updatedBy: "user-123",
        },
        { transaction: "mock-transaction" },
      );

      expect(result).toEqual({
        datasetId: "dataset123",
        customerId: "customer-1",
        profileId: "profile-1",
        datasetType: "payment",
        sourceType: "csv_upload",
        sourceName: "July payments",
        originalFileName: "payments.csv",
        storedFileName: "dataset123.csv",
        storagePath:
          "platform/data/customer-1/datasets/dataset123/payments.csv",
        mimeType: "text/csv",
        fileSize: 12345,
        headers: ["Supplier", "Invoice"],
        headersCount: 2,
        rowsCount: 1,
        status: "available",
        isImmutable: true,
        createdAt: "2026-07-05T00:00:00.000Z",
      });
    });

    it("persists provided detectedCoverage and meta", async () => {
      const PlatformDataDataset = {
        create: jest.fn().mockResolvedValue(createModelRecord()),
      };

      await datasetRepository.createDatasetRecord({
        PlatformDataDataset,
        dataset: createDataset({
          detectedCoverage: { from: "2026-07-01", to: "2026-07-31" },
          meta: { source: "manual_upload" },
        }),
      });

      expect(PlatformDataDataset.create).toHaveBeenCalledWith(
        expect.objectContaining({
          detectedCoverage: { from: "2026-07-01", to: "2026-07-31" },
          meta: {
            source: "manual_upload",
            headers: ["Supplier", "Invoice"],
            rowsCount: 1,
            uploadedAt: "2026-07-05T00:00:00.000Z",
          },
        }),
        { transaction: "mock-transaction" },
      );
    });

    it("throws when the model is missing", async () => {
      await expect(
        datasetRepository.createDatasetRecord({
          dataset: createDataset(),
        }),
      ).rejects.toThrow("PlatformDataDataset model is required.");
    });

    it("throws when dataset is missing", async () => {
      await expect(
        datasetRepository.createDatasetRecord({
          PlatformDataDataset: { create: jest.fn() },
        }),
      ).rejects.toThrow("dataset is required for persistence.");
    });

    it("throws when datasetId is missing", async () => {
      await expect(
        datasetRepository.createDatasetRecord({
          PlatformDataDataset: { create: jest.fn() },
          dataset: createDataset({ datasetId: null }),
        }),
      ).rejects.toThrow("datasetId is required for persistence.");
    });

    it("throws when actor id is missing", async () => {
      await expect(
        datasetRepository.createDatasetRecord({
          PlatformDataDataset: { create: jest.fn() },
          dataset: createDataset({ actor: { role: "Admin" } }),
        }),
      ).rejects.toThrow("actor id is required for persistence.");
    });

    it("does not open a customer transaction when customerId is missing", async () => {
      await expect(
        datasetRepository.createDatasetRecord({
          PlatformDataDataset: { create: jest.fn() },
          dataset: createDataset({ customerId: null }),
        }),
      ).rejects.toThrow("customerId is required for persistence.");

      expect(withCustomerTransaction).not.toHaveBeenCalled();
    });

    it("fails loudly when persistence fails", async () => {
      const PlatformDataDataset = {
        create: jest.fn().mockRejectedValue(new Error("database failed")),
      };

      await expect(
        datasetRepository.createDatasetRecord({
          PlatformDataDataset,
          dataset: createDataset(),
        }),
      ).rejects.toThrow("database failed");
    });
  });

  describe("getDatasetRecordById", () => {
    it("loads a source dataset by id, customerId and profileId inside a customer transaction", async () => {
      const PlatformDataDataset = {
        findOne: jest.fn().mockResolvedValue(
          createModelRecord({
            id: "source-dataset-123",
            storedFileName: "source-dataset-123.csv",
            storagePath:
              "platform/data/customer-1/datasets/source-dataset-123/payments.csv",
          }),
        ),
      };

      const result = await datasetRepository.getDatasetRecordById({
        PlatformDataDataset,
        datasetId: "source-dataset-123",
        customerId: "customer-1",
        profileId: "profile-1",
      });

      expect(withCustomerTransaction).toHaveBeenCalledWith(
        "customer-1",
        expect.any(Function),
      );
      expect(PlatformDataDataset.findOne).toHaveBeenCalledWith({
        where: {
          id: "source-dataset-123",
          customerId: "customer-1",
          profileId: "profile-1",
        },
        transaction: "mock-transaction",
      });
      expect(result).toEqual({
        datasetId: "source-dataset-123",
        customerId: "customer-1",
        profileId: "profile-1",
        datasetType: "payment",
        sourceType: "csv_upload",
        sourceName: "July payments",
        originalFileName: "payments.csv",
        storedFileName: "source-dataset-123.csv",
        storagePath:
          "platform/data/customer-1/datasets/source-dataset-123/payments.csv",
        mimeType: "text/csv",
        fileSize: 12345,
        headers: ["Supplier", "Invoice"],
        headersCount: 2,
        rowsCount: 1,
        status: "available",
        isImmutable: true,
        createdAt: "2026-07-05T00:00:00.000Z",
      });
    });

    it("throws 404 when the source dataset is not found", async () => {
      const PlatformDataDataset = {
        findOne: jest.fn().mockResolvedValue(null),
      };

      await expect(
        datasetRepository.getDatasetRecordById({
          PlatformDataDataset,
          datasetId: "missing-dataset",
          customerId: "customer-1",
          profileId: "profile-1",
        }),
      ).rejects.toMatchObject({
        message: "source dataset was not found for working data creation.",
        status: 404,
      });
    });

    it("does not open a customer transaction when customerId is missing", async () => {
      await expect(
        datasetRepository.getDatasetRecordById({
          PlatformDataDataset: { findOne: jest.fn() },
          datasetId: "source-dataset-123",
          profileId: "profile-1",
        }),
      ).rejects.toThrow("customerId is required.");

      expect(withCustomerTransaction).not.toHaveBeenCalled();
    });
  });

  describe("createWorkingDatasetRecord", () => {
    it("creates a dedicated working dataset record with source lineage", async () => {
      const PlatformDataWorkingDataset = {
        create: jest.fn().mockResolvedValue(createWorkingModelRecord()),
      };

      const result = await datasetRepository.createWorkingDatasetRecord({
        PlatformDataWorkingDataset,
        sourceDataset: createSourceDataset(),
        workingDataset: createWorkingDatasetCommand(),
      });

      expect(withCustomerTransaction).toHaveBeenCalledWith(
        "customer-1",
        expect.any(Function),
      );
      expect(PlatformDataWorkingDataset.create).toHaveBeenCalledWith(
        {
          id: "working-dataset-123",
          customerId: "customer-1",
          profileId: "profile-1",
          sourceDatasetId: "source-dataset-123",
          workingName: "July payments working data",
          datasetType: "payment",
          status: "in_progress",
          currentStepNumber: 1,
          storedFileName: "source-dataset-123.csv",
          storagePath:
            "platform/data/customer-1/datasets/source-dataset-123/payments.csv",
          mimeType: "text/csv",
          fileSize: 12345,
          headers: ["Supplier", "Invoice"],
          headersCount: 2,
          rowsCount: 1,
          lineage: {
            sourceDatasetId: "source-dataset-123",
            createdFrom: "immutable_dataset",
          },
          meta: {
            sourceDatasetId: "source-dataset-123",
            sourceOriginalFileName: "payments.csv",
          },
          createdBy: "user-123",
          updatedBy: "user-123",
        },
        { transaction: "mock-transaction" },
      );
      expect(result).toEqual({
        workingDatasetId: "working-dataset-123",
        sourceDatasetId: "source-dataset-123",
        customerId: "customer-1",
        profileId: "profile-1",
        workingName: "July payments working data",
        datasetType: "payment",
        status: "in_progress",
        currentStepNumber: 1,
        storagePath:
          "platform/data/customer-1/datasets/source-dataset-123/payments.csv",
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
        meta: {
          sourceDatasetId: "source-dataset-123",
          sourceOriginalFileName: "payments.csv",
        },
        activeEditor: {
          userId: null,
          sessionId: null,
          startedAt: null,
          lastSeenAt: null,
          expiresAt: null,
        },
        finalisedAt: null,
        finalisedBy: null,
        createdAt: "2026-07-06T00:00:00.000Z",
        updatedAt: "2026-07-06T00:00:00.000Z",
      });
    });

    it("normalises numeric working dataset fields returned as database strings", () => {
      const result = datasetRepository.normaliseWorkingDatasetRecord(
        createWorkingModelRecord({
          fileSize: "12345",
          headersCount: "2",
          rowsCount: "1",
        }),
      );

      expect(result.headersCount).toBe(2);
      expect(result.rowsCount).toBe(1);
    });

    it("throws when lineage is missing during normalisation", () => {
      expect(() =>
        datasetRepository.normaliseWorkingDatasetRecord(
          createWorkingModelRecord({ lineage: null }),
        ),
      ).toThrow("working dataset lineage is required.");
    });

    it("does not open a customer transaction when working dataset customerId is missing", async () => {
      await expect(
        datasetRepository.createWorkingDatasetRecord({
          PlatformDataWorkingDataset: { create: jest.fn() },
          sourceDataset: createSourceDataset(),
          workingDataset: createWorkingDatasetCommand({ customerId: null }),
        }),
      ).rejects.toThrow("customerId is required for persistence.");

      expect(withCustomerTransaction).not.toHaveBeenCalled();
    });
  });

  describe("createWorkingDatasetActivityRecord", () => {
    it("creates a working dataset activity record", async () => {
      const PlatformDataWorkingDatasetActivity = {
        create: jest.fn().mockResolvedValue(createWorkingActivityModelRecord()),
      };

      const result = await datasetRepository.createWorkingDatasetActivityRecord(
        {
          PlatformDataWorkingDatasetActivity,
          activity: {
            customerId: "customer-1",
            profileId: "profile-1",
            workingDatasetId: "working-dataset-123",
            activityType: "working_dataset_created",
            stepNumber: 1,
            summary: "Created working dataset July payments working data",
            details: {
              sourceDatasetId: "source-dataset-123",
            },
            relatedCapability: "data",
            relatedRecordId: "working-dataset-123",
            actor: {
              id: "user-123",
              role: "Admin",
              customerId: "customer-1",
            },
          },
        },
      );

      expect(withCustomerTransaction).toHaveBeenCalledWith(
        "customer-1",
        expect.any(Function),
      );
      expect(PlatformDataWorkingDatasetActivity.create).toHaveBeenCalledWith(
        {
          customerId: "customer-1",
          profileId: "profile-1",
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
        },
        { transaction: "mock-transaction" },
      );
      expect(result).toEqual({
        activityId: "activity-123",
        customerId: "customer-1",
        profileId: "profile-1",
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
      });
    });
  });

  describe("normaliseDatasetRecord", () => {
    it("normalises a plain persisted record", () => {
      const result = datasetRepository.normaliseDatasetRecord({
        id: "dataset123",
        customerId: "customer-1",
        profileId: "profile-1",
        datasetType: "payment",
        sourceType: "csv_upload",
        sourceName: "July payments",
        originalFileName: "payments.csv",
        storedFileName: "dataset123.csv",
        storagePath:
          "platform/data/customer-1/datasets/dataset123/payments.csv",
        mimeType: "text/csv",
        fileSize: 12345,
        headers: ["Supplier", "Invoice"],
        headersCount: 2,
        rowsCount: 1,
        status: "available",
        createdAt: "2026-07-05T00:00:00.000Z",
      });

      expect(result).toEqual({
        datasetId: "dataset123",
        customerId: "customer-1",
        profileId: "profile-1",
        datasetType: "payment",
        sourceType: "csv_upload",
        sourceName: "July payments",
        originalFileName: "payments.csv",
        storedFileName: "dataset123.csv",
        storagePath:
          "platform/data/customer-1/datasets/dataset123/payments.csv",
        mimeType: "text/csv",
        fileSize: 12345,
        headers: ["Supplier", "Invoice"],
        headersCount: 2,
        rowsCount: 1,
        status: "available",
        isImmutable: true,
        createdAt: "2026-07-05T00:00:00.000Z",
      });
    });

    it("normalises numeric database strings to numbers", () => {
      const result = datasetRepository.normaliseDatasetRecord({
        id: "dataset123",
        customerId: "customer-1",
        profileId: "profile-1",
        datasetType: "payment",
        sourceType: "csv_upload",
        sourceName: "July payments",
        originalFileName: "payments.csv",
        storedFileName: "dataset123.csv",
        storagePath:
          "platform/data/customer-1/datasets/dataset123/payments.csv",
        mimeType: "text/csv",
        fileSize: "12345",
        headers: ["Supplier", "Invoice"],
        headersCount: "2",
        rowsCount: "1",
        status: "available",
        createdAt: "2026-07-05T00:00:00.000Z",
      });

      expect(result.fileSize).toBe(12345);
      expect(result.headersCount).toBe(2);
      expect(result.rowsCount).toBe(1);
    });

    it("throws when persisted numeric fields are invalid", () => {
      expect(() =>
        datasetRepository.normaliseDatasetRecord({
          id: "dataset123",
          customerId: "customer-1",
          profileId: "profile-1",
          datasetType: "payment",
          sourceType: "csv_upload",
          sourceName: "July payments",
          originalFileName: "payments.csv",
          storedFileName: "dataset123.csv",
          storagePath:
            "platform/data/customer-1/datasets/dataset123/payments.csv",
          mimeType: "text/csv",
          fileSize: "not-a-number",
          headers: ["Supplier", "Invoice"],
          headersCount: "2",
          rowsCount: "1",
          status: "available",
          createdAt: "2026-07-05T00:00:00.000Z",
        }),
      ).toThrow("fileSize must be a non-negative integer.");
    });

    it("throws when the persisted record is missing", () => {
      expect(() => datasetRepository.normaliseDatasetRecord()).toThrow(
        "dataset record is required.",
      );
    });
  });
});

describe("finaliseWorkingDatasetRecord", () => {
  it("finalises a working dataset and clears active editor lease fields", async () => {
    const finalisedAt = new Date("2026-07-06T00:30:00.000Z");
    const workingRecord = createUpdatableWorkingModelRecord({
      activeEditorUserId: "user-123",
      activeEditorSessionId: "session-123",
      activeEditorStartedAt: new Date("2026-07-06T00:00:00.000Z"),
      activeEditorLastSeenAt: new Date("2026-07-06T00:10:00.000Z"),
      activeEditorExpiresAt: new Date("2026-07-06T00:40:00.000Z"),
    });
    const PlatformDataWorkingDataset = {
      findOne: jest.fn().mockResolvedValue(workingRecord),
    };

    const result = await datasetRepository.finaliseWorkingDatasetRecord({
      PlatformDataWorkingDataset,
      workingDatasetId: "working-dataset-123",
      customerId: "customer-1",
      profileId: "profile-1",
      actor: {
        id: "user-123",
        role: "Admin",
        customerId: "customer-1",
      },
      finalisedAt,
    });

    expect(withCustomerTransaction).toHaveBeenCalledWith(
      "customer-1",
      expect.any(Function),
    );
    expect(PlatformDataWorkingDataset.findOne).toHaveBeenCalledWith({
      where: {
        id: "working-dataset-123",
        customerId: "customer-1",
        profileId: "profile-1",
      },
      transaction: "mock-transaction",
    });
    expect(workingRecord.update).toHaveBeenCalledWith(
      {
        status: "final",
        finalisedAt,
        finalisedBy: "user-123",
        activeEditorUserId: null,
        activeEditorSessionId: null,
        activeEditorStartedAt: null,
        activeEditorLastSeenAt: null,
        activeEditorExpiresAt: null,
        updatedBy: "user-123",
      },
      { transaction: "mock-transaction" },
    );
    expect(result).toEqual({
      workingDatasetId: "working-dataset-123",
      sourceDatasetId: "source-dataset-123",
      customerId: "customer-1",
      profileId: "profile-1",
      workingName: "July payments working data",
      datasetType: "payment",
      status: "final",
      currentStepNumber: 1,
      storagePath:
        "platform/data/customer-1/datasets/source-dataset-123/payments.csv",
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
      meta: {
        sourceDatasetId: "source-dataset-123",
        sourceOriginalFileName: "payments.csv",
      },
      activeEditor: {
        userId: null,
        sessionId: null,
        startedAt: null,
        lastSeenAt: null,
        expiresAt: null,
      },
      finalisedAt: "2026-07-06T00:30:00.000Z",
      finalisedBy: "user-123",
      createdAt: "2026-07-06T00:00:00.000Z",
      updatedAt: "2026-07-06T00:30:00.000Z",
    });
  });

  it("throws 404 when finalising a missing working dataset", async () => {
    const PlatformDataWorkingDataset = {
      findOne: jest.fn().mockResolvedValue(null),
    };

    await expect(
      datasetRepository.finaliseWorkingDatasetRecord({
        PlatformDataWorkingDataset,
        workingDatasetId: "missing-working-dataset",
        customerId: "customer-1",
        profileId: "profile-1",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-1",
        },
        finalisedAt: new Date("2026-07-06T00:30:00.000Z"),
      }),
    ).rejects.toMatchObject({
      message: "working dataset was not found.",
      status: 404,
    });
  });
});

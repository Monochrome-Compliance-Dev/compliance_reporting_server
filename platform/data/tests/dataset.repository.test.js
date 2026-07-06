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

describe("dataset.repository", () => {
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

const fs = require("fs/promises");
const path = require("path");

jest.mock("fs/promises", () => ({
  mkdir: jest.fn(),
  copyFile: jest.fn(),
}));

const fileStorageService = require("@/platform/data/file-storage.service");

describe("file-storage.service", () => {
  beforeEach(() => {
    fs.mkdir.mockResolvedValue(undefined);
    fs.copyFile.mockResolvedValue(undefined);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("buildStoredFileName", () => {
    it("builds the stored CSV file name from the dataset id", () => {
      expect(fileStorageService.buildStoredFileName("dataset123")).toBe(
        "dataset123.csv",
      );
    });

    it("throws when datasetId is missing", () => {
      expect(() => fileStorageService.buildStoredFileName()).toThrow(
        "datasetId is required for stored file name.",
      );
    });
  });

  describe("buildDatasetStorageDirectory", () => {
    it("builds the customer dataset storage directory", () => {
      const result = fileStorageService.buildDatasetStorageDirectory({
        storageRoot: "/tmp/storage/data_hub",
        customerId: "customer-1",
      });

      expect(result).toBe(
        path.join("/tmp/storage/data_hub", "customer-1", "datasets"),
      );
    });

    it("throws when storageRoot is missing", () => {
      expect(() =>
        fileStorageService.buildDatasetStorageDirectory({
          storageRoot: null,
          customerId: "customer-1",
        }),
      ).toThrow("storageRoot is required for dataset storage.");
    });

    it("throws when customerId is missing", () => {
      expect(() =>
        fileStorageService.buildDatasetStorageDirectory({
          storageRoot: "/tmp/storage/data_hub",
        }),
      ).toThrow("customerId is required for dataset storage.");
    });
  });

  describe("buildDatasetStoragePath", () => {
    it("builds the customer dataset storage path", () => {
      const result = fileStorageService.buildDatasetStoragePath({
        storageRoot: "/tmp/storage/data_hub",
        customerId: "customer-1",
        datasetId: "dataset123",
      });

      expect(result).toBe(
        path.join(
          "/tmp/storage/data_hub",
          "customer-1",
          "datasets",
          "dataset123.csv",
        ),
      );
    });
  });

  describe("storeDatasetFile", () => {
    it("creates the dataset directory and copies the scanned immutable CSV file", async () => {
      const result = await fileStorageService.storeDatasetFile({
        storageRoot: "/tmp/storage/data_hub",
        customerId: "customer-1",
        datasetId: "dataset123",
        sourceFilePath: "/tmp/mc-platform-data-uploads/payments.csv",
      });

      expect(fs.mkdir).toHaveBeenCalledWith(
        path.join("/tmp/storage/data_hub", "customer-1", "datasets"),
        { recursive: true },
      );
      expect(fs.copyFile).toHaveBeenCalledWith(
        "/tmp/mc-platform-data-uploads/payments.csv",
        path.join(
          "/tmp/storage/data_hub",
          "customer-1",
          "datasets",
          "dataset123.csv",
        ),
        expect.any(Number),
      );
      expect(result).toEqual({
        storedFileName: "dataset123.csv",
        storagePath: path.join(
          "/tmp/storage/data_hub",
          "customer-1",
          "datasets",
          "dataset123.csv",
        ),
      });
    });

    it("throws when customerId is missing", async () => {
      await expect(
        fileStorageService.storeDatasetFile({
          datasetId: "dataset123",
          sourceFilePath: "/tmp/mc-platform-data-uploads/payments.csv",
        }),
      ).rejects.toThrow("customerId is required for dataset storage.");
    });

    it("throws when datasetId is missing", async () => {
      await expect(
        fileStorageService.storeDatasetFile({
          customerId: "customer-1",
          sourceFilePath: "/tmp/mc-platform-data-uploads/payments.csv",
        }),
      ).rejects.toThrow("datasetId is required for dataset storage.");
    });

    it("throws when sourceFilePath is missing", async () => {
      await expect(
        fileStorageService.storeDatasetFile({
          customerId: "customer-1",
          datasetId: "dataset123",
        }),
      ).rejects.toThrow("sourceFilePath is required for dataset storage.");
    });

    it("fails loudly when directory creation fails", async () => {
      fs.mkdir.mockRejectedValue(new Error("mkdir failed"));

      await expect(
        fileStorageService.storeDatasetFile({
          storageRoot: "/tmp/storage/data_hub",
          customerId: "customer-1",
          datasetId: "dataset123",
          sourceFilePath: "/tmp/mc-platform-data-uploads/payments.csv",
        }),
      ).rejects.toThrow("mkdir failed");
    });

    it("fails loudly when file copy fails", async () => {
      fs.copyFile.mockRejectedValue(new Error("copy failed"));

      await expect(
        fileStorageService.storeDatasetFile({
          storageRoot: "/tmp/storage/data_hub",
          customerId: "customer-1",
          datasetId: "dataset123",
          sourceFilePath: "/tmp/mc-platform-data-uploads/payments.csv",
        }),
      ).rejects.toThrow("copy failed");
    });
  });
});

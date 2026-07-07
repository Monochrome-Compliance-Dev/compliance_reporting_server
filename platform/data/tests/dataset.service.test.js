jest.mock("@/platform/data/csv-inspection.service", () => ({
  inspectCsvFile: jest.fn(),
}));

jest.mock("@/platform/data/dataset.contract", () => ({
  buildDatasetCreationResponse: jest.fn(),
}));

const csvInspectionService = require("@/platform/data/csv-inspection.service");
const datasetContract = require("@/platform/data/dataset.contract");
const datasetService = require("@/platform/data/dataset.service");

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

function createDatasetInput(overrides = {}) {
  return {
    command: createCommand(),
    datasetId: "dataset123",
    storageResult: createStorageResult(),
    ...overrides,
  };
}

describe("dataset.service", () => {
  beforeEach(() => {
    jest.useFakeTimers().setSystemTime(new Date("2026-07-05T00:00:00.000Z"));

    csvInspectionService.inspectCsvFile.mockReturnValue({
      headers: ["Supplier", "Invoice"],
      headersCount: 2,
      rowsCount: 1,
    });

    datasetContract.buildDatasetCreationResponse.mockImplementation(
      (dataset) => ({
        success: true,
        dataset,
      }),
    );
  });

  afterEach(() => {
    jest.clearAllMocks();
    jest.useRealTimers();
  });

  describe("createImmutableDatasetFromCommand", () => {
    it("creates the immutable dataset response from a dataset creation command and storage result", () => {
      const result =
        datasetService.createImmutableDatasetFromCommand(createDatasetInput());

      expect(csvInspectionService.inspectCsvFile).toHaveBeenCalledWith(
        "/tmp/mc-platform-data-uploads/payments.csv",
      );

      expect(datasetContract.buildDatasetCreationResponse).toHaveBeenCalledWith(
        {
          datasetId: "dataset123",
          customerId: "customer-123",
          profileId: "profile-123",
          datasetType: "payment",
          sourceType: "csv_upload",
          sourceName: "July payments",
          originalFileName: "payments.csv",
          storedFileName: "dataset123.csv",
          storagePath:
            "/tmp/storage/data_hub/customer-123/datasets/dataset123.csv",
          mimeType: "text/csv",
          fileSize: 12345,
          headers: ["Supplier", "Invoice"],
          headersCount: 2,
          rowsCount: 1,
          status: "available",
          isImmutable: true,
          createdAt: "2026-07-05T00:00:00.000Z",
        },
      );

      expect(result).toEqual({
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
          storagePath:
            "/tmp/storage/data_hub/customer-123/datasets/dataset123.csv",
          mimeType: "text/csv",
          fileSize: 12345,
          headers: ["Supplier", "Invoice"],
          headersCount: 2,
          rowsCount: 1,
          status: "available",
          isImmutable: true,
          createdAt: "2026-07-05T00:00:00.000Z",
        },
      });
    });

    it("throws when the dataset creation command is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({ command: null }),
        ),
      ).toThrow("dataset creation command is required.");
    });

    it("throws when datasetId is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({ datasetId: null }),
        ),
      ).toThrow("datasetId is required for dataset creation.");
    });

    it("throws when storage result is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({ storageResult: null }),
        ),
      ).toThrow("storage result is required for dataset creation.");
    });

    it("throws when storedFileName is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({
            storageResult: createStorageResult({ storedFileName: null }),
          }),
        ),
      ).toThrow("storedFileName is required for dataset creation.");
    });

    it("throws when storagePath is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({
            storageResult: createStorageResult({ storagePath: null }),
          }),
        ),
      ).toThrow("storagePath is required for dataset creation.");
    });

    it("throws when customerId is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({ command: createCommand({ customerId: null }) }),
        ),
      ).toThrow("customerId is required for dataset creation.");
    });

    it("throws when profileId is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({ command: createCommand({ profileId: null }) }),
        ),
      ).toThrow("profileId is required for dataset creation.");
    });

    it("throws when datasetType is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({ command: createCommand({ datasetType: null }) }),
        ),
      ).toThrow("datasetType is required for dataset creation.");
    });

    it("throws when sourceType is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({ command: createCommand({ sourceType: null }) }),
        ),
      ).toThrow("sourceType is required for dataset creation.");
    });

    it("throws when sourceName is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({ command: createCommand({ sourceName: null }) }),
        ),
      ).toThrow("sourceName is required for dataset creation.");
    });

    it("throws when file is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({ command: createCommand({ file: null }) }),
        ),
      ).toThrow("file is required for dataset creation.");
    });

    it("throws when originalFileName is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({
            command: createCommand({
              file: {
                ...createCommand().file,
                originalFileName: null,
              },
            }),
          }),
        ),
      ).toThrow("originalFileName is required for dataset creation.");
    });

    it("throws when mimeType is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({
            command: createCommand({
              file: {
                ...createCommand().file,
                mimeType: null,
              },
            }),
          }),
        ),
      ).toThrow("mimeType is required for dataset creation.");
    });

    it("throws when fileSize is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({
            command: createCommand({
              file: {
                ...createCommand().file,
                fileSize: null,
              },
            }),
          }),
        ),
      ).toThrow("fileSize is required for dataset creation.");
    });

    it("throws when file path is missing", () => {
      expect(() =>
        datasetService.createImmutableDatasetFromCommand(
          createDatasetInput({
            command: createCommand({
              file: {
                ...createCommand().file,
                path: null,
              },
            }),
          }),
        ),
      ).toThrow("file path is required for dataset creation.");
    });

    it("fails loudly when CSV inspection fails", () => {
      csvInspectionService.inspectCsvFile.mockImplementation(() => {
        throw new Error("CSV inspection failed");
      });

      expect(() =>
        datasetService.createImmutableDatasetFromCommand(createDatasetInput()),
      ).toThrow("CSV inspection failed");
    });

    it("fails loudly when dataset contract validation fails", () => {
      datasetContract.buildDatasetCreationResponse.mockImplementation(() => {
        throw new Error("dataset contract failed");
      });

      expect(() =>
        datasetService.createImmutableDatasetFromCommand(createDatasetInput()),
      ).toThrow("dataset contract failed");
    });
  });
});

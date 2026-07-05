const datasetContract = require("@/platform/data/dataset.contract");

function createValidDataset(overrides = {}) {
  return {
    datasetId: "dataset-123",
    customerId: "customer-123",
    profileId: "profile-123",
    datasetType: "payment",
    sourceType: "csv_upload",
    sourceName: "July payments",
    originalFileName: "payments.csv",
    storagePath: "platform/data/customer-123/dataset-123/payments.csv",
    mimeType: "text/csv",
    fileSize: 12345,
    headers: ["Supplier", "Invoice", "Paid Date"],
    headersCount: 3,
    rowsCount: 250,
    status: "available",
    isImmutable: true,
    createdAt: "2026-07-05T00:00:00.000Z",
    ...overrides,
  };
}

describe("dataset.contract", () => {
  describe("buildDatasetCreationResponse", () => {
    it("returns the immutable dataset creation response contract", () => {
      const result =
        datasetContract.buildDatasetCreationResponse(createValidDataset());

      expect(result).toEqual({
        success: true,
        dataset: {
          datasetId: "dataset-123",
          customerId: "customer-123",
          profileId: "profile-123",
          datasetType: "payment",
          sourceType: "csv_upload",
          sourceName: "July payments",
          originalFileName: "payments.csv",
          storagePath: "platform/data/customer-123/dataset-123/payments.csv",
          mimeType: "text/csv",
          fileSize: 12345,
          headers: ["Supplier", "Invoice", "Paid Date"],
          headersCount: 3,
          rowsCount: 250,
          status: "available",
          isImmutable: true,
          createdAt: "2026-07-05T00:00:00.000Z",
        },
      });
    });

    it("throws when mimeType is missing", () => {
      expect(() =>
        datasetContract.buildDatasetCreationResponse(
          createValidDataset({ mimeType: undefined }),
        ),
      ).toThrow("mimeType is required for dataset contract.");
    });

    it("throws when fileSize is missing", () => {
      expect(() =>
        datasetContract.buildDatasetCreationResponse(
          createValidDataset({ fileSize: undefined }),
        ),
      ).toThrow("fileSize is required for dataset contract.");
    });

    it("throws when the dataset is missing", () => {
      expect(() => datasetContract.buildDatasetCreationResponse()).toThrow(
        "dataset is required.",
      );
    });

    it("throws when a required dataset field is missing", () => {
      expect(() =>
        datasetContract.buildDatasetCreationResponse(
          createValidDataset({ datasetId: null }),
        ),
      ).toThrow("datasetId is required for dataset contract.");
    });

    it("throws when headers is not an array", () => {
      expect(() =>
        datasetContract.buildDatasetCreationResponse(
          createValidDataset({ headers: "Supplier,Invoice" }),
        ),
      ).toThrow("headers must be an array.");
    });

    it("throws when headers is empty", () => {
      expect(() =>
        datasetContract.buildDatasetCreationResponse(
          createValidDataset({ headers: [], headersCount: 0 }),
        ),
      ).toThrow("headers must include at least one header.");
    });

    it("throws when headersCount does not match headers length", () => {
      expect(() =>
        datasetContract.buildDatasetCreationResponse(
          createValidDataset({ headersCount: 2 }),
        ),
      ).toThrow("headersCount must match headers length.");
    });

    it("throws when rowsCount is not a non-negative integer", () => {
      expect(() =>
        datasetContract.buildDatasetCreationResponse(
          createValidDataset({ rowsCount: -1 }),
        ),
      ).toThrow("rowsCount must be a non-negative integer.");
    });

    it("throws when fileSize is not a non-negative integer", () => {
      expect(() =>
        datasetContract.buildDatasetCreationResponse(
          createValidDataset({ fileSize: -1 }),
        ),
      ).toThrow("fileSize must be a non-negative integer.");
    });

    it("throws when isImmutable is not true", () => {
      expect(() =>
        datasetContract.buildDatasetCreationResponse(
          createValidDataset({ isImmutable: false }),
        ),
      ).toThrow("isImmutable must be true for dataset contract.");
    });

    it("rejects PTRS-specific fields", () => {
      expect(() =>
        datasetContract.buildDatasetCreationResponse(
          createValidDataset({ ptrsId: "ptrs-123" }),
        ),
      ).toThrow("ptrsId is not allowed in dataset contract.");
    });
  });
});

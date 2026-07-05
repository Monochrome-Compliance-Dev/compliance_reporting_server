const acquisitionService = require("@/platform/data/acquisition.service");

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

describe("acquisition.service", () => {
  describe("buildDatasetCreationCommand", () => {
    it("returns a normalised Data dataset creation command", () => {
      const result = acquisitionService.buildDatasetCreationCommand({
        executionContext: createExecutionContext(),
        body: createBody(),
        file: createFile(),
      });

      expect(result).toEqual({
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
      });
    });

    it("trims body string fields", () => {
      const result = acquisitionService.buildDatasetCreationCommand({
        executionContext: createExecutionContext(),
        body: createBody({
          sourceName: "  July payments  ",
          datasetType: "  payment  ",
          profileId: "  profile-123  ",
        }),
        file: createFile(),
      });

      expect(result.sourceName).toBe("July payments");
      expect(result.datasetType).toBe("payment");
      expect(result.profileId).toBe("profile-123");
    });

    it("normalises file mimetype to lowercase", () => {
      const result = acquisitionService.buildDatasetCreationCommand({
        executionContext: createExecutionContext(),
        body: createBody(),
        file: createFile({ mimetype: "TEXT/CSV" }),
      });

      expect(result.file.mimeType).toBe("text/csv");
    });

    it("allows CSV files by extension when the MIME type is generic", () => {
      const result = acquisitionService.buildDatasetCreationCommand({
        executionContext: createExecutionContext(),
        body: createBody(),
        file: createFile({ mimetype: "application/octet-stream" }),
      });

      expect(result.file.originalFileName).toBe("payments.csv");
    });

    it("throws when actorId is missing", () => {
      expect(() =>
        acquisitionService.buildDatasetCreationCommand({
          executionContext: createExecutionContext({ actorId: null }),
          body: createBody(),
          file: createFile(),
        }),
      ).toThrow("actorId is required for dataset creation.");
    });

    it("throws when customerId is missing", () => {
      expect(() =>
        acquisitionService.buildDatasetCreationCommand({
          executionContext: createExecutionContext({ customerId: null }),
          body: createBody(),
          file: createFile(),
        }),
      ).toThrow("customerId is required for dataset creation.");
    });

    it("throws when sourceName is missing", () => {
      expect(() =>
        acquisitionService.buildDatasetCreationCommand({
          executionContext: createExecutionContext(),
          body: createBody({ sourceName: "" }),
          file: createFile(),
        }),
      ).toThrow("sourceName is required for dataset creation.");
    });

    it("throws when datasetType is missing", () => {
      expect(() =>
        acquisitionService.buildDatasetCreationCommand({
          executionContext: createExecutionContext(),
          body: createBody({ datasetType: "" }),
          file: createFile(),
        }),
      ).toThrow("datasetType is required for dataset creation.");
    });

    it("throws when datasetType is unsupported", () => {
      expect(() =>
        acquisitionService.buildDatasetCreationCommand({
          executionContext: createExecutionContext(),
          body: createBody({ datasetType: "ptrs_payment" }),
          file: createFile(),
        }),
      ).toThrow("datasetType is not supported for dataset creation.");
    });

    it("throws when profileId is missing", () => {
      expect(() =>
        acquisitionService.buildDatasetCreationCommand({
          executionContext: createExecutionContext(),
          body: createBody({ profileId: "" }),
          file: createFile(),
        }),
      ).toThrow("profileId is required for dataset creation.");
    });

    it("throws when file is missing", () => {
      expect(() =>
        acquisitionService.buildDatasetCreationCommand({
          executionContext: createExecutionContext(),
          body: createBody(),
        }),
      ).toThrow("file is required for dataset creation.");
    });

    it("throws when file.originalname is missing", () => {
      expect(() =>
        acquisitionService.buildDatasetCreationCommand({
          executionContext: createExecutionContext(),
          body: createBody(),
          file: createFile({ originalname: "" }),
        }),
      ).toThrow("file.originalname is required for dataset creation.");
    });

    it("throws when file.mimetype is missing", () => {
      expect(() =>
        acquisitionService.buildDatasetCreationCommand({
          executionContext: createExecutionContext(),
          body: createBody(),
          file: createFile({ mimetype: "" }),
        }),
      ).toThrow("file.mimetype is required for dataset creation.");
    });

    it("throws when file.size is missing", () => {
      expect(() =>
        acquisitionService.buildDatasetCreationCommand({
          executionContext: createExecutionContext(),
          body: createBody(),
          file: createFile({ size: undefined }),
        }),
      ).toThrow("file.size is required for dataset creation.");
    });

    it("throws when file.size is not positive", () => {
      expect(() =>
        acquisitionService.buildDatasetCreationCommand({
          executionContext: createExecutionContext(),
          body: createBody(),
          file: createFile({ size: 0 }),
        }),
      ).toThrow("file.size must be a positive integer.");
    });

    it("throws when file is not CSV", () => {
      expect(() =>
        acquisitionService.buildDatasetCreationCommand({
          executionContext: createExecutionContext(),
          body: createBody(),
          file: createFile({
            originalname: "payments.xlsx",
            mimetype:
              "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
          }),
        }),
      ).toThrow("file must be a CSV file.");
    });
  });
});

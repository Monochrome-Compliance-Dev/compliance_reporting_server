const express = require("express");
const request = require("supertest");

jest.mock("@/middleware/authorise", () => () => [
  (req, res, next) => {
    req.auth = {
      id: "user-123",
    };
    req.user = {
      id: "user-123",
      role: "Admin",
      customerId: "home-customer-123",
    };
    req.effectiveCustomerId = "customer-123";
    req.tenantCustomerId = "customer-123";
    req.actingRole = "Admin";
    next();
  },
]);

jest.mock("@/platform/data/data.service", () => ({
  createDataset: jest.fn(),
}));

const dataService = require("@/platform/data/data.service");
const { createDataRouter } = require("@/platform/data/data.routes");

function createApp() {
  const app = express();

  app.use(
    "/api/platform/data",
    createDataRouter({ PlatformDataDataset: "PlatformDataDatasetModel" }),
  );

  app.use((error, req, res, next) => {
    res.status(error.status || 500).json({
      success: false,
      error: error.message,
    });
  });

  return app;
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

describe("data.routes", () => {
  beforeEach(() => {
    dataService.createDataset.mockResolvedValue(createDatasetResponse());
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("POST /api/platform/data/datasets", () => {
    it("creates a Data-owned dataset from multipart form data", async () => {
      const app = createApp();

      const response = await request(app)
        .post("/api/platform/data/datasets")
        .field("sourceName", "July payments")
        .field("datasetType", "payment")
        .field("profileId", "profile-123")
        .attach("file", Buffer.from("Supplier,Invoice\nABC,INV-001\n"), {
          filename: "payments.csv",
          contentType: "text/csv",
        });

      expect(response.status).toBe(201);
      expect(response.body).toEqual(createDatasetResponse());
      expect(dataService.createDataset).toHaveBeenCalledWith({
        executionContext: {
          actorId: "user-123",
          role: "Admin",
          customerId: "customer-123",
          source: "tenantContext",
        },
        body: {
          sourceName: "July payments",
          datasetType: "payment",
          profileId: "profile-123",
        },
        file: expect.objectContaining({
          originalname: "payments.csv",
          mimetype: "text/csv",
          size: Buffer.byteLength("Supplier,Invoice\nABC,INV-001\n"),
          buffer: Buffer.from("Supplier,Invoice\nABC,INV-001\n"),
        }),
        PlatformDataDataset: "PlatformDataDatasetModel",
      });
    });

    it("returns service errors through the error handler", async () => {
      const error = new Error("dataset creation failed");
      error.status = 400;
      dataService.createDataset.mockRejectedValue(error);

      const app = createApp();

      const response = await request(app)
        .post("/api/platform/data/datasets")
        .field("sourceName", "July payments")
        .field("datasetType", "payment")
        .field("profileId", "profile-123")
        .attach("file", Buffer.from("Supplier,Invoice\nABC,INV-001\n"), {
          filename: "payments.csv",
          contentType: "text/csv",
        });

      expect(response.status).toBe(400);
      expect(response.body).toEqual({
        success: false,
        error: "dataset creation failed",
      });
    });
  });
});

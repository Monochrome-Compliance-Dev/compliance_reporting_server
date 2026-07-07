const express = require("express");
const request = require("supertest");

let MockauthoriseScenario = "allowed";

jest.mock("@/middleware/authorise", () => () => [
  (req, res, next) => {
    if (MockauthoriseScenario === "missingCredentials") {
      return res.status(401).json({
        status: "unauthorised",
        reason: "credentials_missing",
        message: "Unauthorised",
      });
    }

    if (MockauthoriseScenario === "roleDenied") {
      return res.status(401).json({
        status: "unauthorised",
        reason: "role_denied",
        message: "Unauthorised",
      });
    }

    if (MockauthoriseScenario === "missingCustomerContext") {
      req.auth = {
        id: "user-123",
      };
      req.user = {
        id: "user-123",
        role: "Admin",
      };
      req.actingRole = "Admin";
      return next();
    }

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
    return next();
  },
]);

jest.mock("@/platform/data/data.service", () => ({
  createDataset: jest.fn(),
  createWorkingDataset: jest.fn(),
}));

const dataService = require("@/platform/data/data.service");
const { createDataRouter } = require("@/platform/data/data.routes");

function createApp() {
  const app = express();
  app.use(express.json());

  app.use(
    "/api/platform/data",
    createDataRouter({
      PlatformDataDataset: "PlatformDataDatasetModel",
      PlatformDataWorkingDataset: "PlatformDataWorkingDatasetModel",
      PlatformDataWorkingDatasetActivity:
        "PlatformDataWorkingDatasetActivityModel",
    }),
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

function createWorkingDatasetResponse(overrides = {}) {
  return {
    success: true,
    workingDataset: {
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
    },
    activity: {
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
    },
    ...overrides,
  };
}

function postDataset(app) {
  return request(app)
    .post("/api/platform/data/datasets")
    .field("sourceName", "July payments")
    .field("datasetType", "payment")
    .field("profileId", "profile-123")
    .attach("file", Buffer.from("Supplier,Invoice\nABC,INV-001\n"), {
      filename: "payments.csv",
      contentType: "text/csv",
    });
}

function postWorkingDataset(app, body = {}) {
  return request(app)
    .post("/api/platform/data/working-datasets")
    .send({
      sourceDatasetId: "source-dataset-123",
      profileId: "profile-123",
      workingName: "July payments working data",
      ...body,
    });
}

describe("data.routes", () => {
  beforeEach(() => {
    MockauthoriseScenario = "allowed";
    dataService.createDataset.mockResolvedValue(createDatasetResponse());
    dataService.createWorkingDataset.mockResolvedValue(
      createWorkingDatasetResponse(),
    );
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("POST /api/platform/data/datasets", () => {
    it("creates a Data-owned dataset from multipart form data", async () => {
      const app = createApp();

      const response = await postDataset(app);

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
          path: expect.stringContaining("payments.csv"),
        }),
        PlatformDataDataset: "PlatformDataDatasetModel",
      });
    });

    it("returns service errors through the error handler", async () => {
      const error = new Error("dataset creation failed");
      error.status = 400;
      dataService.createDataset.mockRejectedValue(error);

      const app = createApp();

      const response = await postDataset(app);

      expect(response.status).toBe(400);
      expect(response.body).toEqual({
        success: false,
        error: "dataset creation failed",
      });
    });

    it("does not reach the Data service when credentials are missing", async () => {
      MockauthoriseScenario = "missingCredentials";
      const app = createApp();

      const response = await postDataset(app);

      expect(response.status).toBe(401);
      expect(response.body).toEqual({
        status: "unauthorised",
        reason: "credentials_missing",
        message: "Unauthorised",
      });
      expect(dataService.createDataset).not.toHaveBeenCalled();
    });

    it("does not reach the Data service when the authenticated role is not allowed", async () => {
      MockauthoriseScenario = "roleDenied";
      const app = createApp();

      const response = await postDataset(app);

      expect(response.status).toBe(401);
      expect(response.body).toEqual({
        status: "unauthorised",
        reason: "role_denied",
        message: "Unauthorised",
      });
      expect(dataService.createDataset).not.toHaveBeenCalled();
    });

    it("fails before the Data service when authenticated context has no customer context", async () => {
      MockauthoriseScenario = "missingCustomerContext";
      const app = createApp();

      const response = await postDataset(app);

      expect(response.status).toBe(500);
      expect(response.body).toEqual({
        success: false,
        error: "customerId is required for governed execution.",
      });
      expect(dataService.createDataset).not.toHaveBeenCalled();
    });

    it("surfaces Data Security denials as forbidden responses", async () => {
      const error = new Error(
        "Customer context does not match governed execution.",
      );
      error.status = 403;
      error.securityObservation = {
        outcome: "denied",
        reason: "customer_mismatch",
      };
      dataService.createDataset.mockRejectedValue(error);

      const app = createApp();

      const response = await postDataset(app);

      expect(response.status).toBe(403);
      expect(response.body).toEqual({
        success: false,
        error: "Customer context does not match governed execution.",
      });
    });
  });

  describe("POST /api/platform/data/working-datasets", () => {
    it("creates a Data-owned working dataset from JSON request body", async () => {
      const app = createApp();

      const response = await postWorkingDataset(app);

      expect(response.status).toBe(201);
      expect(response.body).toEqual(createWorkingDatasetResponse());
      expect(dataService.createWorkingDataset).toHaveBeenCalledWith({
        executionContext: {
          actorId: "user-123",
          role: "Admin",
          customerId: "customer-123",
          source: "tenantContext",
        },
        body: {
          sourceDatasetId: "source-dataset-123",
          profileId: "profile-123",
          workingName: "July payments working data",
        },
        PlatformDataDataset: "PlatformDataDatasetModel",
        PlatformDataWorkingDataset: "PlatformDataWorkingDatasetModel",
        PlatformDataWorkingDatasetActivity:
          "PlatformDataWorkingDatasetActivityModel",
      });
    });

    it("returns working dataset service errors through the error handler", async () => {
      const error = new Error("working dataset creation failed");
      error.status = 400;
      dataService.createWorkingDataset.mockRejectedValue(error);

      const app = createApp();

      const response = await postWorkingDataset(app);

      expect(response.status).toBe(400);
      expect(response.body).toEqual({
        success: false,
        error: "working dataset creation failed",
      });
    });

    it("does not reach working dataset creation when credentials are missing", async () => {
      MockauthoriseScenario = "missingCredentials";
      const app = createApp();

      const response = await postWorkingDataset(app);

      expect(response.status).toBe(401);
      expect(response.body).toEqual({
        status: "unauthorised",
        reason: "credentials_missing",
        message: "Unauthorised",
      });
      expect(dataService.createWorkingDataset).not.toHaveBeenCalled();
    });

    it("does not reach working dataset creation when the authenticated role is not allowed", async () => {
      MockauthoriseScenario = "roleDenied";
      const app = createApp();

      const response = await postWorkingDataset(app);

      expect(response.status).toBe(401);
      expect(response.body).toEqual({
        status: "unauthorised",
        reason: "role_denied",
        message: "Unauthorised",
      });
      expect(dataService.createWorkingDataset).not.toHaveBeenCalled();
    });

    it("fails before working dataset creation when authenticated context has no customer context", async () => {
      MockauthoriseScenario = "missingCustomerContext";
      const app = createApp();

      const response = await postWorkingDataset(app);

      expect(response.status).toBe(500);
      expect(response.body).toEqual({
        success: false,
        error: "customerId is required for governed execution.",
      });
      expect(dataService.createWorkingDataset).not.toHaveBeenCalled();
    });

    it("surfaces working dataset Security denials as forbidden responses", async () => {
      const error = new Error(
        "Customer context does not match governed execution.",
      );
      error.status = 403;
      error.securityObservation = {
        outcome: "denied",
        reason: "customer_mismatch",
      };
      dataService.createWorkingDataset.mockRejectedValue(error);

      const app = createApp();

      const response = await postWorkingDataset(app);

      expect(response.status).toBe(403);
      expect(response.body).toEqual({
        success: false,
        error: "Customer context does not match governed execution.",
      });
    });
  });
});

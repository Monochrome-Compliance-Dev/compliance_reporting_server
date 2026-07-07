const express = require("express");
const request = require("supertest");

jest.mock("@/platform/identity/identity.service", () => ({
  attachExecutionContext: (req, res, next) => {
    req.executionContext = {
      actorId: "user-123",
      role: "Admin",
      customerId: "customer-123",
    };
    next();
  },
}));

jest.mock("@/middleware/authorise", () =>
  jest.fn(() => (req, res, next) => next()),
);

jest.mock("@/platform/transformation/transformation.service", () => ({
  materialiseWorkingDataset: jest.fn(),
}));

const transformationService = require("@/platform/transformation/transformation.service");
const {
  createTransformationRouter,
} = require("@/platform/transformation/transformation.routes");

function createApp(models = {}) {
  const app = express();

  app.use(express.json());
  app.use("/api/platform/transformation", createTransformationRouter(models));
  app.use((error, req, res, next) => {
    res.status(error.status || 500).json({
      message: error.message,
    });
  });

  return app;
}

function createModels() {
  return {
    PlatformDataWorkingDataset: "PlatformDataWorkingDatasetModel",
    PlatformDataWorkingDatasetActivity:
      "PlatformDataWorkingDatasetActivityModel",
  };
}

function createBody(overrides = {}) {
  return {
    profileId: "profile-123",
    editorSessionId: "session-123",
    fields: [
      {
        sourceField: "Invoice No",
        targetField: "invoice_reference_number",
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

describe("transformation.routes", () => {
  beforeEach(() => {
    transformationService.materialiseWorkingDataset.mockResolvedValue({
      success: true,
      workingDataset: {
        workingDatasetId: "working-dataset-123",
        status: "in_progress",
      },
      activity: {
        activityId: "activity-123",
        activityType: "working_dataset_materialised",
      },
    });
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it("materialises a working dataset", async () => {
    const app = createApp(createModels());

    const response = await request(app)
      .post(
        "/api/platform/transformation/working-datasets/working-dataset-123/materialise",
      )
      .send(createBody());

    expect(response.status).toBe(200);
    expect(response.body).toEqual({
      success: true,
      workingDataset: {
        workingDatasetId: "working-dataset-123",
        status: "in_progress",
      },
      activity: {
        activityId: "activity-123",
        activityType: "working_dataset_materialised",
      },
    });
    expect(
      transformationService.materialiseWorkingDataset,
    ).toHaveBeenCalledWith({
      executionContext: {
        actorId: "user-123",
        role: "Admin",
        customerId: "customer-123",
      },
      params: {
        workingDatasetId: "working-dataset-123",
      },
      body: createBody(),
      PlatformDataWorkingDataset: "PlatformDataWorkingDatasetModel",
      PlatformDataWorkingDatasetActivity:
        "PlatformDataWorkingDatasetActivityModel",
    });
  });

  it("returns materialisation service errors through the error handler", async () => {
    const error = new Error("materialisation failed");
    error.status = 409;
    transformationService.materialiseWorkingDataset.mockRejectedValue(error);
    const app = createApp(createModels());

    const response = await request(app)
      .post(
        "/api/platform/transformation/working-datasets/working-dataset-123/materialise",
      )
      .send(createBody());

    expect(response.status).toBe(409);
    expect(response.body).toEqual({
      message: "materialisation failed",
    });
  });
});

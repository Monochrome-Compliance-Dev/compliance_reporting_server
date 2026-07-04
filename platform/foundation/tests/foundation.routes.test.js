const express = require("express");
const request = require("supertest");

jest.mock("@/middleware/authorise", () =>
  jest.fn(() => (req, res, next) => {
    req.auth = {
      id: "user-123",
      role: "Admin",
      customerId: "customer-123",
    };
    next();
  }),
);

jest.mock("@/platform/audit/audit.service", () => ({
  recordFoundationAudit: jest.fn().mockResolvedValue({
    eventType: "platform.foundation.executed",
  }),
}));

const authorise = require("@/middleware/authorise");
const foundationService = require("@/platform/foundation/foundation.service");
const foundationRoutes = require("@/platform/foundation/foundation.routes");

function createTestApp() {
  const app = express();

  app.use(express.json());
  app.use("/", foundationRoutes);

  app.use((error, req, res, next) => {
    res.status(error.status || 500).json({
      message: error.message,
    });
  });

  return app;
}

describe("foundation.routes", () => {
  it("uses platform access authorisation", () => {
    expect(authorise).toHaveBeenCalledWith({
      roles: ["Admin", "Boss", "User"],
    });
  });

  it("returns a successful foundation envelope", async () => {
    const response = await request(createTestApp()).post("/").send({});

    expect(response.status).toBe(200);
    expect(response.body).toEqual({
      success: true,
      foundationId: expect.any(String),
      capability: "foundation",
      message: "Platform foundation executed successfully.",
      actor: {
        id: "user-123",
        role: "Admin",
        customerId: "customer-123",
      },
    });
  });

  it("passes controller errors to Express error handling", async () => {
    const error = new Error("Route failure test.");
    error.status = 418;

    jest
      .spyOn(foundationService, "executeFoundation")
      .mockImplementationOnce(() => {
        throw error;
      });

    const response = await request(createTestApp()).post("/").send({});

    expect(response.status).toBe(418);
    expect(response.body).toEqual({
      message: "Route failure test.",
    });
  });
});

const express = require("express");
const request = require("supertest");

jest.mock("@/helpers/logger", () => ({
  logger: {
    logEvent: jest.fn(),
  },
}));

const { logger } = require("@/helpers/logger");
const {
  createRequestBodyMiddleware,
  requestSizeErrorHandler,
} = require("@/platform/security/request-size.middleware");

function createApp(options) {
  const app = express();

  app.use((request, response, next) => {
    request.id = "request-123";
    next();
  });

  app.use(...createRequestBodyMiddleware(options));

  app.post("/test", (request, response) => {
    response.status(200).json({
      status: "success",
      body: request.body,
    });
  });

  app.use(requestSizeErrorHandler);

  return app;
}

describe("request-size.middleware", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("createRequestBodyMiddleware", () => {
    it("fails loudly when jsonBodyLimit is missing", () => {
      expect(() =>
        createRequestBodyMiddleware({
          jsonBodyLimit: "",
          urlencodedBodyLimit: "100kb",
        }),
      ).toThrow("jsonBodyLimit is required for request size protection.");
    });

    it("fails loudly when urlencodedBodyLimit is missing", () => {
      expect(() =>
        createRequestBodyMiddleware({
          jsonBodyLimit: "100kb",
          urlencodedBodyLimit: "",
        }),
      ).toThrow("urlencodedBodyLimit is required for request size protection.");
    });

    it("fails loudly when urlencodedParameterLimit is invalid", () => {
      expect(() =>
        createRequestBodyMiddleware({
          jsonBodyLimit: "100kb",
          urlencodedBodyLimit: "100kb",
          urlencodedParameterLimit: 0,
        }),
      ).toThrow("Request size parameter limits must be positive integers.");
    });

    it("allows JSON payloads below the configured limit", async () => {
      const app = createApp({
        jsonBodyLimit: "1kb",
        urlencodedBodyLimit: "1kb",
        urlencodedParameterLimit: 10,
      });

      const response = await request(app).post("/test").send({
        message: "small payload",
      });

      expect(response.status).toBe(200);
      expect(response.body).toEqual({
        status: "success",
        body: {
          message: "small payload",
        },
      });
      expect(logger.logEvent).not.toHaveBeenCalled();
    });

    it("allows URL-encoded payloads below the configured limit", async () => {
      const app = createApp({
        jsonBodyLimit: "1kb",
        urlencodedBodyLimit: "1kb",
        urlencodedParameterLimit: 10,
      });

      const response = await request(app).post("/test").type("form").send({
        name: "Darryll",
      });

      expect(response.status).toBe(200);
      expect(response.body).toEqual({
        status: "success",
        body: {
          name: "Darryll",
        },
      });
      expect(logger.logEvent).not.toHaveBeenCalled();
    });
  });

  describe("requestSizeErrorHandler", () => {
    it("returns a governed 413 response for oversized JSON", async () => {
      const app = createApp({
        jsonBodyLimit: "100b",
        urlencodedBodyLimit: "1kb",
        urlencodedParameterLimit: 10,
      });

      const response = await request(app)
        .post("/test")
        .send({
          message: "x".repeat(500),
        });

      expect(response.status).toBe(413);
      expect(response.body).toEqual({
        status: "error",
        code: "PAYLOAD_TOO_LARGE",
        message: "Request payload exceeds the permitted size.",
        requestId: "request-123",
      });

      expect(logger.logEvent).toHaveBeenCalledWith(
        "warn",
        "Request payload rejected",
        expect.objectContaining({
          action: "RequestPayloadRejected",
          requestId: "request-123",
          method: "POST",
          path: "/test",
          contentType: expect.stringContaining("application/json"),
          reason: "entity.too.large",
        }),
      );
    });

    it("returns a governed 413 response for oversized URL-encoded input", async () => {
      const app = createApp({
        jsonBodyLimit: "1kb",
        urlencodedBodyLimit: "100b",
        urlencodedParameterLimit: 10,
      });

      const response = await request(app)
        .post("/test")
        .type("form")
        .send({
          message: "x".repeat(500),
        });

      expect(response.status).toBe(413);
      expect(response.body).toEqual({
        status: "error",
        code: "PAYLOAD_TOO_LARGE",
        message: "Request payload exceeds the permitted size.",
        requestId: "request-123",
      });

      expect(logger.logEvent).toHaveBeenCalledWith(
        "warn",
        "Request payload rejected",
        expect.objectContaining({
          action: "RequestPayloadRejected",
          requestId: "request-123",
          method: "POST",
          path: "/test",
          contentType: expect.stringContaining(
            "application/x-www-form-urlencoded",
          ),
          reason: "entity.too.large",
        }),
      );
    });

    it("does not log or expose the submitted payload", async () => {
      const sensitiveValue = "sensitive-secret-value";
      const app = createApp({
        jsonBodyLimit: "100b",
        urlencodedBodyLimit: "1kb",
        urlencodedParameterLimit: 10,
      });

      const response = await request(app)
        .post("/test")
        .send({
          token: sensitiveValue.repeat(20),
        });

      expect(response.status).toBe(413);

      const loggedPayload = JSON.stringify(logger.logEvent.mock.calls);
      const responsePayload = JSON.stringify(response.body);

      expect(loggedPayload).not.toContain(sensitiveValue);
      expect(responsePayload).not.toContain(sensitiveValue);
    });

    it("passes non-size errors to the next error handler", () => {
      const error = new Error("Unexpected parser failure");
      const request = {
        id: "request-123",
        method: "POST",
        originalUrl: "/test",
        headers: {},
      };
      const response = {};
      const next = jest.fn();

      requestSizeErrorHandler(error, request, response, next);

      expect(next).toHaveBeenCalledWith(error);
      expect(logger.logEvent).not.toHaveBeenCalled();
    });
  });
});

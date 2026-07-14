const express = require("express");
const request = require("supertest");

jest.mock("fs", () => ({
  mkdirSync: jest.fn(),
  appendFileSync: jest.fn(),
}));

jest.mock("@/helpers/logger", () => ({
  logger: {
    logEvent: jest.fn(),
  },
}));

function createTestApp(limiter, route = "/test") {
  const app = express();
  app.set("trust proxy", 1);

  app.use(express.json());

  app.use((req, res, next) => {
    req.id = "request-123";
    next();
  });

  app.use(route, limiter);

  app.get(route, (req, res) => {
    res.status(200).json({
      status: "success",
    });
  });

  app.post(route, (req, res) => {
    res.status(200).json({
      status: "success",
    });
  });

  return app;
}

function loadRateLimitingMiddleware() {
  jest.resetModules();

  const middleware = require("@/platform/security/rate-limiting.middleware");
  const fs = require("fs");
  const { logger } = require("@/helpers/logger");

  return {
    ...middleware,
    fs,
    logger,
  };
}

describe("rate-limiting.middleware", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("apiLimiter", () => {
    it("allows API requests below the configured threshold", async () => {
      const { apiLimiter, fs, logger } = loadRateLimitingMiddleware();
      const app = createTestApp(apiLimiter);

      const firstResponse = await request(app).get("/test");
      const secondResponse = await request(app).get("/test");

      expect(firstResponse.status).toBe(200);
      expect(secondResponse.status).toBe(200);
      expect(logger.logEvent).not.toHaveBeenCalled();
      expect(fs.appendFileSync).not.toHaveBeenCalled();
    });
  });

  describe("loginLimiter", () => {
    it("returns a governed 429 response after five attempts", async () => {
      const { loginLimiter, fs, logger } = loadRateLimitingMiddleware();
      const app = createTestApp(loginLimiter, "/login");

      for (let attempt = 0; attempt < 5; attempt += 1) {
        const response = await request(app).post("/login").send({
          email: "test@example.com",
        });

        expect(response.status).toBe(200);
      }

      const limitedResponse = await request(app).post("/login").send({
        email: "test@example.com",
      });

      expect(limitedResponse.status).toBe(429);
      expect(limitedResponse.body).toEqual({
        status: "error",
        code: "RATE_LIMITED",
        message:
          "Too many login attempts from this IP, please try again later.",
        requestId: "request-123",
      });
    });

    it("records one rate-limit event for a blocked login request", async () => {
      const { loginLimiter, fs, logger } = loadRateLimitingMiddleware();
      const app = createTestApp(loginLimiter, "/login");

      for (let attempt = 0; attempt < 6; attempt += 1) {
        await request(app)
          .post("/login")
          .set("x-forwarded-for", "203.0.113.10")
          .send({
            email: "test@example.com",
          });
      }

      expect(logger.logEvent).toHaveBeenCalledTimes(1);
      expect(logger.logEvent).toHaveBeenCalledWith(
        "warn",
        "RateLimit429",
        expect.objectContaining({
          type: "rate_limit",
          stage: "limiter_handler",
          limiter: "login",
          requestId: "request-123",
          method: "POST",
          path: "/login",
          ip: "203.0.113.10",
          hasAuthHeader: false,
          hasCookie: false,
        }),
      );

      expect(fs.mkdirSync).toHaveBeenCalledTimes(1);
      expect(fs.appendFileSync).toHaveBeenCalledTimes(1);
    });
  });

  describe("emailLimiter", () => {
    it("returns a governed 429 response after five attempts", async () => {
      const { emailLimiter, logger } = loadRateLimitingMiddleware();
      const app = createTestApp(emailLimiter, "/email");

      for (let attempt = 0; attempt < 5; attempt += 1) {
        const response = await request(app).post("/email").send({
          recipient: "test@example.com",
        });

        expect(response.status).toBe(200);
      }

      const limitedResponse = await request(app).post("/email").send({
        recipient: "test@example.com",
      });

      expect(limitedResponse.status).toBe(429);
      expect(limitedResponse.body).toEqual({
        status: "error",
        code: "RATE_LIMITED",
        message: "Too many attempts, please try again later.",
        requestId: "request-123",
      });

      expect(logger.logEvent).toHaveBeenCalledTimes(1);
      expect(logger.logEvent).toHaveBeenCalledWith(
        "warn",
        "RateLimit429",
        expect.objectContaining({
          type: "rate_limit",
          stage: "limiter_handler",
          limiter: "email",
        }),
      );
    });
  });
});

const express = require("express");
const request = require("supertest");

jest.mock("@/helpers/logger", () => ({
  logger: {
    logEvent: jest.fn(),
  },
}));

const { logger } = require("@/helpers/logger");
const {
  blockSuspiciousBotRoute,
  createCorsMiddleware,
  createSecurityHeadersMiddleware,
  disablePoweredByHeader,
  enforceHttps,
} = require("@/platform/security/http-boundary.middleware");

function createApp() {
  const app = express();

  app.set("trust proxy", 1);

  return app;
}

describe("http-boundary.middleware", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("createCorsMiddleware", () => {
    it("fails loudly when allowedOrigins is missing", () => {
      expect(() => createCorsMiddleware({})).toThrow(
        "allowedOrigins is required for HTTP boundary protection.",
      );
    });

    it("allows a configured origin", async () => {
      const app = createApp();

      app.use(
        createCorsMiddleware({
          allowedOrigins: ["https://allowed.example.com"],
        }),
      );

      app.get("/test", (request, response) => {
        response.status(200).json({ status: "success" });
      });

      const response = await request(app)
        .get("/test")
        .set("Origin", "https://allowed.example.com");

      expect(response.status).toBe(200);
      expect(response.headers["access-control-allow-origin"]).toBe(
        "https://allowed.example.com",
      );
      expect(response.headers["access-control-allow-credentials"]).toBe("true");
      expect(logger.logEvent).not.toHaveBeenCalled();
    });

    it("allows requests without an Origin header", async () => {
      const app = createApp();

      app.use(
        createCorsMiddleware({
          allowedOrigins: ["https://allowed.example.com"],
        }),
      );

      app.get("/test", (request, response) => {
        response.status(200).json({ status: "success" });
      });

      const response = await request(app).get("/test");

      expect(response.status).toBe(200);
      expect(logger.logEvent).not.toHaveBeenCalled();
    });

    it("rejects and logs an unconfigured origin", async () => {
      const app = createApp();

      app.use(
        createCorsMiddleware({
          allowedOrigins: ["https://allowed.example.com"],
        }),
      );

      app.get("/test", (request, response) => {
        response.status(200).json({ status: "success" });
      });

      app.use((error, request, response, next) => {
        response.status(500).json({ message: error.message });
      });

      const response = await request(app)
        .get("/test")
        .set("Origin", "https://rejected.example.com");

      expect(response.status).toBe(500);
      expect(response.body).toEqual({
        message: "CORS: Origin not allowed",
      });
      expect(logger.logEvent).toHaveBeenCalledWith("warn", "CORS Rejected", {
        action: "CORSRejected",
        origin: "https://rejected.example.com",
      });
    });
  });

  describe("blockSuspiciousBotRoute", () => {
    it("returns 403 and records the blocked request", async () => {
      const app = createApp();

      app.use("/boaform", blockSuspiciousBotRoute);

      const response = await request(app)
        .get("/boaform/admin/formLogin")
        .set("x-forwarded-for", "203.0.113.10");

      expect(response.status).toBe(403);
      expect(response.text).toBe("Forbidden");
      expect(logger.logEvent).toHaveBeenCalledWith(
        "warn",
        "Blocked suspicious request",
        expect.objectContaining({
          action: "BotRouteBlocked",
          path: "/admin/formLogin",
          ip: "203.0.113.10",
        }),
      );
    });
  });

  describe("createSecurityHeadersMiddleware", () => {
    it("sets CSP and omits HSTS outside production", async () => {
      const app = createApp();

      app.use(
        createSecurityHeadersMiddleware({
          environment: "development",
        }),
      );

      app.get("/test", (request, response) => {
        response.status(200).send("ok");
      });

      const response = await request(app).get("/test");

      expect(response.status).toBe(200);
      expect(response.headers["content-security-policy"]).toContain(
        "default-src 'self'",
      );
      expect(response.headers["content-security-policy"]).toContain(
        "frame-ancestors 'none'",
      );
      expect(response.headers["strict-transport-security"]).toBeUndefined();
    });

    it("sets HSTS in production", async () => {
      const app = createApp();

      app.use(
        createSecurityHeadersMiddleware({
          environment: "production",
        }),
      );

      app.get("/test", (request, response) => {
        response.status(200).send("ok");
      });

      const response = await request(app).get("/test");

      expect(response.status).toBe(200);
      expect(response.headers["strict-transport-security"]).toBe(
        "max-age=63072000; includeSubDomains; preload",
      );
    });
  });

  describe("disablePoweredByHeader", () => {
    it("removes the Express powered-by header", async () => {
      const app = createApp();

      disablePoweredByHeader(app);

      app.get("/test", (request, response) => {
        response.status(200).send("ok");
      });

      const response = await request(app).get("/test");

      expect(response.status).toBe(200);
      expect(response.headers["x-powered-by"]).toBeUndefined();
    });
  });

  describe("enforceHttps", () => {
    it("allows HTTP requests in development", async () => {
      const app = createApp();

      app.use(enforceHttps({ environment: "development" }));

      app.get("/test", (request, response) => {
        response.status(200).send("ok");
      });

      const response = await request(app).get("/test");

      expect(response.status).toBe(200);
    });

    it("allows forwarded HTTPS requests outside development", async () => {
      const app = createApp();

      app.use(enforceHttps({ environment: "production" }));

      app.get("/test", (request, response) => {
        response.status(200).send("ok");
      });

      const response = await request(app)
        .get("/test")
        .set("x-forwarded-proto", "https");

      expect(response.status).toBe(200);
    });

    it("redirects non-HTTPS requests outside development", async () => {
      const app = createApp();

      app.use(enforceHttps({ environment: "production" }));

      app.get("/test", (request, response) => {
        response.status(200).send("ok");
      });

      const response = await request(app)
        .get("/test?source=security")
        .set("Host", "api.example.com");

      expect(response.status).toBe(302);
      expect(response.headers.location).toBe(
        "https://api.example.com/test?source=security",
      );
    });
  });
});

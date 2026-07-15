// Code to run the deployment zip script
// ./create-deploy-zip.sh

// If NODE_ENV is already set (like by AWS), do not overwrite it
process.env.NODE_ENV = process.env.NODE_ENV || "development";

// Load .env.development only in development; other envs use AWS-injected vars
require("module-alias/register");
const path = require("path");
const dotenv = require("dotenv");
if (process.env.NODE_ENV === "development") {
  // Load env relative to this file so cwd doesn't matter
  const envPath = path.join(__dirname, ".env.development");
  const loaded = dotenv.config({ path: envPath });
  if (loaded.error) {
    console.warn("⚠️ Could not load .env.development at", envPath);
  }
}

const { logger } = require("@/helpers/logger");

const {
  apiLimiter,
  emailLimiter,
  loginLimiter,
} = require("@/platform/security/rate-limiting.middleware");

const {
  blockSuspiciousBotRoute,
  createCorsMiddleware,
  createSecurityHeadersMiddleware,
  disablePoweredByHeader,
  enforceHttps,
} = require("@/platform/security/http-boundary.middleware");

const {
  createRequestBodyMiddleware,
  requestSizeErrorHandler,
} = require("@/platform/security/request-size.middleware");

const crypto = require("crypto");
const os = require("os");
const fs = require("fs");

// --- BEGIN: request correlation + optional request telemetry ---
const rateLimitLogDir = path.join(process.cwd(), "logs", "rate-limits");
const requestsLogFile = path.join(rateLimitLogDir, "requests.ndjson");
const REQUESTS_TEXT_LOG =
  String(process.env.REQUESTS_TEXT_LOG || "false").toLowerCase() === "true";

function safeString(value) {
  if (value == null) {
    return null;
  }

  try {
    const stringValue = String(value);

    return stringValue.length > 500
      ? `${stringValue.slice(0, 500)}…`
      : stringValue;
  } catch {
    return null;
  }
}

function writeRequestsNdjson(entry) {
  try {
    fs.mkdirSync(rateLimitLogDir, { recursive: true });
    fs.appendFileSync(requestsLogFile, JSON.stringify(entry) + os.EOL);
  } catch (error) {
    console.warn("⚠️ Failed to write requests log", error?.message);
  }
}

function buildRequestTelemetry(request, response, durationMs) {
  const origin = request.headers.origin;
  const ptrsId = request.query?.ptrsId || request.body?.ptrsId || null;
  const profileId = request.query?.profileId || request.body?.profileId || null;

  return {
    ts: new Date().toISOString(),
    requestId: request.id || null,
    method: request.method,
    path: request.originalUrl,
    ptrsId: ptrsId ? String(ptrsId) : null,
    profileId: profileId ? String(profileId) : null,
    origin: safeString(origin),
    status: response?.statusCode,
    durationMs,
  };
}
// --- END: request correlation + optional request telemetry ---

const allowedOrigins = process.env.ALLOWED_ORIGINS?.split(",") || [
  "https://monochrome-compliance.com",
  "https://www.monochrome-compliance.com",
  "http://localhost:3000",
  "https://sit.monochrome-compliance.com",
  "https://www.sit.monochrome-compliance.com",
];
console.log("🧾 Final allowed origins:", allowedOrigins);

console.log("Running in environment:", process.env.NODE_ENV);

const mask = (v) => (v ? String(v).slice(0, 10) + "…" : "<missing>");
console.log("🔑 Stripe keys:", {
  STRIPE_SECRET_KEY: mask(process.env.STRIPE_SECRET_KEY),
  STRIPE_WEBHOOK_SECRET: mask(process.env.STRIPE_WEBHOOK_SECRET),
});

if (!process.env.JWT_SECRET) {
  console.warn("⚠️ JWT_SECRET is missing. Authentication may fail.");
}
if (!process.env.DB_HOST || !process.env.DB_USER || !process.env.DB_NAME) {
  console.warn("⚠️ One or more database environment variables are missing.");
}

require("rootpath")();

// Global crash handlers for diagnostics
process.on("uncaughtException", (err) => {
  console.error("💥 Uncaught Exception:", err);
  logger.logEvent("error", "UncaughtException", {
    error: err.message,
    stack: err.stack,
  });
});

process.on("unhandledRejection", (reason) => {
  console.error("💥 Unhandled Rejection:", reason);
  logger.logEvent("error", "UnhandledRejection", {
    reason: reason instanceof Error ? reason.message : String(reason),
  });
});

const express = require("express");
const http = require("http");
const app = express();

// Correlate requests across logs
app.use((req, res, next) => {
  req.id = req.headers["x-request-id"] || crypto.randomUUID();
  res.setHeader("x-request-id", req.id);

  const start = process.hrtime.bigint();

  res.on("finish", () => {
    const durationMs = Number(process.hrtime.bigint() - start) / 1e6;

    if (REQUESTS_TEXT_LOG) {
      const requestEntry = buildRequestTelemetry(
        req,
        res,
        Math.round(durationMs),
      );

      writeRequestsNdjson(requestEntry);
    }
  });

  next();
});

app.set("trust proxy", ["loopback", "linklocal", "uniquelocal"]); // Trust local traffic only

const server = http.createServer(app);

// --- Socket.io setup for robust event-based updates ---
const { Server } = require("socket.io");
const io = new Server(server, {
  cors: {
    origin: allowedOrigins,
    credentials: true,
  },
});

io.on("connection", (socket) => {
  console.log("✅ [SOCKET] Customer connected:", socket.id);
  logger.logEvent("info", "Socket.io customer connected", {
    action: "SocketConnect",
    socketId: socket.id,
  });

  // Emit a standard structured socket message on connect
  socket.emit("message", {
    type: "file",
    stage: "connected",
    payload: { message: "Welcome to Monochrome Compliance socket updates!" },
  });

  // Allow clients to subscribe to PTRS run updates (MVP)
  // Room convention: ptrs:<ptrsId>
  socket.on("ptrs:join", (payload = {}, ack) => {
    try {
      const ptrsId = payload?.ptrsId ? String(payload.ptrsId) : "";
      if (!ptrsId) {
        if (typeof ack === "function")
          ack({ ok: false, error: "ptrsId is required" });
        return;
      }

      const room = `ptrs:${ptrsId}`;
      socket.join(room);

      logger.logEvent("info", "Socket subscribed to PTRS room", {
        action: "SocketJoinPtrs",
        socketId: socket.id,
        room,
        ptrsId,
      });

      if (typeof ack === "function") ack({ ok: true, room });
    } catch (e) {
      if (typeof ack === "function")
        ack({ ok: false, error: e?.message || "join failed" });
    }
  });

  socket.on("ptrs:leave", (payload = {}, ack) => {
    try {
      const ptrsId = payload?.ptrsId ? String(payload.ptrsId) : "";
      if (!ptrsId) {
        if (typeof ack === "function")
          ack({ ok: false, error: "ptrsId is required" });
        return;
      }

      const room = `ptrs:${ptrsId}`;
      socket.leave(room);

      logger.logEvent("info", "Socket unsubscribed from PTRS room", {
        action: "SocketLeavePtrs",
        socketId: socket.id,
        room,
        ptrsId,
      });

      if (typeof ack === "function") ack({ ok: true, room });
    } catch (e) {
      if (typeof ack === "function")
        ack({ ok: false, error: e?.message || "leave failed" });
    }
  });

  socket.on("disconnect", () => {
    console.log("⚠️ [SOCKET] Customer disconnected:", socket.id);
    logger.logEvent("info", "Socket.io customer disconnected", {
      action: "SocketDisconnect",
      socketId: socket.id,
    });
  });
});

// Also add a low-level debug for engine upgrade
io.engine.on("upgrade", (req) => {
  console.log(
    "🔥 [SOCKET] Upgrading transport to websocket for origin:",
    req.headers.origin,
  );
});

io.engine.on("connection", (rawSocket) => {
  console.log("🟢 [ENGINE] Engine.IO raw connection established", rawSocket.id);
});

// Make io accessible from controllers
app.set("socketio", io);
// Expose for service-layer emitters (MVP)
global.__socketio = io;

const cookieParser = require("cookie-parser");
const errorHandler = require("./middleware/error-handler");

// Middleware to set customerId for RLS
// const setCustomerIdRLS = require("./helpers/setCustomerIdRLS");
// const transactionCleanup = require("./middleware/transactionCleanup");

app.use(
  createCorsMiddleware({
    allowedOrigins,
  }),
);

app.use("/boaform", blockSuspiciousBotRoute);

// Health check endpoint
// This endpoint is used to check if the backend is running
// It can be used by load balancers or monitoring tools
// Has to be placed before the rate limiter to ensure it is always accessible
app.get("/api/health-check", (req, res) => {
  res.status(200).json({ status: "ok", message: "Backend is up and running." });
});

app.use("/api/", apiLimiter); // Apply to all API routes

app.use("/api/users/authenticate", loginLimiter);
app.use("/api/users/forgot-password", loginLimiter);
app.use("/api/users/reset-password", loginLimiter);
app.use("/api/booking", loginLimiter);
app.use("/api/customers/register", loginLimiter);
app.use("/api/public/send-attachment-email", emailLimiter);
app.use("/api/booking", emailLimiter);

// --- Stripe webhook MUST receive raw body BEFORE body parsers ---
const billingService = require("./stripe/billing.service");

app.post(
  "/api/billing/webhook",
  express.raw({ type: "application/json" }),
  async (req, res) => {
    try {
      await billingService.handleWebhook({
        rawBody: req.body,
        sig: req.headers["stripe-signature"],
      });
      res.status(200).send("ok");
    } catch (e) {
      res.status(e.statusCode || 400).send(`Webhook Error: ${e.message}`);
    }
  },
);
// --- end webhook mount ---

app.use(...createRequestBodyMiddleware());
app.use(requestSizeErrorHandler);
app.use(cookieParser());

// Log incoming request IPs
// app.use((req, res, next) => {
//   const ip = req.headers["x-forwarded-for"] || req.socket.remoteAddress;
//   logger.logEvent("info", "Incoming request", {
//     action: "RequestIPLog",
//     ip,
//   });
//   next();
// });

app.use(
  createSecurityHeadersMiddleware({
    environment: process.env.NODE_ENV,
  }),
);

disablePoweredByHeader(app);

app.use(
  enforceHttps({
    environment: process.env.NODE_ENV,
  }),
);

// Set RLS customerId for every request
// app.use(setCustomerIdRLS); // Converted to a helper function at the service level
// app.use(transactionCleanup);

// Add the /api prefix to all routes
app.use("/api/users", require("./users/users.controller"));
app.use("/api/customers", require("./customers/customers.controller"));
app.use("/api/ptrs", require("./ptrs/ptrs.controller"));
app.use("/api/tcp", require("./tcp/tcp.controller"));
app.use("/api/entities", require("./entities/entity.controller"));
app.use("/api/public", require("./public/public.controller"));
app.use("/api/booking", require("./booking/booking.controller"));
app.use("/api/tracking", require("./tracking/tracking.controller"));
app.use("/api/admin", require("./admin/admin.controller"));
app.use("/api/xero", require("./xero/xero.controller"));
app.use("/api/data-cleanse", require("./data_cleanse/data_cleanse.controller"));
app.use("/api/tcp/dashboard", require("./dashboard/dashboard.controller"));
app.use("/api/esg", require("./esg/esg.controller"));
app.use("/api/files", require("./files/file.controller"));
app.use("/api/ms", require("./ms/ms.controller"));
app.use("/api/partners", require("./partners/partner.controller"));
app.use("/api/invoices", require("./invoices/invoice.controller"));

app.use("/api/products", require("./products/product.controller"));

// --- Pulse (Monochrome Compliance) routes ---
app.use("/api/pulse/clients", require("./pulse/clients/client.controller"));
app.use(
  "/api/pulse/trackables",
  require("./pulse/trackables/trackable.controller"),
);
app.use(
  "/api/pulse/resources",
  require("./pulse/resources/resource.controller"),
);
app.use(
  "/api/pulse/assignments",
  require("./pulse/assignments/assignment.controller"),
);
app.use(
  "/api/pulse/contributions",
  require("./pulse/contributions/contribution.controller"),
);
// Combined controller handles /budget-items and /budgets under /api/pulse
app.use("/api/pulse", require("./pulse/budgets/budget.controller"));
app.use(
  "/api/pulse/maximiser",
  require("./pulse/maximiser/maximiser.controller"),
);
app.use(
  "/api/pulse",
  require("./pulse/pulse-dashboard/pulse_dashboard.controller"),
);

app.use("/api/stripe", require("./stripe/stripe.controller"));
app.use("/api/billing", require("./stripe/billing.controller"));

app.use("/api/big-bertha", require("./bigBertha/bigBertha.controller"));

// Platform routes
const { sequelize } = require("./db/database");
const { loadPlatformModels } = require("@/platform/platform.model_loader");
const { createDataRouter } = require("@/platform/data/data.routes");
const {
  createTransformationRouter,
} = require("@/platform/transformation/transformation.routes");
const platformModels = loadPlatformModels(sequelize);

app.use(
  "/api/platform/foundation",
  require("@/platform/foundation/foundation.routes"),
);
app.use("/api/platform/data", createDataRouter(platformModels));
app.use(
  "/api/platform/transformation",
  createTransformationRouter(platformModels),
);

// V2 routes
// PTRS
app.use("/api/v2/ptrs", require("@/v2/ptrs/routes/ptrs.routes"));

// Users
app.use("/api/v2/users", require("@/v2/users/user.routes"));

app.use("/api/v2/users/authenticate", loginLimiter);
app.use("/api/v2/users/forgot-password", loginLimiter);
app.use("/api/v2/users/reset-password", loginLimiter);

// Customers
app.use("/api/v2/customers", require("@/v2/customers/customers.routes"));
app.use(
  "/api/v2/customers",
  require("@/v2/entitlements/customerEntitlements.controller"),
);
app.use(
  "/api/v2/customers",
  require("@/v2/profiles/customerProfiles.controller"),
);

// Xero (static OAuth callback)
app.use("/api/v2/xero", require("@/v2/core/xero/xero.routes"));

// Middleware to log all registered routes
// app._router.stack.forEach((middleware) => {
//   if (middleware.route) {
//     logger.logEvent("info", "Registered route", {
//       action: "RouteRegistration",
//       path: middleware.route?.path,
//       methods: Object.keys(middleware.route?.methods || {}).join(", "),
//     });
//   } else if (middleware.name === "router") {
//     middleware.handle.stack.forEach((handler) => {
//       if (handler.route) {
//         logger.logEvent("info", "Registered route", {
//           action: "RouteRegistration",
//           path: handler.route?.path,
//           methods: Object.keys(handler.route?.methods || {}).join(", "),
//         });
//       }
//     });
//   }
// });

// --- BEGIN: Check Postgres custom GUC app.current_customer_id on startup ---

async function verifyAppCustomerIdGUC() {
  try {
    const [[{ current_customer_id }]] = await sequelize.query(
      "SELECT current_setting('app.current_customer_id', true) AS current_customer_id;",
    );
    // logger.logEvent("info", "Verified Postgres app.current_customer_id GUC", {
    //   current_customer_id,
    // });
  } catch (error) {
    logger.logEvent(
      "error",
      "Postgres custom GUC app.current_customer_id not found or not accessible",
      { error: error.message },
    );
  }
}
// Run this check on server startup
verifyAppCustomerIdGUC();
// --- END: Check Postgres custom GUC app.current_customer_id on startup ---

// Commented out noisy DB and RLS info logs
// logger.logEvent("info", "Database connection established", { action: "DatabaseInit" });
// logger.logEvent("info", "RLS policies initialised", { action: "DatabaseInit" });

// global error handler
app.use(errorHandler);

// start server unless in test mode
const port = process.env.PORT || 4000;
if ((process.env.NODE_ENV || "development") !== "test") {
  server.listen(port, "0.0.0.0", () => {
    const message = `✅ Server running in ${process.env.NODE_ENV || "development"} mode on port ${port}`;
    logger.logEvent("info", message, {
      action: "ServerStart",
      port,
      env: process.env.NODE_ENV || "development",
    });
  });
}

process.on("SIGTERM", () => {
  logger.logEvent("info", "SIGTERM received, shutting down gracefully", {
    action: "ServerShutdown",
  });
  server.close(() => {
    logger.logEvent("info", "HTTP server closed", { action: "ServerShutdown" });
    process.exit(0);
  });
});

module.exports = app;

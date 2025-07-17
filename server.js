// Code to run the deployment zip script
// ./create-deploy-zip.sh

// If NODE_ENV is already set (like by AWS), do not overwrite it
process.env.NODE_ENV = process.env.NODE_ENV || "development";

// Load .env.development only in development; other envs use AWS-injected vars
const dotenv = require("dotenv");
if (process.env.NODE_ENV === "development") {
  dotenv.config({ path: ".env.development" });
}

const { logger } = require("./helpers/logger");

const allowedOrigins = process.env.ALLOWED_ORIGINS?.split(",") || [
  "https://monochrome-compliance.com",
  "https://www.monochrome-compliance.com",
  "http://localhost:3000",
  "https://sit.monochrome-compliance.com",
  "https://www.sit.monochrome-compliance.com",
];
console.log("🧾 Final allowed origins:", allowedOrigins);

console.log("Running in environment:", process.env.NODE_ENV);

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
  console.log("✅ [SOCKET] Client connected:", socket.id);
  logger.logEvent("info", "Socket.io client connected", {
    action: "SocketConnect",
    socketId: socket.id,
  });

  // Emit a standard structured socket message on connect
  socket.emit("message", {
    type: "file",
    stage: "connected",
    payload: { message: "Welcome to Monochrome Compliance socket updates!" },
  });

  socket.on("disconnect", () => {
    console.log("⚠️ [SOCKET] Client disconnected:", socket.id);
    logger.logEvent("info", "Socket.io client disconnected", {
      action: "SocketDisconnect",
      socketId: socket.id,
    });
  });
});

// Also add a low-level debug for engine upgrade
io.engine.on("upgrade", (req) => {
  console.log(
    "🔥 [SOCKET] Upgrading transport to websocket for origin:",
    req.headers.origin
  );
});

io.engine.on("connection", (rawSocket) => {
  console.log("🟢 [ENGINE] Engine.IO raw connection established", rawSocket.id);
});

// Make io accessible from controllers
app.set("socketio", io);

const bodyParser = require("body-parser");
const cookieParser = require("cookie-parser");
const cors = require("cors");
const errorHandler = require("./middleware/error-handler");
const helmet = require("helmet");
const setCspHeaders = require("./cspHeaders");
const rateLimit = require("express-rate-limit");

// Middleware to set clientId for RLS
// const setClientIdRLS = require("./helpers/setClientIdRLS");
// const transactionCleanup = require("./middleware/transactionCleanup");

// Apply CORS middleware globally with custom origin logic
app.use(
  cors({
    origin: function (origin, callback) {
      // console.log("🔍 CORS origin received:", origin);
      // console.log("🧾 Allowed origins:", allowedOrigins);

      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
      } else {
        logger.logEvent("warn", "CORS Rejected", {
          action: "CORSRejected",
          origin,
        });
        callback(new Error("CORS: Origin not allowed"));
      }
    },
    credentials: true,
  })
);

// Immediately reject suspicious bot routes such as /boaform/admin/formLogin
app.use("/boaform", (req, res) => {
  logger.logEvent("warn", "Blocked suspicious request", {
    action: "BotRouteBlocked",
    path: req.path,
    ip: req.headers["x-forwarded-for"] || req.socket.remoteAddress,
  });
  res.status(403).send("Forbidden");
});

// Health check endpoint
// This endpoint is used to check if the backend is running
// It can be used by load balancers or monitoring tools
// Has to be placed before the rate limiter to ensure it is always accessible
app.get("/api/health-check", (req, res) => {
  res.status(200).json({ status: "ok", message: "Backend is up and running." });
});

const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // limit each IP to 100 requests per windowMs
  standardHeaders: true,
  legacyHeaders: false,
});

app.use("/api/", apiLimiter); // Apply to all API routes

const loginLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 5,
  message: "Too many login attempts from this IP, please try again later.",
});

app.use("/api/users/authenticate", loginLimiter);
app.use("/api/users/forgot-password", loginLimiter);
app.use("/api/users/reset-password", loginLimiter);
app.use("/api/booking", loginLimiter);
app.use("/api/clients/register", loginLimiter);

const emailLimiter = rateLimit({
  windowMs: 10 * 60 * 1000, // 10 minutes
  max: 5,
  message: "Too many attempts, please try again later.",
});

app.use("/api/public/send-attachment-email", emailLimiter);
app.use("/api/booking", emailLimiter);

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use(cookieParser());

app.use(helmet());
if (process.env.NODE_ENV === "production") {
  app.use(
    helmet.hsts({
      maxAge: 63072000, // 2 years
      includeSubDomains: true,
      preload: true,
    })
  );
}

// Log incoming request IPs
// app.use((req, res, next) => {
//   const ip = req.headers["x-forwarded-for"] || req.socket.remoteAddress;
//   logger.logEvent("info", "Incoming request", {
//     action: "RequestIPLog",
//     ip,
//   });
//   next();
// });

app.use(setCspHeaders);

app.disable("x-powered-by");

// Enforce HTTPS in production
app.use((req, res, next) => {
  if (
    process.env.NODE_ENV !== "development" &&
    req.headers["x-forwarded-proto"] !== "https"
  ) {
    return res.redirect("https://" + req.headers.host + req.url);
  }
  next();
});

// Set RLS clientId for every request
// app.use(setClientIdRLS); // Converted to a helper function at the service level
// app.use(transactionCleanup);

// Add the /api prefix to all routes
app.use("/api/users", require("./users/users.controller"));
app.use("/api/clients", require("./clients/clients.controller"));
app.use("/api/reports", require("./reports/reports.controller"));
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

// --- BEGIN: Check Postgres custom GUC app.current_client_id on startup ---
const { sequelize } = require("./db/database");
// console.log("sequelize:", sequelize);

async function verifyAppClientIdGUC() {
  try {
    const [[{ current_client_id }]] = await sequelize.query(
      "SELECT current_setting('app.current_client_id', true) AS current_client_id;"
    );
    // logger.logEvent("info", "Verified Postgres app.current_client_id GUC", {
    //   current_client_id,
    // });
  } catch (error) {
    logger.logEvent(
      "error",
      "Postgres custom GUC app.current_client_id not found or not accessible",
      { error: error.message }
    );
  }
}
// Run this check on server startup
verifyAppClientIdGUC();
// --- END: Check Postgres custom GUC app.current_client_id on startup ---

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

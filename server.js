process.env.NODE_ENV = process.env.NODE_ENV || "development";
if (process.env.NODE_ENV === "development") {
  require("dotenv").config({ path: ".env.development" });
}
require("rootpath")();
const express = require("express");
const bodyParser = require("body-parser");
const cookieParser = require("cookie-parser");
const cors = require("cors");
const helmet = require("helmet");
const errorHandler = require("./middleware/error-handler");
const upload = require("./middleware/upload");
const { logger } = require("./helpers/logger");

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

const app = express();
app.disable("x-powered-by");

const allowedOrigins = [
  "https://sit.monochrome-compliance.com",
  "https://wwww.sit.monochrome-compliance.com",
  "https://www.monochrome-compliance.com",
  "https://monochrome-compliance.com",
  "http://localhost:3000",
];

app.use(
  cors({
    origin: function (origin, callback) {
      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
      } else {
        callback(new Error("CORS not allowed for this origin"));
      }
    },
    credentials: true,
  })
);
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use(cookieParser());
app.use(helmet());

app.get("/api/health-check", (req, res) => {
  res.status(200).json({ status: "ok" });
});
app.use("/api/public", require("./public/public.controller"));

const {
  handleUnprocessedSubmission,
} = require("./controllers/unprocessedSubmission.controller");

// Logging middleware for /api/unprocessed-submission
app.use("/api/unprocessed-submission", (req, res, next) => {
  console.log("🛰 Incoming request to /api/unprocessed-submission");
  console.log("Headers:", req.headers["content-type"]);
  next();
});

app.post(
  "/api/unprocessed-submission",
  upload.single("file"),
  (req, res, next) => {
    console.log("📦 Received in route: file =", req.file);
    console.log("📨 Received in route: body =", req.body);
    handleUnprocessedSubmission(req, res, next);
  }
);

app.use(errorHandler);

const port = process.env.PORT || 4000;
app.listen(port, "0.0.0.0", () => {
  logger.logEvent("info", "ServerStart", {
    action: "ServerStart",
    port,
    env: process.env.NODE_ENV,
  });
});

module.exports = app;

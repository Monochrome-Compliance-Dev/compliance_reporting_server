const { logger } = require("../helpers/logger");

module.exports = errorHandler;

function errorHandler(err, req, res, next) {
  // Prefer explicit status values set upstream (controller/service), otherwise infer.
  let statusCode = Number(err?.statusCode || err?.status) || null;

  // Strings are sometimes thrown directly.
  if (typeof err === "string") {
    const is404 = err.toLowerCase().endsWith("not found");
    statusCode = is404 ? 404 : 400;
    err = new Error(err);
  }

  // Auth middleware errors
  if (!statusCode && err?.name === "UnauthorizedError") {
    statusCode = 401;
  }

  // Basic inference if still missing
  if (!statusCode) {
    const msg = (err?.message || "").toLowerCase();
    if (msg.endsWith("not found") || msg.includes("not found"))
      statusCode = 404;
    else statusCode = 500;
  }

  // Guard against invalid codes
  if (!Number.isFinite(statusCode) || statusCode < 100 || statusCode > 599) {
    statusCode = 500;
  }

  // Ensure consistent fields for anything else that inspects the error
  if (err && typeof err === "object") {
    err.status = statusCode;
    err.statusCode = statusCode;
  }

  logger.logEvent("error", "Unhandled error", {
    action: "UnhandledException",
    path: req.originalUrl,
    method: req.method,
    ip: req.ip,
    customerId: req.effectiveCustomerId || req.auth?.customerId,
    userId: req.auth?.id,
    statusCode,
    error: err?.message,
    stack: err?.stack,
  });

  const message =
    process.env.NODE_ENV === "production"
      ? "An unexpected error occurred. Please try again later."
      : err?.message;

  return res.status(statusCode).json({ message });
}

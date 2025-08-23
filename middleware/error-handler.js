const { logger } = require("../helpers/logger");

module.exports = errorHandler;

function errorHandler(err, req, res, next) {
  let statusCode;
  switch (true) {
    case typeof err === "string":
      const is404 = err.toLowerCase().endsWith("not found");
      statusCode = is404 ? 404 : 400;
      break;
    case err.name === "UnauthorizedError":
      statusCode = 401;
      break;
    default:
      statusCode = 500;
  }

  logger.logEvent("error", "Unhandled error", {
    action: "UnhandledException",
    path: req.originalUrl,
    method: req.method,
    ip: req.ip,
    customerId: req.auth?.customerId,
    userId: req.auth?.id,
    statusCode,
    error: err.message,
    stack: err.stack,
  });

  const message =
    process.env.NODE_ENV === "production"
      ? "An unexpected error occurred. Please try again later."
      : err.message;

  return res.status(statusCode).json({ message });
}

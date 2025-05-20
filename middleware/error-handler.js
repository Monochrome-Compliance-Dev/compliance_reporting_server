const { logger } = require("../helpers/logger");

module.exports = errorHandler;

function errorHandler(err, req, res, next) {
  logger.logEvent("error", "Unhandled error", {
    action: "UnhandledException",
    path: req.originalUrl,
    method: req.method,
    ip: req.ip,
    error: err.message,
    stack: err.stack,
  });

  switch (true) {
    case typeof err === "string":
      // custom application error
      const is404 = err.toLowerCase().endsWith("not found");
      const statusCode = is404 ? 404 : 400;
      return res.status(statusCode).json({ message: err });
    case err.name === "UnauthorizedError":
      // jwt authentication error
      return res.status(401).json({ message: "Unauthorized" });
    default:
      const message =
        process.env.NODE_ENV === "production"
          ? "An unexpected error occurred. Please try again later."
          : err.message;
      return res.status(500).json({ message });
  }
}

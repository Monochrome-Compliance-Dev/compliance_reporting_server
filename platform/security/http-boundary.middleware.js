const cors = require("cors");
const helmet = require("helmet");
const { logger } = require("../../helpers/logger");

function createCorsMiddleware({ allowedOrigins }) {
  if (!Array.isArray(allowedOrigins) || allowedOrigins.length === 0) {
    throw new Error("allowedOrigins is required for HTTP boundary protection.");
  }

  return cors({
    origin(origin, callback) {
      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
        return;
      }

      logger.logEvent("warn", "CORS Rejected", {
        action: "CORSRejected",
        origin,
      });

      callback(new Error("CORS: Origin not allowed"));
    },
    credentials: true,
  });
}

function blockSuspiciousBotRoute(request, response) {
  logger.logEvent("warn", "Blocked suspicious request", {
    action: "BotRouteBlocked",
    path: request.path,
    ip:
      request.headers["x-forwarded-for"] ||
      request.socket?.remoteAddress ||
      null,
  });

  response.status(403).send("Forbidden");
}

function createSecurityHeadersMiddleware({ environment }) {
  return helmet({
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'"],
        styleSrc: ["'self'", "'unsafe-inline'"],
        fontSrc: ["'self'", "data:"],
        imgSrc: ["'self'", "data:"],
        objectSrc: ["'none'"],
        frameAncestors: ["'none'"],
        baseUri: ["'self'"],
      },
    },
    hsts:
      environment === "production"
        ? {
            maxAge: 63072000,
            includeSubDomains: true,
            preload: true,
          }
        : false,
  });
}

function disablePoweredByHeader(app) {
  app.disable("x-powered-by");
}

function enforceHttps({ environment }) {
  return (request, response, next) => {
    if (
      environment !== "development" &&
      request.headers["x-forwarded-proto"] !== "https"
    ) {
      response.redirect(`https://${request.headers.host}${request.url}`);
      return;
    }

    next();
  };
}

module.exports = {
  blockSuspiciousBotRoute,
  createCorsMiddleware,
  createSecurityHeadersMiddleware,
  disablePoweredByHeader,
  enforceHttps,
};

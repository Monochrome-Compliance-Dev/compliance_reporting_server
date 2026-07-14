const fs = require("fs");
const os = require("os");
const path = require("path");
const rateLimit = require("express-rate-limit");
const { logger } = require("@/helpers/logger");

const rateLimitLogDir = path.join(process.cwd(), "logs", "rate-limits");
const rateLimitLogFile = path.join(rateLimitLogDir, "rate-limit.ndjson");

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

function getClientIp(request) {
  const forwardedFor = request.headers["x-forwarded-for"];

  if (forwardedFor) {
    const firstAddress = String(forwardedFor).split(",")[0].trim();

    if (firstAddress) {
      return firstAddress;
    }
  }

  return (
    request.socket?.remoteAddress || request.connection?.remoteAddress || null
  );
}

function writeRateLimitNdjson(entry) {
  try {
    fs.mkdirSync(rateLimitLogDir, { recursive: true });
    fs.appendFileSync(rateLimitLogFile, JSON.stringify(entry) + os.EOL);
  } catch (error) {
    console.warn("⚠️ Failed to write rate-limit log", error?.message);
  }
}

function buildRateLimitContext(request, response, extra = {}) {
  const ptrsId = request.query?.ptrsId || request.body?.ptrsId || null;
  const profileId = request.query?.profileId || request.body?.profileId || null;

  return {
    ts: new Date().toISOString(),
    requestId: request.id || null,
    status: response?.statusCode,
    method: request.method,
    path: request.originalUrl,
    ip: getClientIp(request),
    origin: safeString(request.headers.origin),
    referer: safeString(request.headers.referer),
    userAgent: safeString(request.headers["user-agent"]),
    hasAuthHeader: Boolean(request.headers.authorization),
    hasCookie: Boolean(request.headers.cookie),
    ptrsId: ptrsId ? String(ptrsId) : null,
    profileId: profileId ? String(profileId) : null,
    ...extra,
  };
}

function logRateLimit(request, response, extra) {
  const entry = buildRateLimitContext(request, response, extra);

  writeRateLimitNdjson(entry);
  logger.logEvent("warn", "RateLimit429", entry);
}

function createRateLimitHandler(limiterName) {
  return (request, response, next, options) => {
    logRateLimit(request, response, {
      type: "rate_limit",
      stage: "limiter_handler",
      limiter: limiterName,
      windowMs: options?.windowMs,
      max: options?.max,
    });

    response.status(options.statusCode || 429).json({
      status: "error",
      code: "RATE_LIMITED",
      message: options.message || "Too many requests, please try again later.",
      requestId: request.id || null,
    });
  };
}

const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 1000,
  standardHeaders: true,
  legacyHeaders: false,
  handler: createRateLimitHandler("api"),
});

const loginLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 5,
  message: "Too many login attempts from this IP, please try again later.",
  handler: createRateLimitHandler("login"),
});

const emailLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 5,
  message: "Too many attempts, please try again later.",
  handler: createRateLimitHandler("email"),
});

module.exports = {
  apiLimiter,
  emailLimiter,
  loginLimiter,
};

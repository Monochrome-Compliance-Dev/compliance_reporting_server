const fs = require("fs");
const path = require("path");
const winston = require("winston");
winston.addColors({ audit: "cyan" });

const customLevels = {
  error: 0,
  warn: 1,
  info: 2,
  debug: 3,
  audit: 4,
};
require("winston-daily-rotate-file");

// Ensure the logs directory exists
const logDir = path.join(__dirname, "..", "logs");
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}

const logger = winston.createLogger({
  levels: customLevels,
  level: process.env.LOG_LEVEL || "debug",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.printf(({ timestamp, level, message, ...meta }) => {
          return `${timestamp} [${level}]: ${message} ${Object.keys(meta).length ? JSON.stringify(meta) : ""}`;
        })
      ),
    }),
    new winston.transports.DailyRotateFile({
      filename: path.join(logDir, "info-%DATE%.log"),
      datePattern: "YYYY-MM-DD",
      level: "info",
      zippedArchive: true,
      maxSize: "10m",
      maxFiles: "14d",
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.json()
      ),
    }),
    new winston.transports.DailyRotateFile({
      filename: path.join(logDir, "warn-%DATE%.log"),
      datePattern: "YYYY-MM-DD",
      level: "warn",
      zippedArchive: true,
      maxSize: "10m",
      maxFiles: "14d",
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.json()
      ),
    }),
    new winston.transports.DailyRotateFile({
      filename: path.join(logDir, "error-%DATE%.log"),
      datePattern: "YYYY-MM-DD",
      level: "error",
      zippedArchive: true,
      maxSize: "10m",
      maxFiles: "14d",
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.json()
      ),
    }),
  ],
});

const auditLogger = winston.createLogger({
  levels: customLevels,
  level: "audit",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.printf(({ timestamp, level, message, ...meta }) => {
          return `${timestamp} [${level}]: ${message} ${Object.keys(meta).length ? JSON.stringify(meta) : ""}`;
        })
      ),
    }),
    new winston.transports.DailyRotateFile({
      filename: path.join(logDir, "audit-%DATE%.log"),
      datePattern: "YYYY-MM-DD",
      zippedArchive: true,
      maxSize: "10m",
      maxFiles: "30d",
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.json()
      ),
    }),
  ],
});

logger.audit = (message) => {
  auditLogger.log({ level: "info", message });
};

logger.logEvent = (level, message, meta = {}) => {
  logger.log({ level, message, meta });
};

logger.auditEvent = (message, meta = {}) => {
  auditLogger.log({ level: "audit", message, meta });
};

module.exports = { logger, auditLogger };

const winston = require("winston");

const logger = winston.createLogger({
  level: "info", // Set the default logging level
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      return `${timestamp} [${level.toUpperCase()}]: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(), // Log to the console
    new winston.transports.File({ filename: "logs/app.log" }), // Log to a file
  ],
});

module.exports = logger;

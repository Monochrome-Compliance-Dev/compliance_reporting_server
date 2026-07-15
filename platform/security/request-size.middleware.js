const express = require("express");
const { logger } = require("@/helpers/logger");

const DEFAULT_JSON_BODY_LIMIT = "100kb";
const DEFAULT_URLENCODED_BODY_LIMIT = "100kb";
const DEFAULT_URLENCODED_PARAMETER_LIMIT = 1000;

function getPositiveInteger(value, fallback) {
  if (value == null || value === "") {
    return fallback;
  }

  const parsedValue = Number.parseInt(value, 10);

  if (!Number.isInteger(parsedValue) || parsedValue <= 0) {
    throw new Error("Request size parameter limits must be positive integers.");
  }

  return parsedValue;
}

function createRequestBodyMiddleware({
  jsonBodyLimit = process.env.JSON_BODY_LIMIT || DEFAULT_JSON_BODY_LIMIT,
  urlencodedBodyLimit = process.env.URLENCODED_BODY_LIMIT ||
    DEFAULT_URLENCODED_BODY_LIMIT,
  urlencodedParameterLimit = getPositiveInteger(
    process.env.URLENCODED_PARAMETER_LIMIT,
    DEFAULT_URLENCODED_PARAMETER_LIMIT,
  ),
} = {}) {
  if (!jsonBodyLimit) {
    throw new Error("jsonBodyLimit is required for request size protection.");
  }

  if (!urlencodedBodyLimit) {
    throw new Error(
      "urlencodedBodyLimit is required for request size protection.",
    );
  }

  const validatedUrlencodedParameterLimit = getPositiveInteger(
    urlencodedParameterLimit,
    DEFAULT_URLENCODED_PARAMETER_LIMIT,
  );

  return [
    express.urlencoded({
      extended: false,
      limit: urlencodedBodyLimit,
      parameterLimit: validatedUrlencodedParameterLimit,
    }),
    express.json({
      limit: jsonBodyLimit,
    }),
  ];
}

function requestSizeErrorHandler(error, request, response, next) {
  if (error?.status !== 413 && error?.statusCode !== 413) {
    next(error);
    return;
  }

  logger.logEvent("warn", "Request payload rejected", {
    action: "RequestPayloadRejected",
    requestId: request.id || null,
    method: request.method,
    path: request.originalUrl,
    contentType: request.headers["content-type"] || null,
    contentLength: request.headers["content-length"] || null,
    limit: error.limit || null,
    length: error.length || null,
    reason: error.type || "payload_too_large",
  });

  response.status(413).json({
    status: "error",
    code: "PAYLOAD_TOO_LARGE",
    message: "Request payload exceeds the permitted size.",
    requestId: request.id || null,
  });
}

module.exports = {
  createRequestBodyMiddleware,
  requestSizeErrorHandler,
};

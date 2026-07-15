function sanitiseValidationError(error) {
  if (!error || !Array.isArray(error.details)) {
    throw new Error("Joi validation error details are required.");
  }

  return error.details.map((detail) => ({
    path: Array.isArray(detail.path) ? detail.path.join(".") : "",
    message: detail.message || "Invalid value.",
    type: detail.type || "validation.error",
  }));
}

module.exports = {
  sanitiseValidationError,
};

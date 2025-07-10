const { logger } = require("../helpers/logger");
module.exports = function validateRequest(schema) {
  return function (req, res, next) {
    const clientId = req.auth?.clientId || req.body?.clientId;
    if (!clientId) {
      logger.logEvent("error", "Client ID missing in auth context", {
        action: "ValidateRequest",
        path: req.originalUrl,
      });
      return res
        .status(400)
        .json({ message: "Client ID missing from authentication context." });
    }

    const options = {
      abortEarly: false,
      allowUnknown: true,
      stripUnknown: true,
    };

    const isArray = Array.isArray(req.body);
    const records = isArray ? req.body : [req.body];
    const results = [];
    const errors = [];

    records.forEach((record, index) => {
      const fullRecord = { ...record, clientId };
      const { error, value } = schema.validate(fullRecord, options);
      if (error) {
        const details = error.details.map((x) => {
          const path = x.path.join(".");
          const val = x.context?.value;
          const type = typeof val;
          return `[${path}] ${x.message} | Value: "${val}" (${type})`;
        });
        errors.push({ index, errors: details });
      } else {
        results.push(value);
      }
    });

    if (errors.length > 0) {
      logger.logEvent("warn", "Validation failed", {
        action: "ValidateRequest",
        path: req.originalUrl,
        clientId,
        errors,
      });
      return res
        .status(400)
        .json({ message: "Validation failed for some records", errors });
    }

    req.body = isArray ? results : results[0];
    next();
  };
};

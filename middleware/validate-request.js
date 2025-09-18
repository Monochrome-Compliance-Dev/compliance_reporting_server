const { logger } = require("../helpers/logger");

/**
 * validateRequest(schema, location?)
 *
 * @param {Joi.Schema} schema - Joi schema to validate with
 * @param {('body'|'query'|'params')} [location='body'] - where to read/write the payload
 *
 * Notes:
 * - Defaults to 'body' (backwards compatible)
 * - For 'query' and 'params' we always validate a single record
 */
module.exports = function validateRequest(schema, location = "body") {
  const loc = ["body", "query", "params"].includes(location)
    ? location
    : "body";

  return function (req, res, next) {
    // Determine if this route requires a customerId based on Joi schema meta
    const metas = (schema && schema.$_terms && schema.$_terms.metas) || [];
    const metaFlag = metas.find((m) =>
      Object.prototype.hasOwnProperty.call(m, "requireCustomer")
    );
    const requireCustomer = metaFlag ? !!metaFlag.requireCustomer : true; // default to true

    const customerId = req.auth?.customerId || req.body?.customerId;

    if (requireCustomer && !customerId) {
      logger.logEvent("error", "Customer ID missing in auth context", {
        action: "ValidateRequest",
        path: req.originalUrl,
        location: loc,
      });
      return res
        .status(400)
        .json({ message: "Customer ID missing from authentication context." });
    }

    const options = {
      abortEarly: false,
      allowUnknown: true,
      stripUnknown: true,
    };

    // Select source payload by location
    const source =
      loc === "body" ? req.body : loc === "query" ? req.query : req.params;

    // For body we support array payloads; for query/params always single record
    const isArray = loc === "body" && Array.isArray(source);
    const records = isArray ? source : [source];

    const results = [];
    const errors = [];

    records.forEach((record, index) => {
      const fullRecord = requireCustomer
        ? { ...(record || {}), customerId }
        : { ...(record || {}) };

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
        location: loc,
        ...(customerId ? { customerId } : {}),
        errors,
      });
      return res
        .status(400)
        .json({ message: "Validation failed for some records", errors });
    }

    // Assign validated payload back to the same location
    if (loc === "body") {
      req.body = isArray ? results : results[0];
    } else if (loc === "query") {
      req.query = results[0];
    } else {
      req.params = results[0];
    }

    next();
  };
};

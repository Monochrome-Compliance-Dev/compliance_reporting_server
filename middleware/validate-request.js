const { logger } = require("../helpers/logger");
module.exports = function validateRequest(schema) {
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

    const isArray = Array.isArray(req.body);
    const records = isArray ? req.body : [req.body];
    const results = [];
    const errors = [];

    records.forEach((record, index) => {
      const fullRecord = requireCustomer
        ? { ...record, customerId }
        : { ...record };
      // console.log("fullRecord: ", fullRecord);
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
        ...(customerId ? { customerId } : {}),
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

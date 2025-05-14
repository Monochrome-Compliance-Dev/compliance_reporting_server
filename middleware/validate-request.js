module.exports = function validateRequest(schema) {
  return function (req, res, next) {
    const options = {
      abortEarly: false,
      allowUnknown: true,
      stripUnknown: true,
    };

    if (!Array.isArray(req.body)) {
      return res.status(400).json({ error: "Expected an array of records" });
    }

    const { error, value } = schema.validate(req.body, options);
    if (error) {
      next(
        `Validation error: ${error.details.map((x) => `[${x.path}] ${x.message}`).join(", ")}`
      );
    } else {
      req.body = value;
      next();
    }
  };
};

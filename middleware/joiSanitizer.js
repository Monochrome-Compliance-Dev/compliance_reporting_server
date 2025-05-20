const JoiBase = require("joi");
const { escape, trim, stripLow } = require("validator");
const { logger } = require("../helpers/logger");

const Joi = JoiBase.extend((joi) => ({
  type: "string",
  base: joi.string(),
  messages: {
    "string.sanitize": "{{#label}} contains unsafe characters",
  },
  rules: {
    sanitize: {
      method() {
        return this.$_addRule("sanitize");
      },
      validate(value, helpers) {
        if (typeof value !== "string") return value;

        const cleaned = escape(trim(stripLow(value)));
        logger.logEvent("info", "Sanitizing string input", {
          action: "SanitizeInput",
          original: value,
          sanitized: cleaned,
        });
        return cleaned;
      },
    },
  },
}));

module.exports = Joi;

const JoiBase = require("joi");
const { escape, trim, stripLow } = require("validator");

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
        return cleaned;
      },
    },
  },
}));

module.exports = Joi;

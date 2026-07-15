const {
  sanitiseValidationError,
} = require("@/platform/security/validation-error-sanitiser");

describe("validation-error-sanitiser", () => {
  describe("sanitiseValidationError", () => {
    it("fails loudly when Joi error details are missing", () => {
      expect(() => sanitiseValidationError()).toThrow(
        "Joi validation error details are required.",
      );

      expect(() => sanitiseValidationError({})).toThrow(
        "Joi validation error details are required.",
      );
    });

    it("returns safe structured validation details", () => {
      const result = sanitiseValidationError({
        details: [
          {
            path: ["email"],
            message: '"email" must be a valid email',
            type: "string.email",
            context: {
              value: "not-an-email",
              key: "email",
              label: "email",
            },
          },
        ],
      });

      expect(result).toEqual([
        {
          path: "email",
          message: '"email" must be a valid email',
          type: "string.email",
        },
      ]);
    });

    it("does not expose submitted values or other Joi context", () => {
      const password = "SuperSecretPassword123!";
      const token = "sensitive-reset-token";

      const result = sanitiseValidationError({
        details: [
          {
            path: ["password"],
            message: '"password" length must be at least 12 characters long',
            type: "string.min",
            context: {
              value: password,
              limit: 12,
              key: "password",
              label: "password",
              token,
            },
          },
        ],
      });

      const serialisedResult = JSON.stringify(result);

      expect(serialisedResult).not.toContain(password);
      expect(serialisedResult).not.toContain(token);
      expect(serialisedResult).not.toContain("context");
      expect(serialisedResult).not.toContain("value");
      expect(serialisedResult).not.toContain("limit");
    });

    it("joins nested Joi paths using dot notation", () => {
      const result = sanitiseValidationError({
        details: [
          {
            path: ["contacts", 0, "email"],
            message: '"contacts[0].email" must be a valid email',
            type: "string.email",
            context: {
              value: "invalid-email",
            },
          },
        ],
      });

      expect(result).toEqual([
        {
          path: "contacts.0.email",
          message: '"contacts[0].email" must be a valid email',
          type: "string.email",
        },
      ]);
    });

    it("sanitises multiple validation errors in their original order", () => {
      const result = sanitiseValidationError({
        details: [
          {
            path: ["email"],
            message: '"email" must be a valid email',
            type: "string.email",
            context: {
              value: "invalid-email",
            },
          },
          {
            path: ["name"],
            message: '"name" is required',
            type: "any.required",
            context: {
              value: undefined,
            },
          },
        ],
      });

      expect(result).toEqual([
        {
          path: "email",
          message: '"email" must be a valid email',
          type: "string.email",
        },
        {
          path: "name",
          message: '"name" is required',
          type: "any.required",
        },
      ]);
    });

    it("uses safe defaults for malformed validation detail fields", () => {
      const result = sanitiseValidationError({
        details: [
          {
            context: {
              value: "sensitive-value",
            },
          },
        ],
      });

      expect(result).toEqual([
        {
          path: "",
          message: "Invalid value.",
          type: "validation.error",
        },
      ]);

      expect(JSON.stringify(result)).not.toContain("sensitive-value");
    });
  });
});

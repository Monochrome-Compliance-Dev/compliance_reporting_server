const {
  normaliseCanonicalNumber,
} = require("./canonical.scalar-normalisation.ptrs.service");

describe("canonical scalar normalisation", () => {
  test.each([
    ["343.20", "343.20"],
    ["1,155.00", "1155.00"],
    ["(27,844.63)", "-27844.63"],
    ["-572.14", "-572.14"],
    ["  343.20  ", "343.20"],
  ])("normalises accounting number %s", (source, expected) => {
    expect(normaliseCanonicalNumber(source)).toEqual({
      value: expected,
      error: null,
    });
  });

  test.each(["1.234,56", "1,23", "(27,84.63)", "amount unknown"])(
    "rejects unsupported or malformed numeric value %s",
    (source) => {
      expect(normaliseCanonicalNumber(source)).toEqual({
        value: source,
        error: "UNSUPPORTED_NUMERIC_FORMAT",
      });
    },
  );
});

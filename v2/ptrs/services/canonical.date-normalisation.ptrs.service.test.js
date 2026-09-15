const {
  normaliseCanonicalDate,
  normaliseDatasetDateFormat,
} = require("./canonical.date-normalisation.ptrs.service");

describe("direct-payment canonical date normalisation", () => {
  test.each([
    ["2/26/2026", "MDY", "2026-02-26"],
    ["12/29/2025", "MDY", "2025-12-29"],
    ["12/02/2025", "MDY", "2025-12-02"],
    ["03/02/2026", "MDY", "2026-03-02"],
  ])("normalises Orontide MDY date %s", (source, format, expected) => {
    expect(normaliseCanonicalDate(source, format)).toEqual({
      value: expected,
      error: null,
    });
  });

  test.each([
    ["2026-01-21", "DMY", "2026-01-21"],
    ["2026-02-28", "DMY", "2026-02-28"],
    ["2026-02-26", "DMY", "2026-02-26"],
    ["21/01/2026", "DMY", "2026-01-21"],
    ["30/01/2026", "DMY", "2026-01-30"],
  ])("normalises EnviroPacific DMY date %s", (source, format, expected) => {
    expect(normaliseCanonicalDate(source, format)).toEqual({
      value: expected,
      error: null,
    });
  });

  test.each(["MDY", "DMY"])(
    "keeps ISO dates unchanged when the dataset convention is %s",
    (format) => {
      expect(normaliseCanonicalDate("2026-02-26", format)).toEqual({
        value: "2026-02-26",
        error: null,
      });
    },
  );

  test("interprets an ambiguous slash date only by the explicit convention", () => {
    expect(normaliseCanonicalDate("12/02/2025", "MDY").value).toBe(
      "2025-12-02",
    );
    expect(normaliseCanonicalDate("12/02/2025", "DMY").value).toBe(
      "2025-02-12",
    );
  });

  test("requires governed dataset format for slash dates", () => {
    expect(normaliseCanonicalDate("12/02/2025", null)).toEqual({
      value: "12/02/2025",
      error: "DATASET_DATE_FORMAT_REQUIRED",
    });
  });

  test("rejects slash dates when the governed format is ISO", () => {
    expect(normaliseCanonicalDate("12/02/2025", "ISO")).toEqual({
      value: "12/02/2025",
      error: "DATASET_DATE_FORMAT_MISMATCH",
    });
  });

  test("does not roll invalid calendar dates", () => {
    expect(normaliseCanonicalDate("2/30/2026", "MDY")).toEqual({
      value: "2/30/2026",
      error: "INVALID_DATE",
    });
  });

  test("accepts only the governed formats", () => {
    expect(normaliseDatasetDateFormat("mdy")).toBe("MDY");
    expect(normaliseDatasetDateFormat("guess")).toBeNull();
  });
});

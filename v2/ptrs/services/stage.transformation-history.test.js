const {
  appendTransformationHistory,
} = require("./stage.transformation-history");

describe("Stage transformation history", () => {
  test("preserves multiple actions and ignores a duplicate deterministic key", () => {
    const first = appendTransformationHistory(null, {
      key: "exclusion:GOV_ENTITY",
      comment: "Excluded because government entity",
    });
    const second = appendTransformationHistory(first, {
      key: "sbi:upload01:true",
      comment: "SBI status resolved true from SBI upload upload01",
    });
    const rerun = appendTransformationHistory(second, {
      key: "sbi:upload01:true",
      comment: "SBI status resolved true from SBI upload upload01",
    });

    expect(second.transformationHistory).toHaveLength(2);
    expect(rerun).toBe(second);
  });
});

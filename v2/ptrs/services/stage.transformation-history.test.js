const {
  appendTransformationHistory,
  appendTransformationHistoryEventsSql,
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

  test("builds a set-based append expression for multiple events", () => {
    const sql = appendTransformationHistoryEventsSql(
      'stage_row."meta"',
      "grouped.events",
    );

    expect(sql).toContain("jsonb_array_elements");
    expect(sql).toContain("WITH ORDINALITY");
    expect(sql).toContain("SELECT DISTINCT ON (candidate.event->>'key')");
    expect(sql).toContain("history_item->>'key' = candidate.event->>'key'");
  });
});

const { applyGovExclusion, previewGovExclusion } = require("./exclusions.gov");

describe("PTRS government exclusion SQL", () => {
  test("preview uses exact normalised local ABN matching without stage mutation", async () => {
    const sequelize = {
      query: jest
        .fn()
        .mockResolvedValueOnce([[{ matchedCount: 1, alreadyExcludedCount: 0 }]])
        .mockResolvedValueOnce([[{ row_no: 1 }]]),
    };

    await previewGovExclusion({
      sequelize,
      transaction: { id: "preview-tx" },
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      effectiveLimit: 10,
    });

    const sql = sequelize.query.mock.calls.map(([query]) => query).join("\n");
    expect(sql).toContain('JOIN "tbl_ptrs_gov_entity_ref" g');
    expect(sql).toContain("regexp_replace");
    expect(sql).toContain("= regexp_replace");
    expect(sql).not.toMatch(/\bUPDATE\s+"tbl_ptrs_stage_row"/i);
  });

  test("apply uses exact normalised local ABN matching", async () => {
    const sequelize = {
      query: jest.fn().mockResolvedValue([[], { rowCount: 2 }]),
    };

    const affected = await applyGovExclusion({
      sequelize,
      transaction: { id: "apply-tx" },
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    const sql = sequelize.query.mock.calls[0][0];
    expect(sql).toContain('FROM "tbl_ptrs_gov_entity_ref" g');
    expect(sql).toContain("regexp_replace");
    expect(sql).toContain("= NULLIF(regexp_replace");
    expect(affected).toBe(2);
  });
});

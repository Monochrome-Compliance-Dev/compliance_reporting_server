const {
  applyCreditAppliedExclusion,
  previewCreditAppliedExclusion,
} = require("./exclusions.creditApplied");

describe("PTRS credit-applied clearing-group exclusion", () => {
  test("deduplicates 200-series groups and isolates a shared clearing document by company and account", async () => {
    const sequelize = {
      query: jest.fn().mockResolvedValue([[], { rowCount: 4 }]),
    };

    const affected = await applyCreditAppliedExclusion({
      sequelize,
      transaction: { id: "apply-tx" },
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    const sql = sequelize.query.mock.calls[0][0];
    expect(sql).toContain("SELECT DISTINCT");
    expect(sql).toContain("LIKE '200%'");
    expect(sql).toContain(
      'matched_group."companyCode" = COALESCE(NULLIF(BTRIM(COALESCE(s."data"->>\'company_code\'',
    );
    expect(sql).toContain(
      'matched_group."account" = COALESCE(NULLIF(BTRIM(COALESCE(s."sourceAccountCode"',
    );
    expect(sql).toContain(
      'matched_group."clearingDocument" = COALESCE(NULLIF(BTRIM(COALESCE(s."clearingDocument"',
    );
    expect(sql).not.toContain("document_type IN");
    expect(affected).toBe(4);
  });

  test("uses typed/canonical fields first and retains legacy Company Code and Account fallbacks", async () => {
    const sequelize = {
      query: jest.fn().mockResolvedValue([[], { rowCount: 0 }]),
    };

    await applyCreditAppliedExclusion({
      sequelize,
      transaction: { id: "apply-tx" },
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    const sql = sequelize.query.mock.calls[0][0];
    expect(sql).toContain('candidate."data"->>\'company_code\'');
    expect(sql).toContain('candidate."data"->>\'Company Code\'');
    expect(sql).toContain('candidate."sourceAccountCode"');
    expect(sql).toContain('candidate."data"->>\'source_account_code\'');
    expect(sql).toContain('candidate."data"->>\'Account\'');
    expect(sql).toContain('candidate."clearingDocument"');
    expect(sql).toContain('candidate."data"->>\'clearing_document\'');
  });

  test("preserves existing reasons and appends CREDIT_APPLIED metadata", async () => {
    const sequelize = {
      query: jest.fn().mockResolvedValue([[], { rowCount: 1 }]),
    };

    await applyCreditAppliedExclusion({
      sequelize,
      transaction: { id: "apply-tx" },
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    const sql = sequelize.query.mock.calls[0][0];
    expect(sql).toContain("'CREDIT_APPLIED'");
    expect(sql).toContain("Credit applied — clearing group begins 200");
    expect(sql).toContain("s.\"data\"->>'exclude_reason'");
    expect(sql).toContain("s.\"data\"->'exclude_reasons'");
    expect(sql).toContain("'{exclusions,reasons}'");
    expect(sql).toContain("'{exclusions,comments}'");
  });

  test("preview uses the same group key and does not mutate stage rows", async () => {
    const sequelize = {
      query: jest
        .fn()
        .mockResolvedValueOnce([[{ matchedCount: 4, alreadyExcludedCount: 1 }]])
        .mockResolvedValueOnce([[{ row_no: 2, document_type: "RE" }]]),
    };

    const result = await previewCreditAppliedExclusion({
      sequelize,
      transaction: { id: "preview-tx" },
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      effectiveLimit: 10,
    });

    for (const [sql] of sequelize.query.mock.calls) {
      expect(sql).toContain("SELECT DISTINCT");
      expect(sql).toContain("LIKE '200%'");
      expect(sql).toContain('matched_group."companyCode" =');
      expect(sql).toContain('matched_group."account" =');
      expect(sql).toContain('matched_group."clearingDocument" =');
      expect(sql).not.toMatch(/\bUPDATE\s+"tbl_ptrs_stage_row"/i);
    }
    expect(result).toEqual({
      matched: 4,
      alreadyExcluded: 1,
      sampleRows: [{ row_no: 2, document_type: "RE" }],
    });
  });
});

const {
  applyInternationalExclusion,
  previewInternationalExclusion,
} = require("./exclusions.international");

describe("PTRS international exclusion", () => {
  test("applies the existing exclusion metadata from the typed currency in one compact update", async () => {
    const transaction = { id: "apply-tx" };
    const sequelize = {
      query: jest.fn().mockResolvedValue([[], { rowCount: 3 }]),
    };

    const affected = await applyInternationalExclusion({
      sequelize,
      transaction,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });

    expect(affected).toBe(3);
    expect(sequelize.query).toHaveBeenCalledTimes(1);
    const [sql, options] = sequelize.query.mock.calls[0];
    expect(sql).toContain("NULLIF(trim(s.\"documentCurrency\"), '')");
    expect(sql).toContain("upper(s.\"documentCurrency\") <> 'AUD'");
    expect(sql).not.toContain("data\"->>'document_currency'");
    expect(sql).not.toContain("data\"->>'Document Currency'");
    expect(sql).toContain("'exclude', true");
    expect(sql).toContain("'exclude_from_metrics', true");
    expect(sql).toContain("'exclude_reason'");
    expect(sql).toContain("'exclude_reasons'");
    expect(sql).toContain("'exclude_comment'");
    expect(sql).toContain("'INTERNATIONAL'");
    expect(sql).toContain("International supplier — non-AUD document currency");
    expect(sql).toContain("'exclusions'");
    expect(sql).toContain("'reasons'");
    expect(sql).toContain("'comments'");
    expect(sql).toContain("AND NOT (");
    expect(sql).not.toMatch(/\bRETURNING\b/i);
    expect(Buffer.byteLength(sql)).toBeLessThan(10000);
    expect(options).toEqual({
      replacements: { customerId: "customer-1", ptrsId: "ptrs-1" },
      transaction,
    });
  });

  test("returns zero without adding statements when no rows match", async () => {
    const sequelize = {
      query: jest.fn().mockResolvedValue([[], { rowCount: 0 }]),
    };

    await expect(
      applyInternationalExclusion({
        sequelize,
        transaction: { id: "apply-tx" },
        customerId: "customer-1",
        ptrsId: "ptrs-1",
      }),
    ).resolves.toBe(0);
    expect(sequelize.query).toHaveBeenCalledTimes(1);
  });

  test("preview uses the same typed predicate and remains read-only", async () => {
    const transaction = { id: "preview-tx" };
    const sequelize = {
      query: jest
        .fn()
        .mockResolvedValueOnce([[{ matchedCount: 3, alreadyExcludedCount: 1 }]])
        .mockResolvedValueOnce([
          [{ rowNo: 7, document_currency: "NZD", alreadyExcluded: false }],
        ]),
    };

    const result = await previewInternationalExclusion({
      sequelize,
      transaction,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      effectiveLimit: 10,
    });

    expect(result).toEqual({
      matched: 3,
      alreadyExcluded: 1,
      sampleRows: [
        { rowNo: 7, document_currency: "NZD", alreadyExcluded: false },
      ],
    });
    expect(sequelize.query).toHaveBeenCalledTimes(2);
    for (const [sql, options] of sequelize.query.mock.calls) {
      expect(sql).toContain("NULLIF(trim(s.\"documentCurrency\"), '')");
      expect(sql).toContain("upper(s.\"documentCurrency\") <> 'AUD'");
      expect(sql).not.toContain("data\"->>'document_currency'");
      expect(sql).not.toContain("data\"->>'Document Currency'");
      expect(sql).not.toMatch(/\bUPDATE\b/i);
      expect(options).toEqual(
        expect.objectContaining({
          replacements: expect.objectContaining({
            customerId: "customer-1",
            ptrsId: "ptrs-1",
          }),
          transaction,
        }),
      );
    }
  });
});

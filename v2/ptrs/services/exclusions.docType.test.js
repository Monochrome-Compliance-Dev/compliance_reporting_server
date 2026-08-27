const {
  applyDocTypeExclusion,
  previewDocTypeExclusion,
} = require("./exclusions.docType");

describe("PTRS document-type exclusion", () => {
  test("retains K1 and Z/KZ/AB rules without the old 2000-prefix rule", async () => {
    const sequelize = {
      query: jest
        .fn()
        .mockResolvedValueOnce([[], { rowCount: 1 }])
        .mockResolvedValueOnce([[{ matchedCount: 1, alreadyExcludedCount: 0 }]])
        .mockResolvedValueOnce([[]]),
    };

    await applyDocTypeExclusion({
      sequelize,
      transaction: { id: "apply-tx" },
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });
    await previewDocTypeExclusion({
      sequelize,
      transaction: { id: "preview-tx" },
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      effectiveLimit: 10,
    });

    const sql = sequelize.query.mock.calls.map(([query]) => query).join("\n");
    expect(sql).toContain("= 'K1'");
    expect(sql).toContain("IN ('Z', 'KZ', 'AB')");
    expect(sql).toContain("LIKE '5%'");
    expect(sql.match(/s\."semanticKind" = 'accounting_event'/g)).toHaveLength(3);
    expect(sql).not.toContain("LIKE '2000%'");
    expect(sql).not.toContain("clearing document begins 2000");
  });
});

jest.mock("@/db/database", () => ({}));

const {
  applyTcpGroupExclusionPropagation,
  buildTcpGroupExclusionPropagationSql,
} = require("./exclusions.group");
const {
  buildIntraGroupExclusionSql,
} = require("./exclusions.intraCompany");

describe("PTRS TCP exclusion group propagation", () => {
  test("uses the exact accounting normalisation group and approved reason", () => {
    const sql = buildTcpGroupExclusionPropagationSql("GOVERNMENT_ENTITY");

    expect(sql).toContain('source."sourceGroupScope"');
    expect(sql).toContain('source."data"->>\'company_code\'');
    expect(sql).toContain('source."sourceAccountCode"');
    expect(sql).toContain('source."clearingDocument"');
    expect(sql).toContain("'dataset:' || source.\"datasetId\"");
    expect(sql).toContain("GOVERNMENT_ENTITY");
    expect(sql).toContain("Payment group inherits GOVERNMENT_ENTITY");
    expect(sql).not.toMatch(/payeeEntityName|description/);
  });

  test("propagates INTRA_GROUP from a 106 supplier over the existing exact group key", () => {
    const originSql = buildIntraGroupExclusionSql();
    const propagationSql = buildTcpGroupExclusionPropagationSql("INTRA_GROUP");

    expect(originSql).toContain("LIKE '106%'");
    expect(originSql).toContain("INTRA_GROUP");
    expect(propagationSql).toContain("Payment group inherits INTRA_GROUP");
    expect(propagationSql).toContain('source."sourceGroupScope"');
    expect(propagationSql).toContain('source."data"->>\'company_code\'');
    expect(propagationSql).toContain('source."sourceAccountCode"');
    expect(propagationSql).toContain('source."clearingDocument"');
  });

  test("rejects reasons outside the approved TCP group set", () => {
    expect(() =>
      buildTcpGroupExclusionPropagationSql("UNRECOGNISED_DOCUMENT_TYPE"),
    ).toThrow("Unsupported TCP group exclusion reason");
  });

  test("propagates each requested reason in the existing transaction", async () => {
    const sequelize = {
      query: jest
        .fn()
        .mockResolvedValueOnce([[], { rowCount: 2 }])
        .mockResolvedValueOnce([[], { rowCount: 1 }]),
    };
    const transaction = { id: "tx-1" };

    await expect(
      applyTcpGroupExclusionPropagation({
        sequelize,
        transaction,
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        reasons: ["GOVERNMENT_ENTITY", "NO_ABN"],
      }),
    ).resolves.toBe(3);
    expect(sequelize.query).toHaveBeenCalledTimes(2);
    for (const [, options] of sequelize.query.mock.calls) {
      expect(options).toEqual({
        replacements: { customerId: "customer-1", ptrsId: "ptrs-1" },
        transaction,
      });
    }
  });
});

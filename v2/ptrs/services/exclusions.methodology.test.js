const {
  buildEmployeeExclusionSql,
  isEmployeePayment,
} = require("./exclusions.employee");
const {
  buildIntraGroupExclusionSql,
  isIntraGroupSupplier,
} = require("./exclusions.intraCompany");
const { buildAbnEligibilitySql } = require("./exclusions.abn");

describe("PTRS Veolia eligibility rules", () => {
  test("uses supplier prefix 103 or document type K1/EZ for employee payments", () => {
    const sql = buildEmployeeExclusionSql();
    expect(sql).toContain("LIKE '103%'");
    expect(sql).toContain("IN ('K1', 'EZ')");
    expect(sql).toContain("EMPLOYEE_PAYMENT");
    expect(sql).not.toContain("LIKE '106%'");
    expect(sql).not.toMatch(/description|payeeEntityName|sourceUser/);
  });

  test.each([
    ["supplier prefix 103", { sourceAccountCode: " 103456 " }, true],
    ["document type K1", { documentType: " k1 " }, true],
    ["document type EZ", { documentType: " ez " }, true],
    ["supplier prefix 106 alone", { sourceAccountCode: "106456" }, false],
    [
      "unrelated supplier and document type",
      { sourceAccountCode: "101234", documentType: "RE" },
      false,
    ],
  ])("classifies %s deterministically", (_label, input, expected) => {
    expect(isEmployeePayment(input)).toBe(expected);
  });

  test("uses supplier-number prefixes S and 106 for intra-group payments", () => {
    const sql = buildIntraGroupExclusionSql();
    expect(sql).toContain("LIKE 'S%'");
    expect(sql).toContain("LIKE '106%'");
    expect(sql).toContain("INTRA_GROUP");
    expect(sql).not.toContain("EMPLOYEE_PAYMENT");
    expect(sql).not.toMatch(/description|payeeEntityName|payeeEntityAbn/);
  });

  test.each([
    ["supplier prefix S", " s12345 ", true],
    ["supplier prefix 106", " 106456 ", true],
    ["employee supplier prefix 103", "103456", false],
    ["ordinary supplier", "101234", false],
  ])("classifies %s for intra-group eligibility", (_label, account, expected) => {
    expect(isIntraGroupSupplier(account)).toBe(expected);
  });

  test("distinguishes explicit no ABN, mapping, checksum and lookup failures", () => {
    const sql = buildAbnEligibilitySql();
    expect(sql).toContain("= 'N/A'");
    expect(sql).toContain("'NO_ABN'");
    expect(sql).toContain("'MAPPING_EXCEPTION'");
    expect(sql).toContain("'INVALID_ABN'");
    expect(sql).toContain("'ABN_NOT_CONFIRMED'");
    expect(sql).toContain('"tbl_ptrs_abr_lookup_cache"');
    expect(sql).toContain("% 89 = 0");
  });
});

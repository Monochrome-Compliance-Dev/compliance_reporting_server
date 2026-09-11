jest.mock("@/db/database", () => ({}));

const xlsx = require("xlsx");
const { parseConfiguredWorkbook } = require("./workbook-import.ptrs.service");

function workbookWithSheets(definitions) {
  const workbook = xlsx.utils.book_new();
  for (const [sheetName, rows] of Object.entries(definitions)) {
    xlsx.utils.book_append_sheet(
      workbook,
      xlsx.utils.aoa_to_sheet(rows),
      sheetName,
    );
  }
  return workbook;
}

const transactionSheet = {
  sheetName: "Transactions",
  purpose: "transaction",
  adapterType: "sap_accounting_event",
  adapterVersion: "1",
  sourceGroupScope: "orontide-payables",
  requiredColumns: ["Document Type", "Amount"],
};

describe("PTRS configured workbook import", () => {
  test("turns configured sheets into ordinary classified raw-row datasets", () => {
    const workbook = workbookWithSheets({
      Transactions: [
        ["Document Type", "Amount"],
        ["RE", 100],
        ["ZP", -100],
      ],
      "Vendor Master": [
        ["Supplier", "ABN"],
        ["10001", "51824753556"],
      ],
    });

    const parsed = parseConfiguredWorkbook({
      workbook,
      sheetConfigs: [
        transactionSheet,
        {
          sheetName: "Vendor Master",
          purpose: "reference",
          referenceKind: "vendormaster",
          requiredColumns: ["Supplier", "ABN"],
        },
      ],
    });

    expect(parsed).toHaveLength(2);
    expect(parsed[0]).toMatchObject({
      actualSheetName: "Transactions",
      headers: ["Document Type", "Amount"],
      rows: [
        { rowNo: 2, data: { "Document Type": "RE", Amount: "100" } },
        { rowNo: 3, data: { "Document Type": "ZP", Amount: "-100" } },
      ],
    });
    expect(parsed[0].config).toMatchObject({
      purpose: "transaction",
      role: "transaction",
      sourceFormat: "xlsx",
    });
    expect(parsed[1].config).toMatchObject({
      purpose: "reference",
      role: "vendormaster",
    });
  });

  test("fails explicitly when a configured sheet is missing", () => {
    const workbook = workbookWithSheets({ Other: [["Column"], ["value"]] });
    expect(() =>
      parseConfiguredWorkbook({ workbook, sheetConfigs: [transactionSheet] }),
    ).toThrow(expect.objectContaining({ code: "WORKBOOK_SHEET_MISSING" }));
  });

  test("fails explicitly for missing or ambiguous columns", () => {
    const missing = workbookWithSheets({
      Transactions: [["Document Type"], ["RE"]],
    });
    expect(() =>
      parseConfiguredWorkbook({
        workbook: missing,
        sheetConfigs: [transactionSheet],
      }),
    ).toThrow(expect.objectContaining({ code: "WORKBOOK_COLUMNS_MISSING" }));

    const duplicate = workbookWithSheets({
      Transactions: [
        ["Document Type", "Amount", "amount"],
        ["RE", 1, 1],
      ],
    });
    expect(() =>
      parseConfiguredWorkbook({
        workbook: duplicate,
        sheetConfigs: [transactionSheet],
      }),
    ).toThrow(expect.objectContaining({ code: "WORKBOOK_COLUMNS_AMBIGUOUS" }));
  });

  test("does not silently skip an unconfigured worksheet", () => {
    const workbook = workbookWithSheets({
      Transactions: [
        ["Document Type", "Amount"],
        ["RE", 100],
      ],
      Mystery: [["Value"], ["unknown"]],
    });
    expect(() =>
      parseConfiguredWorkbook({ workbook, sheetConfigs: [transactionSheet] }),
    ).toThrow(
      expect.objectContaining({ code: "WORKBOOK_STRUCTURE_UNRECOGNISED" }),
    );
    expect(() =>
      parseConfiguredWorkbook({
        workbook,
        sheetConfigs: [transactionSheet],
        ignoredSheetNames: ["Mystery"],
      }),
    ).not.toThrow();
  });

  test("requires explicit transaction adapter and workbook grouping scope", () => {
    const workbook = workbookWithSheets({
      Transactions: [
        ["Document Type", "Amount"],
        ["RE", 100],
      ],
    });
    expect(() =>
      parseConfiguredWorkbook({
        workbook,
        sheetConfigs: [
          {
            ...transactionSheet,
            adapterType: null,
            sourceGroupScope: null,
          },
        ],
      }),
    ).toThrow(expect.objectContaining({ code: "WORKBOOK_PROFILE_INVALID" }));
  });

  test("preserves physical worksheet row numbers across headings and blank rows", () => {
    const workbook = workbookWithSheets({
      Transactions: [
        ["Report title"],
        ["Document Type", "Amount"],
        [null, null],
        ["RE", 100],
      ],
    });
    const parsed = parseConfiguredWorkbook({
      workbook,
      sheetConfigs: [{ ...transactionSheet, headerRow: 2 }],
    });
    expect(parsed[0].rows).toEqual([
      { rowNo: 4, data: { "Document Type": "RE", Amount: "100" } },
    ]);
  });

  test("honours a configured worksheet range without losing physical row numbers", () => {
    const workbook = workbookWithSheets({
      Transactions: [
        ["Ignore", null, null],
        ["Ignore", "Document Type", "Amount"],
        ["Ignore", "RE", 100],
      ],
    });
    const parsed = parseConfiguredWorkbook({
      workbook,
      sheetConfigs: [
        {
          ...transactionSheet,
          headerRow: 2,
          range: "B2:C3",
        },
      ],
    });
    expect(parsed[0].rows).toEqual([
      { rowNo: 3, data: { "Document Type": "RE", Amount: "100" } },
    ]);
  });

  test("fails when a populated cell sits under an unnamed heading", () => {
    const workbook = workbookWithSheets({
      Transactions: [
        ["Document Type", "Amount", null],
        ["RE", 100, "unclassified"],
      ],
    });
    expect(() =>
      parseConfiguredWorkbook({ workbook, sheetConfigs: [transactionSheet] }),
    ).toThrow(expect.objectContaining({ code: "WORKBOOK_ROW_SHAPE_INVALID" }));
  });
});

const csvInspectionService = require("@/platform/data/csv-inspection.service");

jest.mock("fs", () => ({
  readFileSync: jest.fn(),
}));

const fs = require("fs");

function createBuffer(csvText) {
  return Buffer.from(csvText, "utf8");
}

describe("csv-inspection.service", () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("inspectCsvBuffer", () => {
    it("extracts headers and row count from a normal CSV buffer", () => {
      const result = csvInspectionService.inspectCsvBuffer(
        createBuffer(
          "Supplier,Invoice,Paid Date\nABC,INV-001,2026-07-01\nDEF,INV-002,2026-07-02\n",
        ),
      );

      expect(result).toEqual({
        headers: ["Supplier", "Invoice", "Paid Date"],
        headersCount: 3,
        rowsCount: 2,
      });
    });

    it("removes a UTF-8 BOM before extracting headers", () => {
      const result = csvInspectionService.inspectCsvBuffer(
        createBuffer("\uFEFFSupplier,Invoice\nABC,INV-001\n"),
      );

      expect(result.headers).toEqual(["Supplier", "Invoice"]);
      expect(result.rowsCount).toBe(1);
    });

    it("ignores leading blank lines before the header row", () => {
      const result = csvInspectionService.inspectCsvBuffer(
        createBuffer("\n\nSupplier,Invoice\nABC,INV-001\n"),
      );

      expect(result.headers).toEqual(["Supplier", "Invoice"]);
      expect(result.rowsCount).toBe(1);
    });

    it("repairs blank headers using column numbers", () => {
      const result = csvInspectionService.inspectCsvBuffer(
        createBuffer("Supplier,,Paid Date\nABC,INV-001,2026-07-01\n"),
      );

      expect(result.headers).toEqual(["Supplier", "column_2", "Paid Date"]);
      expect(result.headersCount).toBe(3);
    });

    it("deduplicates duplicate headers using occurrence suffixes", () => {
      const result = csvInspectionService.inspectCsvBuffer(
        createBuffer("Supplier,Supplier,Supplier\nABC,DEF,GHI\n"),
      );

      expect(result.headers).toEqual(["Supplier", "Supplier_2", "Supplier_3"]);
      expect(result.headersCount).toBe(3);
    });

    it("trims header labels", () => {
      const result = csvInspectionService.inspectCsvBuffer(
        createBuffer(" Supplier , Invoice \nABC,INV-001\n"),
      );

      expect(result.headers).toEqual(["Supplier", "Invoice"]);
    });

    it("supports quoted commas in header labels", () => {
      const result = csvInspectionService.inspectCsvBuffer(
        createBuffer('"Supplier, Name",Invoice\nABC,INV-001\n'),
      );

      expect(result.headers).toEqual(["Supplier, Name", "Invoice"]);
      expect(result.rowsCount).toBe(1);
    });

    it("normalises CRLF line endings", () => {
      const result = csvInspectionService.inspectCsvBuffer(
        createBuffer("Supplier,Invoice\r\nABC,INV-001\r\nDEF,INV-002\r\n"),
      );

      expect(result.rowsCount).toBe(2);
    });

    it("ignores blank data rows", () => {
      const result = csvInspectionService.inspectCsvBuffer(
        createBuffer("Supplier,Invoice\nABC,INV-001\n\nDEF,INV-002\n"),
      );

      expect(result.rowsCount).toBe(2);
    });

    it("throws when the buffer is missing", () => {
      expect(() => csvInspectionService.inspectCsvBuffer()).toThrow(
        "CSV buffer is required for dataset creation.",
      );
    });

    it("throws when the buffer is empty", () => {
      expect(() =>
        csvInspectionService.inspectCsvBuffer(Buffer.alloc(0)),
      ).toThrow("CSV buffer must not be empty.");
    });

    it("throws when there is no header row", () => {
      expect(() =>
        csvInspectionService.inspectCsvBuffer(createBuffer("\n\n")),
      ).toThrow("CSV appears to have no header row.");
    });

    it("throws when every header is blank", () => {
      expect(() =>
        csvInspectionService.inspectCsvBuffer(
          createBuffer(",,\nABC,DEF,GHI\n"),
        ),
      ).toThrow("CSV appears to have no header row.");
    });
  });

  describe("inspectCsvFile", () => {
    it("reads a CSV file from disk and inspects the file content", () => {
      fs.readFileSync.mockReturnValue(
        createBuffer("Supplier,Invoice\nABC,INV-001\nDEF,INV-002\n"),
      );

      const result = csvInspectionService.inspectCsvFile(
        "/tmp/mc-platform-data-uploads/payments.csv",
      );

      expect(fs.readFileSync).toHaveBeenCalledWith(
        "/tmp/mc-platform-data-uploads/payments.csv",
      );
      expect(result).toEqual({
        headers: ["Supplier", "Invoice"],
        headersCount: 2,
        rowsCount: 2,
      });
    });

    it("throws when the CSV file path is missing", () => {
      expect(() => csvInspectionService.inspectCsvFile()).toThrow(
        "CSV file path is required for dataset creation.",
      );
    });

    it("surfaces file read failures", () => {
      fs.readFileSync.mockImplementation(() => {
        throw new Error("read failed");
      });

      expect(() =>
        csvInspectionService.inspectCsvFile(
          "/tmp/mc-platform-data-uploads/payments.csv",
        ),
      ).toThrow("read failed");
    });
  });
});

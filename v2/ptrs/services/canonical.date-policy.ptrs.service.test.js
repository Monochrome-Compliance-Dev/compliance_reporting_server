const {
  applyCanonicalInvoiceDatePolicy,
} = require("./canonical.date-policy.ptrs.service");
const {
  computePaymentTimeRegulator,
} = require("./stage.payment-time.ptrs.service");

function sapRow(overrides = {}) {
  return {
    payment_date: "2026-02-10",
    invoice_issue_date: "2026-01-02",
    invoice_receipt_date: "2026-01-01",
    invoice_due_date: "2026-02-01",
    _ptrsMeta: {
      canonicalSources: {
        invoice_issue_date: {
          sourceRole: "transaction",
          sourceDatasetId: "transaction-1",
          sourceColumn: "Document Date",
        },
        invoice_receipt_date: {
          sourceRole: "invoices",
          sourceDatasetId: "invoices-1",
          sourceColumn: "Invoice Date Created - Date",
        },
      },
    },
    ...overrides,
  };
}

describe("PTRS canonical invoice date policy", () => {
  test("uses the earlier invoice-source date for both issue and receipt", () => {
    const before = sapRow();
    expect(computePaymentTimeRegulator(before)).toMatchObject({ days: 40 });

    const result = applyCanonicalInvoiceDatePolicy({
      row: before,
      adapterType: "sap_accounting_event",
    });

    expect(result.invoice_issue_date).toBe("2026-01-01");
    expect(result.invoice_receipt_date).toBe("2026-01-01");
    expect(computePaymentTimeRegulator(result)).toMatchObject({
      days: 41,
      referenceDate: "2026-01-01",
    });
    expect(result._ptrsMeta.canonicalSources.invoice_issue_date).toMatchObject({
      sourceRole: "invoices",
      sourceDatasetId: "invoices-1",
    });
  });

  test("falls back both dates to the SAP issue date without an invoice-source date", () => {
    const row = sapRow({ invoice_receipt_date: null });
    delete row._ptrsMeta.canonicalSources.invoice_receipt_date;

    const result = applyCanonicalInvoiceDatePolicy({
      row,
      adapterType: "sap_accounting_event",
    });

    expect(result.invoice_issue_date).toBe("2026-01-02");
    expect(result.invoice_receipt_date).toBe("2026-01-02");
    expect(computePaymentTimeRegulator(result)).toMatchObject({
      days: 40,
      referenceDate: "2026-01-02",
    });
  });

  test("does not change payment or due date semantics", () => {
    const result = applyCanonicalInvoiceDatePolicy({
      row: sapRow(),
      adapterType: "sap_accounting_event",
    });

    expect(result.payment_date).toBe("2026-02-10");
    expect(result.invoice_due_date).toBe("2026-02-01");
  });

  test("does not apply SAP policy to format-neutral direct payments", () => {
    const row = sapRow();
    const result = applyCanonicalInvoiceDatePolicy({
      row,
      adapterType: "direct_payment",
    });

    expect(result).toBe(row);
    expect(result.invoice_issue_date).toBe("2026-01-02");
    expect(result.invoice_receipt_date).toBe("2026-01-01");
  });
});

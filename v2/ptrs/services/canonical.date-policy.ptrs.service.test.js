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
    _ptrsMeta: { canonicalSources: {} },
    ...overrides,
  };
}

describe("PTRS canonical invoice date policy", () => {
  test("preserves distinct issue and receipt dates and uses the shorter duration", () => {
    const result = applyCanonicalInvoiceDatePolicy({
      row: sapRow(),
      adapterType: "sap_accounting_event",
    });

    expect(result.invoice_issue_date).toBe("2026-01-02");
    expect(result.invoice_receipt_date).toBe("2026-01-01");
    expect(computePaymentTimeRegulator(result)).toEqual({
      days: 40,
      referenceDate: "2026-01-02",
      referenceKind: "invoice_issue",
    });
    expect(result._ptrsMeta.invoiceDatePolicy).toEqual({
      policy: "distinct_invoice_issue_and_receipt",
      issueDate: "2026-01-02",
      receiptDate: "2026-01-01",
    });
  });

  test("does not manufacture a receipt or use due date before the issue-date fallback", () => {
    const result = applyCanonicalInvoiceDatePolicy({
      row: sapRow({ invoice_receipt_date: null }),
      adapterType: "sap_accounting_event",
    });

    expect(result.invoice_receipt_date).toBeNull();
    expect(result.invoice_due_date).toBe("2026-02-01");
    expect(computePaymentTimeRegulator(result)).toEqual({
      days: 40,
      referenceDate: "2026-01-02",
      referenceKind: "invoice_issue",
    });
  });

  test("does not apply SAP policy to format-neutral direct payments", () => {
    const row = sapRow();
    expect(
      applyCanonicalInvoiceDatePolicy({ row, adapterType: "direct_payment" }),
    ).toBe(row);
  });
});

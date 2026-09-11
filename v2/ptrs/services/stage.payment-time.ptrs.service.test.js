jest.mock("@/db/database", () => ({}));

const {
  computePaymentTimeRegulator,
} = require("./stage.payment-time.ptrs.service");

describe("PTRS regulator payment time", () => {
  test("uses the shorter inclusive period while preserving distinct invoice dates", () => {
    expect(
      computePaymentTimeRegulator({
        payment_date: "2026-01-10",
        invoice_issue_date: "2026-01-01",
        invoice_receipt_date: "2026-01-04",
      }),
    ).toEqual({
      days: 7,
      referenceDate: "2026-01-04",
      referenceKind: "invoice_receipt",
    });
  });

  test("uses invoice issue date when receipt date is unknown", () => {
    expect(
      computePaymentTimeRegulator({
        payment_date: "2026-01-10",
        invoice_issue_date: "2026-01-01",
        invoice_due_date: "2026-01-09",
      }),
    ).toEqual({
      days: 10,
      referenceDate: "2026-01-01",
      referenceKind: "invoice_issue",
    });
  });

  test("uses invoice receipt date when issue date is unavailable", () => {
    expect(
      computePaymentTimeRegulator({
        payment_date: "2026-01-10",
        invoice_receipt_date: "2026-01-04",
      }),
    ).toEqual({
      days: 7,
      referenceDate: "2026-01-04",
      referenceKind: "invoice_receipt",
    });
  });

  test("does not substitute invoice due date when both invoice dates are missing", () => {
    expect(
      computePaymentTimeRegulator({
        payment_date: "2026-01-10",
        invoice_due_date: "2026-01-09",
      }),
    ).toEqual({
      days: null,
      referenceDate: null,
      referenceKind: null,
    });
  });

  test("floors pre-reference payments at zero calendar days", () => {
    expect(
      computePaymentTimeRegulator({
        payment_date: "2026-01-01",
        invoice_issue_date: "2026-01-02",
        invoice_receipt_date: "2026-01-03",
      }),
    ).toEqual({
      days: 0,
      referenceDate: "2026-01-03",
      referenceKind: "invoice_receipt",
    });
  });

  test("keeps the RCTI issue-date rule distinct", () => {
    expect(
      computePaymentTimeRegulator({
        payment_date: "2026-01-10",
        invoice_issue_date: "2026-01-01",
        invoice_receipt_date: "2026-01-04",
        rcti: "yes",
      }),
    ).toEqual({
      days: 10,
      referenceDate: "2026-01-01",
      referenceKind: "invoice_issue",
    });
  });

  test("keeps no-invoice notice and supply commencement fallbacks", () => {
    expect(
      computePaymentTimeRegulator({
        payment_date: "2026-01-10",
        notice_for_payment_issue_date: "2026-01-03",
        supply_date: "2026-01-01",
      }),
    ).toEqual({
      days: 8,
      referenceDate: "2026-01-03",
      referenceKind: "notice_for_payment",
    });
    expect(
      computePaymentTimeRegulator({
        payment_date: "2026-01-10",
        supply_date: "2026-01-01",
      }),
    ).toEqual({
      days: 10,
      referenceDate: "2026-01-01",
      referenceKind: "supply",
    });
  });
});

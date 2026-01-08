const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

module.exports = {
  validate,
  getValidate,
};

// -------------------------
// Helpers
// -------------------------

function isExcludedRow(stageRow) {
  const meta = stageRow?.meta || {};
  return meta?.rules?.exclude === true;
}

function normalizeAbn(value) {
  if (value == null) return "";
  return String(value).replace(/\D+/g, "");
}

function isProbablyAbn(abn) {
  return typeof abn === "string" && /^\d{11}$/.test(abn);
}

function parseAusDate(value) {
  // Expecting dd/mm/yyyy (common in uploaded CSVs). Returns Date or null.
  if (value == null) return null;
  const s = String(value).trim();
  if (!s) return null;

  const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(s);
  if (!m) return null;

  const dd = Number(m[1]);
  const mm = Number(m[2]);
  const yyyy = Number(m[3]);

  if (!Number.isFinite(dd) || !Number.isFinite(mm) || !Number.isFinite(yyyy)) {
    return null;
  }

  // JS Date months are 0-based
  const d = new Date(Date.UTC(yyyy, mm - 1, dd));

  // Validate round-trip to avoid things like 32/13/2024 coercing
  if (
    d.getUTCFullYear() !== yyyy ||
    d.getUTCMonth() !== mm - 1 ||
    d.getUTCDate() !== dd
  ) {
    return null;
  }

  return d;
}

function parseMoney(value) {
  // Handles "-11,183.65" and "11183.65" etc. Returns number or null.
  if (value == null) return null;
  const s = String(value).trim();
  if (!s) return null;

  const cleaned = s.replace(/,/g, "");
  const n = Number(cleaned);

  if (!Number.isFinite(n)) return null;
  return n;
}

function toIssue(stageRow, code, message, extra = {}) {
  return {
    stageRowId: stageRow.id,
    rowNo: stageRow.rowNo,
    code,
    message,
    ...extra,
  };
}

function makeRowKey(data) {
  // MVP duplicate heuristic: prefer vlookup if present; else composite key.
  const vlookup = data?.vlookup ? String(data.vlookup).trim() : "";
  if (vlookup) return `vlookup:${vlookup}`;

  const companyCode = data?.company_code
    ? String(data.company_code).trim()
    : "";
  const supplier = normalizeAbn(data?.payee_entity_abn);
  const ref = data?.invoice_reference_number
    ? String(data.invoice_reference_number).trim()
    : "";
  const invoiceDate = data?.invoice_issue_date
    ? String(data.invoice_issue_date).trim()
    : "";
  const amount = data?.payment_amount ? String(data.payment_amount).trim() : "";

  return `cc:${companyCode}|abn:${supplier}|ref:${ref}|inv:${invoiceDate}|amt:${amount}`;
}

// -------------------------
// Service entry points
// -------------------------

async function validate({ customerId, ptrsId, userId = null }) {
  return computeValidate({ customerId, ptrsId, userId, mode: "run" });
}

async function getValidate({ customerId, ptrsId, userId = null }) {
  return computeValidate({ customerId, ptrsId, userId, mode: "read" });
}

async function computeValidate({ customerId, ptrsId, userId, mode }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const ptrs = await db.Ptrs.findOne({
      where: { id: ptrsId, customerId },
      transaction: t,
    });

    if (!ptrs) {
      const e = new Error("Ptrs not found");
      e.statusCode = 404;
      throw e;
    }

    const stageRows = await db.PtrsStageRow.findAll({
      where: { customerId, ptrsId },
      order: [["rowNo", "ASC"]],
      raw: false,
      transaction: t,
    });

    const LIMIT = 200;

    const blockers = [];
    const warnings = [];

    let excludedRows = 0;

    let missingPayeeAbnCount = 0;
    let invalidPayeeAbnCount = 0;

    let missingPayerAbnCount = 0;
    let invalidPayerAbnCount = 0;

    let missingInvoiceDateCount = 0;
    let invalidInvoiceDateCount = 0;

    let missingPaymentDateCount = 0;
    let invalidPaymentDateCount = 0;

    let paymentBeforeInvoiceCount = 0;

    let missingPaymentAmountCount = 0;
    let invalidPaymentAmountCount = 0;

    let duplicatesSuspectedCount = 0;

    let smallBusinessUnknownCount = 0;

    const seenKeys = new Map(); // key -> firstRowNo

    for (const r of stageRows) {
      if (isExcludedRow(r)) {
        excludedRows += 1;
        continue;
      }

      const data = r?.data || {};

      // ---- Payee ABN ----
      const payeeAbn = normalizeAbn(data?.payee_entity_abn);
      if (!payeeAbn) {
        missingPayeeAbnCount += 1;
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(r, "PAYEE_ABN_MISSING", "Missing payee_entity_abn", {
              field: "payee_entity_abn",
            })
          );
        }
      } else if (!isProbablyAbn(payeeAbn)) {
        invalidPayeeAbnCount += 1;
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(
              r,
              "PAYEE_ABN_INVALID",
              "payee_entity_abn is not a valid 11-digit ABN",
              {
                field: "payee_entity_abn",
                value: data?.payee_entity_abn,
              }
            )
          );
        }
      }

      // ---- Payer ABN (if present in dataset, treat as required for report grouping) ----
      const payerAbn = normalizeAbn(data?.payer_entity_abn);
      if (!payerAbn) {
        missingPayerAbnCount += 1;
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(r, "PAYER_ABN_MISSING", "Missing payer_entity_abn", {
              field: "payer_entity_abn",
            })
          );
        }
      } else if (!isProbablyAbn(payerAbn)) {
        invalidPayerAbnCount += 1;
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(
              r,
              "PAYER_ABN_INVALID",
              "payer_entity_abn is not a valid 11-digit ABN",
              {
                field: "payer_entity_abn",
                value: data?.payer_entity_abn,
              }
            )
          );
        }
      }

      // ---- Dates ----
      const invoiceDateRaw = data?.invoice_issue_date;
      const paymentDateRaw = data?.payment_date;

      const invoiceDate = parseAusDate(invoiceDateRaw);
      const paymentDate = parseAusDate(paymentDateRaw);

      if (!invoiceDateRaw) {
        missingInvoiceDateCount += 1;
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(r, "INVOICE_DATE_MISSING", "Missing invoice_issue_date", {
              field: "invoice_issue_date",
            })
          );
        }
      } else if (!invoiceDate) {
        invalidInvoiceDateCount += 1;
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(
              r,
              "INVOICE_DATE_INVALID",
              "invoice_issue_date is not a valid dd/mm/yyyy date",
              {
                field: "invoice_issue_date",
                value: invoiceDateRaw,
              }
            )
          );
        }
      }

      if (!paymentDateRaw) {
        missingPaymentDateCount += 1;
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(r, "PAYMENT_DATE_MISSING", "Missing payment_date", {
              field: "payment_date",
            })
          );
        }
      } else if (!paymentDate) {
        invalidPaymentDateCount += 1;
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(
              r,
              "PAYMENT_DATE_INVALID",
              "payment_date is not a valid dd/mm/yyyy date",
              {
                field: "payment_date",
                value: paymentDateRaw,
              }
            )
          );
        }
      }

      if (invoiceDate && paymentDate) {
        if (paymentDate.getTime() < invoiceDate.getTime()) {
          paymentBeforeInvoiceCount += 1;
          if (warnings.length < LIMIT) {
            warnings.push(
              toIssue(
                r,
                "PAYMENT_BEFORE_INVOICE",
                "payment_date is earlier than invoice_issue_date (check for credit notes/adjustments)",
                {
                  invoice_issue_date: invoiceDateRaw,
                  payment_date: paymentDateRaw,
                }
              )
            );
          }
        }
      }

      // ---- Amounts ----
      const paymentAmountRaw = data?.payment_amount;
      const paymentAmount = parseMoney(paymentAmountRaw);

      if (paymentAmountRaw == null || String(paymentAmountRaw).trim() === "") {
        missingPaymentAmountCount += 1;
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(r, "PAYMENT_AMOUNT_MISSING", "Missing payment_amount", {
              field: "payment_amount",
            })
          );
        }
      } else if (paymentAmount == null) {
        invalidPaymentAmountCount += 1;
        if (blockers.length < LIMIT) {
          blockers.push(
            toIssue(
              r,
              "PAYMENT_AMOUNT_INVALID",
              "payment_amount is not a valid number",
              {
                field: "payment_amount",
                value: paymentAmountRaw,
              }
            )
          );
        }
      }

      // ---- Duplicates (heuristic) ----
      const key = makeRowKey(data);
      if (seenKeys.has(key)) {
        duplicatesSuspectedCount += 1;
        const firstRowNo = seenKeys.get(key);
        if (warnings.length < LIMIT) {
          warnings.push(
            toIssue(
              r,
              "DUPLICATE_SUSPECTED",
              "Duplicate-suspected row based on key heuristic",
              {
                duplicateOfRowNo: firstRowNo,
                key,
              }
            )
          );
        }
      } else {
        seenKeys.set(key, r.rowNo);
      }

      // ---- Small business status completeness (post-SBI, this should usually be set) ----
      // Treat as warning only for MVP; we can tighten later.
      if (data?.is_small_business == null) {
        smallBusinessUnknownCount += 1;
        if (warnings.length < LIMIT) {
          warnings.push(
            toIssue(
              r,
              "SMALL_BUSINESS_UNKNOWN",
              "Small business status is missing (post-SBI this should normally be set)",
              { field: "is_small_business" }
            )
          );
        }
      }
    }

    const status =
      blockers.length > 0
        ? "BLOCKED"
        : warnings.length > 0
          ? "PASSED_WITH_WARNINGS"
          : "PASSED";

    await t.commit();

    return {
      status,
      ptrsId,
      mode,
      counts: {
        totalRows: stageRows.length,
        excludedRows,
        blockers: blockers.length,
        warnings: warnings.length,
        missingPayeeAbnCount,
        invalidPayeeAbnCount,
        missingPayerAbnCount,
        invalidPayerAbnCount,
        missingInvoiceDateCount,
        invalidInvoiceDateCount,
        missingPaymentDateCount,
        invalidPaymentDateCount,
        paymentBeforeInvoiceCount,
        missingPaymentAmountCount,
        invalidPaymentAmountCount,
        duplicatesSuspectedCount,
        smallBusinessUnknownCount,
      },
      blockers,
      warnings,
    };
  } catch (err) {
    try {
      await t.rollback();
    } catch (_) {
      // ignore rollback errors
    }
    throw err;
  }
}

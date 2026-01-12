const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

module.exports = {
  validate,
  getValidate,
  getValidateSummary,
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

// Aggregated Validate summary endpoint for PTRS v2
async function getValidateSummary({ customerId, ptrsId, profileId = null }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const baseWhere = `"customerId" = :customerId AND "ptrsId" = :ptrsId`;

    // Exclusion logic: canonical exclude flag OR rule meta flag
    const excludedExpr = `(
      COALESCE((data->>'exclude_from_metrics')::boolean, false) = true
      OR COALESCE((meta->'rules'->>'exclude')::boolean, false) = true
    )`;

    // Included trade credit population: not excluded, trade credit true, excluded_trade_credit_payment not true
    const tradeCreditIncludedExpr = `(
      NOT ${excludedExpr}
      AND COALESCE((data->>'trade_credit_payment')::boolean, false) = true
      AND COALESCE((data->>'excluded_trade_credit_payment')::boolean, false) = false
    )`;

    // Trade credit excluded (for visibility): not excluded, trade credit true, but excluded_trade_credit_payment true
    const tradeCreditExcludedExpr = `(
      NOT ${excludedExpr}
      AND COALESCE((data->>'trade_credit_payment')::boolean, false) = true
      AND COALESCE((data->>'excluded_trade_credit_payment')::boolean, false) = true
    )`;

    const countsResult = await db.sequelize.query(
      `
      SELECT
        COUNT(*)::int AS "stageRowCount",
        SUM(CASE WHEN ${excludedExpr} THEN 1 ELSE 0 END)::int AS "excludedRowCount",
        SUM(CASE WHEN NOT ${excludedExpr} THEN 1 ELSE 0 END)::int AS "includedRowCount",

        SUM(CASE WHEN ${tradeCreditIncludedExpr} THEN 1 ELSE 0 END)::int AS "tradeCreditIncludedCount",
        SUM(CASE WHEN ${tradeCreditExcludedExpr} THEN 1 ELSE 0 END)::int AS "tradeCreditExcludedCount",

        SUM(CASE WHEN ${tradeCreditIncludedExpr} AND COALESCE((data->>'is_small_business')::boolean, NULL) = true THEN 1 ELSE 0 END)::int AS "sbTrueCount",
        SUM(CASE WHEN ${tradeCreditIncludedExpr} AND COALESCE((data->>'is_small_business')::boolean, NULL) = false THEN 1 ELSE 0 END)::int AS "sbFalseCount",
        SUM(CASE WHEN ${tradeCreditIncludedExpr} AND (data->>'is_small_business') IS NULL THEN 1 ELSE 0 END)::int AS "sbUnknownCount"
      FROM tbl_ptrs_stage_row
      WHERE ${baseWhere}
      `,
      {
        transaction: t,
        replacements: { customerId, ptrsId },
        type: db.sequelize.QueryTypes.SELECT,
      }
    );

    const countsRow = Array.isArray(countsResult) ? countsResult[0] : null;

    const stageRowCount = Number(countsRow?.stageRowCount) || 0;
    const excludedRowCount = Number(countsRow?.excludedRowCount) || 0;
    const includedRowCount = Number(countsRow?.includedRowCount) || 0;
    const tradeCreditIncludedCount =
      Number(countsRow?.tradeCreditIncludedCount) || 0;
    const tradeCreditExcludedCount =
      Number(countsRow?.tradeCreditExcludedCount) || 0;
    const sbTrueCount = Number(countsRow?.sbTrueCount) || 0;
    const sbFalseCount = Number(countsRow?.sbFalseCount) || 0;
    const sbUnknownCount = Number(countsRow?.sbUnknownCount) || 0;

    // Payment time breakdown by reference kind
    const byKindRows = await db.sequelize.query(
      `
      SELECT
        CASE
          WHEN (data->>'payment_time_reference_kind') IS NULL OR (data->>'payment_time_reference_kind') = '' THEN 'missing'
          ELSE (data->>'payment_time_reference_kind')
        END AS kind,
        COUNT(*)::int AS count
      FROM tbl_ptrs_stage_row
      WHERE ${baseWhere} AND ${tradeCreditIncludedExpr}
      GROUP BY 1
      ORDER BY 1
      `,
      {
        transaction: t,
        replacements: { customerId, ptrsId },
        type: db.sequelize.QueryTypes.SELECT,
      }
    );

    const kindsWanted = [
      "invoice_issue",
      "invoice_receipt",
      "notice",
      "supply",
      "missing",
    ];
    const kindCountMap = new Map(kindsWanted.map((k) => [k, 0]));
    for (const r of byKindRows || []) {
      const k = String(r.kind || "missing");
      if (!kindCountMap.has(k)) continue;
      kindCountMap.set(k, Number(r.count) || 0);
    }

    const paymentTimeByReferenceKind = Array.from(kindCountMap.entries()).map(
      ([kind, count]) => ({ kind, count })
    );

    const missingTimeRows = await db.sequelize.query(
      `
      SELECT
        SUM(CASE WHEN (data->>'payment_date') IS NULL OR (data->>'payment_date') = '' THEN 1 ELSE 0 END)::int AS missing_payment_date,
        SUM(CASE WHEN (data->>'payment_time_reference_date') IS NULL OR (data->>'payment_time_reference_date') = '' THEN 1 ELSE 0 END)::int AS missing_reference_date,
        SUM(CASE WHEN ((data->>'payment_date') IS NULL OR (data->>'payment_date') = '') AND ((data->>'payment_time_reference_date') IS NULL OR (data->>'payment_time_reference_date') = '') THEN 1 ELSE 0 END)::int AS missing_both
      FROM tbl_ptrs_stage_row
      WHERE ${baseWhere} AND ${tradeCreditIncludedExpr}
      `,
      {
        transaction: t,
        replacements: { customerId, ptrsId },
        type: db.sequelize.QueryTypes.SELECT,
      }
    );

    const missingTimeRow = Array.isArray(missingTimeRows)
      ? missingTimeRows[0]
      : null;

    const missingPaymentDate =
      Number(missingTimeRow?.missing_payment_date) || 0;
    const missingReferenceDate =
      Number(missingTimeRow?.missing_reference_date) || 0;
    const missingBoth = Number(missingTimeRow?.missing_both) || 0;

    const paymentTimeExamples = await db.sequelize.query(
      `
      SELECT
        data->>'invoice_reference_number' AS invoice_reference_number,
        data->>'payment_date' AS payment_date,
        data->>'payment_time_reference_date' AS payment_time_reference_date,
        data->>'payment_time_reference_kind' AS payment_time_reference_kind,
        (data->>'payment_time_days')::int AS payment_time_days
      FROM tbl_ptrs_stage_row
      WHERE ${baseWhere} AND ${tradeCreditIncludedExpr}
      ORDER BY "rowNo" ASC
      LIMIT 5
      `,
      {
        transaction: t,
        replacements: { customerId, ptrsId },
        type: db.sequelize.QueryTypes.SELECT,
      }
    );

    // Payment terms dedupe table
    const paymentTermsRows = await db.sequelize.query(
      `
      SELECT
        CASE
          WHEN (data->>'payment_term') IS NULL OR (data->>'payment_term') = '' THEN '(blank)'
          ELSE (data->>'payment_term')
        END AS payment_term_raw,
        NULLIF((data->>'payment_term_days'), '')::int AS payment_term_days,
        CASE
          WHEN (data->>'payment_term') IS NULL OR (data->>'payment_term') = '' THEN 'missing'
          WHEN (data->>'payment_term_days') IS NULL OR (data->>'payment_term_days') = '' THEN 'unmapped'
          WHEN (data->>'payment_term_source') IS NOT NULL AND (data->>'payment_term_source') <> '' THEN (data->>'payment_term_source')
          ELSE 'unknown'
        END AS payment_term_source,
        COUNT(*)::int AS count
      FROM tbl_ptrs_stage_row
      WHERE ${baseWhere} AND ${tradeCreditIncludedExpr}
      GROUP BY 1,2,3
      ORDER BY 4 DESC, 1 ASC
      `,
      {
        transaction: t,
        replacements: { customerId, ptrsId },
        type: db.sequelize.QueryTypes.SELECT,
      }
    );

    const unmappedRawRows = await db.sequelize.query(
      `
      SELECT
        (data->>'payment_term') AS raw,
        COUNT(*)::int AS count
      FROM tbl_ptrs_stage_row
      WHERE ${baseWhere}
        AND ${tradeCreditIncludedExpr}
        AND (data->>'payment_term') IS NOT NULL
        AND (data->>'payment_term') <> ''
        AND ((data->>'payment_term_days') IS NULL OR (data->>'payment_term_days') = '')
      GROUP BY 1
      ORDER BY 2 DESC, 1 ASC
      `,
      {
        transaction: t,
        replacements: { customerId, ptrsId },
        type: db.sequelize.QueryTypes.SELECT,
      }
    );

    const unmappedRawValues = (unmappedRawRows || [])
      .map((r) => String(r.raw))
      .filter((v) => v != null && v.trim() !== "");

    const unmappedCount = (unmappedRawRows || []).reduce(
      (acc, r) => acc + (Number(r.count) || 0),
      0
    );

    // Canonical missing counts within the trade credit included population
    const missingCanonRows = await db.sequelize.query(
      `
      SELECT
        SUM(CASE WHEN (data->>'payment_term_days') IS NULL OR (data->>'payment_term_days') = '' THEN 1 ELSE 0 END)::int AS missing_payment_term_days,
        SUM(CASE WHEN (data->>'is_small_business') IS NULL OR (data->>'is_small_business') = '' THEN 1 ELSE 0 END)::int AS missing_is_small_business,
        SUM(CASE WHEN (data->>'payment_time_days') IS NULL OR (data->>'payment_time_days') = '' THEN 1 ELSE 0 END)::int AS missing_payment_time_days,
        SUM(CASE WHEN (data->>'payment_amount') IS NULL OR (data->>'payment_amount') = '' THEN 1 ELSE 0 END)::int AS missing_payment_amount,
        SUM(CASE WHEN (data->>'payment_time_reference_date') IS NULL OR (data->>'payment_time_reference_date') = '' THEN 1 ELSE 0 END)::int AS missing_payment_time_reference_date,
        SUM(CASE WHEN (data->>'payment_date') IS NULL OR (data->>'payment_date') = '' THEN 1 ELSE 0 END)::int AS missing_payment_date
      FROM tbl_ptrs_stage_row
      WHERE ${baseWhere} AND ${tradeCreditIncludedExpr}
      `,
      {
        transaction: t,
        replacements: { customerId, ptrsId },
        type: db.sequelize.QueryTypes.SELECT,
      }
    );

    const missingCanonRow = Array.isArray(missingCanonRows)
      ? missingCanonRows[0]
      : null;

    const missingByField = [];
    const pushMissing = (field, count) => {
      const n = Number(count) || 0;
      if (n > 0) missingByField.push({ field, count: n });
    };

    pushMissing(
      "payment_term_days",
      missingCanonRow?.missing_payment_term_days
    );
    pushMissing(
      "is_small_business",
      missingCanonRow?.missing_is_small_business
    );
    pushMissing(
      "payment_time_days",
      missingCanonRow?.missing_payment_time_days
    );
    pushMissing("payment_amount", missingCanonRow?.missing_payment_amount);
    pushMissing(
      "payment_time_reference_date",
      missingCanonRow?.missing_payment_time_reference_date
    );
    pushMissing("payment_date", missingCanonRow?.missing_payment_date);

    const paymentTimeMissingTotal =
      missingPaymentDate + missingReferenceDate + missingBoth;
    const paymentTimeStatus = paymentTimeMissingTotal > 0 ? "fail" : "pass";

    const missingTermDays =
      Number(missingCanonRow?.missing_payment_term_days) || 0;
    const paymentTermsStatus = missingTermDays > 0 ? "fail" : "pass";

    const smallBusinessStatus = sbUnknownCount > 0 ? "warn" : "pass";

    let metricsReadyStatus = "pass";
    if (paymentTimeStatus === "fail" || paymentTermsStatus === "fail") {
      metricsReadyStatus = "fail";
    } else if (smallBusinessStatus === "warn") {
      metricsReadyStatus = "warn";
    }

    const meta = {
      ptrsId,
      profileId: profileId || null,
      generatedAt: new Date().toISOString(),
      mode: "read",
    };

    const summary = {
      stage: {
        stageRowCount,
        excludedRowCount,
        includedRowCount,
      },
      population: {
        tradeCreditIncludedCount,
        tradeCreditExcludedCount,
        smallBusinessTrueCount: sbTrueCount,
        smallBusinessFalseCount: sbFalseCount,
        smallBusinessUnknownCount: sbUnknownCount,
      },
    };

    const gates = {
      paymentTime: {
        status: paymentTimeStatus,
        missingCount: paymentTimeMissingTotal,
        missingFields:
          paymentTimeStatus === "fail"
            ? ["payment_date", "payment_time_reference_date"]
            : [],
        message:
          paymentTimeStatus === "fail"
            ? "Payment time is missing required date inputs for some included trade credit rows."
            : "Payment time is available for all included trade credit rows.",
      },
      paymentTerms: {
        status: paymentTermsStatus,
        missingCount: missingTermDays,
        missingFields:
          paymentTermsStatus === "fail" ? ["payment_term_days"] : [],
        message:
          paymentTermsStatus === "fail"
            ? "Payment term days are missing for some included trade credit rows."
            : "Payment term days are available for all included trade credit rows.",
      },
      smallBusiness: {
        status: smallBusinessStatus,
        missingCount: sbUnknownCount,
        missingFields:
          smallBusinessStatus === "warn" ? ["is_small_business"] : [],
        message:
          smallBusinessStatus === "warn"
            ? "Small business status is missing; SB-only metrics will be unavailable until SBI results are applied."
            : "Small business status is available for all included trade credit rows.",
      },
      metricsReady: {
        status: metricsReadyStatus,
        message:
          metricsReadyStatus === "fail"
            ? "Metrics are blocked until required canonical fields are populated."
            : metricsReadyStatus === "warn"
              ? "Metrics can run, but SB-only metrics may be incomplete until SBI is applied."
              : "Metrics are ready.",
      },
    };

    const sections = {
      paymentTime: {
        byReferenceKind: paymentTimeByReferenceKind,
        missing: {
          count: paymentTimeMissingTotal,
          reasons: [
            { reason: "missing_payment_date", count: missingPaymentDate },
            { reason: "missing_reference_date", count: missingReferenceDate },
            { reason: "missing_both", count: missingBoth },
          ],
        },
        examples: Array.isArray(paymentTimeExamples) ? paymentTimeExamples : [],
      },
      paymentTerms: {
        rows: Array.isArray(paymentTermsRows) ? paymentTermsRows : [],
        unmapped: {
          rawValues: unmappedRawValues,
          count: unmappedCount,
        },
      },
      smallBusiness: {
        counts: {
          true: sbTrueCount,
          false: sbFalseCount,
          unknown: sbUnknownCount,
        },
        notes: [
          "Small business status is required for SB-only metrics. Upload SBI results to populate is_small_business.",
        ],
      },
      canonical: {
        missingByField,
        populationDefinition: {
          includedRule:
            "trade_credit_payment===true && excluded_trade_credit_payment!==true && exclude_from_metrics!==true (and meta.rules.exclude!==true)",
        },
      },
    };

    const actions = {
      downloads: [
        {
          key: "unmapped_payment_terms",
          label: "Download unmapped payment terms (CSV)",
          enabled: unmappedCount > 0,
          count: unmappedCount,
        },
        {
          key: "rows_missing_payment_time_reference",
          label: "Download rows missing payment time reference (CSV)",
          enabled: missingReferenceDate > 0 || missingBoth > 0,
          count: (missingReferenceDate || 0) + (missingBoth || 0),
        },
      ],
    };

    await t.commit();

    return { meta, summary, gates, sections, actions };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {
        // ignore
      }
    }
    throw err;
  }
}

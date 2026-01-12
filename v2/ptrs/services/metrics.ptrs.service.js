const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

const {
  PTRS_CANONICAL_CONTRACT,
} = require("@/v2/ptrs/contracts/ptrs.canonical.contract");

module.exports = {
  getMetrics,
  updateMetricsDraft,
};

// -------------------------
// Helpers
// -------------------------

function isExcludedRow(stageRow) {
  const data = stageRow?.data || {};
  if (data?.exclude_from_metrics === true) return true;
  const meta = stageRow?.meta || {};
  return meta?.rules?.exclude === true;
}

function clampPct(value) {
  if (value == null) return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.max(0, Math.min(100, n));
}

function round2(n) {
  if (n == null) return null;
  const x = Number(n);
  if (!Number.isFinite(x)) return null;
  return Math.round(x * 100) / 100;
}

function percentile(sortedNums, p) {
  // p in [0,1]. Uses linear interpolation between closest ranks.
  if (!sortedNums.length) return null;
  if (p <= 0) return sortedNums[0];
  if (p >= 1) return sortedNums[sortedNums.length - 1];

  const idx = (sortedNums.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sortedNums[lo];

  const w = idx - lo;
  return sortedNums[lo] * (1 - w) + sortedNums[hi] * w;
}

function modeInt(values) {
  const freq = new Map();
  for (const v of values) {
    if (v == null) continue;
    const n = Number(v);
    if (!Number.isFinite(n)) continue;
    const k = Math.round(n);
    freq.set(k, (freq.get(k) || 0) + 1);
  }

  let best = null;
  let bestCount = 0;

  for (const [k, c] of freq.entries()) {
    if (c > bestCount) {
      best = k;
      bestCount = c;
    }
  }

  return best;
}

function makeMissingInputs(declarations) {
  const missing = [];

  const requiredBooleans = [
    "supplyChainFinanceOffered",
    "procurementFeesCharged",
    "smallBusinessPaymentObligations",
  ];

  for (const key of requiredBooleans) {
    if (declarations?.[key] == null) {
      missing.push({ field: `declarations.${key}`, severity: "warning" });
    }
  }

  // Comments are not required for MVP.

  return missing;
}

function safeText(value) {
  if (value == null) return "";
  return String(value);
}

// -------------------------
// Service entry points
// -------------------------

async function getMetrics({ customerId, ptrsId, userId = null }) {
  return computeReportPreview({ customerId, ptrsId, userId, mode: "read" });
}

async function updateMetricsDraft({
  customerId,
  ptrsId,
  userId = null,
  patch,
}) {
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

    const current = ptrs.reportPreviewDraft || {};

    // Only allow known keys at the top-level, to avoid random garbage.
    const next = {
      supplyChainFinanceOffered:
        patch?.supplyChainFinanceOffered ??
        current.supplyChainFinanceOffered ??
        null,
      procurementFeesCharged:
        patch?.procurementFeesCharged ?? current.procurementFeesCharged ?? null,
      smallBusinessPaymentObligations:
        patch?.smallBusinessPaymentObligations ??
        current.smallBusinessPaymentObligations ??
        null,
      anzsicSubdivision:
        patch?.anzsicSubdivision ?? current.anzsicSubdivision ?? null,
      industryDivision:
        patch?.industryDivision ?? current.industryDivision ?? null,
      reportComments: safeText(
        patch?.reportComments ?? current.reportComments ?? ""
      ),
      descriptionOfChanges: safeText(
        patch?.descriptionOfChanges ?? current.descriptionOfChanges ?? ""
      ),
      revisedReport: patch?.revisedReport ?? current.revisedReport ?? false,
      redactedReport: patch?.redactedReport ?? current.redactedReport ?? false,
      updatedBy: userId || null,
      updatedAt: new Date().toISOString(),
    };

    await db.Ptrs.update(
      { reportPreviewDraft: next, updatedBy: userId || null },
      { where: { id: ptrsId, customerId }, transaction: t }
    );

    await t.commit();

    // Return the full preview after update so the FE can render a single source of truth.
    return computeReportPreview({ customerId, ptrsId, userId, mode: "read" });
  } catch (err) {
    try {
      await t.rollback();
    } catch (_) {
      // ignore
    }
    throw err;
  }
}

async function computeReportPreview({ customerId, ptrsId, userId, mode }) {
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

    const draft = ptrs.reportPreviewDraft || {};

    // -------------------------
    // Compute from dataset (best effort, based on existing stage row schema)
    // -------------------------

    const basedOnRows = [];

    // Track total number of non-excluded staged rows considered for metrics gating
    let stageRowCount = 0;

    // Totals across all non-excluded rows
    let totalCount = 0;
    let totalValue = 0;

    // Small Business subset
    let sbCount = 0;
    let sbValue = 0;

    // Bands (for SB only, because that’s what’s commonly reported; adjust later if needed)
    let sbBand0to30Count = 0;
    let sbBand31to60Count = 0;
    let sbBandOver60Count = 0;

    let sbBand0to30Value = 0;
    let sbBand31to60Value = 0;
    let sbBandOver60Value = 0;

    // Payment days list (SB)
    const sbPaymentDays = [];

    // Term days list (all rows) to compute mode/min/max
    const termDaysAll = [];

    // % SB within terms
    let sbWithinTermsKnownCount = 0;
    let sbWithinTermsYesCount = 0;

    // Data availability flags
    let missingTermDaysCount = 0;
    let missingSbFlagCount = 0;
    let missingDatesCount = 0;
    let missingAmountCount = 0;
    // Track missing canonical flags for trade credit gating
    let missingTradeCreditFlagCount = 0;
    let missingExcludedTradeCreditFlagCount = 0;

    for (const r of stageRows) {
      if (isExcludedRow(r)) continue;

      const data = r?.data || {};

      // Increment total staged row count for all non-excluded rows
      stageRowCount += 1;

      // Canonical-only inputs
      const tradeCredit = data?.trade_credit_payment === true;
      const excludedTradeCredit = data?.excluded_trade_credit_payment === true;
      const isSmallBusiness = data?.is_small_business;

      // Check for missing canonical trade credit flags (must be explicit booleans)
      const tradeCreditRaw = data?.trade_credit_payment;
      const excludedTradeCreditRaw = data?.excluded_trade_credit_payment;

      if (tradeCreditRaw !== true && tradeCreditRaw !== false) {
        missingTradeCreditFlagCount += 1;
      }

      if (excludedTradeCreditRaw !== true && excludedTradeCreditRaw !== false) {
        missingExcludedTradeCreditFlagCount += 1;
      }

      // Totals are based on trade credit rows only
      if (!tradeCredit || excludedTradeCredit) {
        continue;
      }

      const amountRaw = data?.payment_amount;
      const amount =
        amountRaw == null || amountRaw === "" ? null : Number(amountRaw);

      totalCount += 1;

      if (amount == null || !Number.isFinite(amount)) {
        missingAmountCount += 1;
      } else {
        totalValue += Math.abs(amount);
      }

      if (isSmallBusiness == null) {
        missingSbFlagCount += 1;
      }

      // Only compute SB metrics when SB flag is true.
      if (isSmallBusiness === true) {
        sbCount += 1;
        if (amount != null && Number.isFinite(amount))
          sbValue += Math.abs(amount);

        const paymentTimeDaysRaw = data?.payment_time_days;
        const paymentTimeDays =
          paymentTimeDaysRaw == null || paymentTimeDaysRaw === ""
            ? null
            : Number(paymentTimeDaysRaw);

        const termDaysRaw = data?.payment_term_days;
        const termDays =
          termDaysRaw == null || termDaysRaw === ""
            ? null
            : Number(termDaysRaw);

        if (termDays == null || !Number.isFinite(termDays)) {
          missingTermDaysCount += 1;
        } else {
          termDaysAll.push(Math.round(termDays));
        }

        if (paymentTimeDays == null || !Number.isFinite(paymentTimeDays)) {
          missingDatesCount += 1;
        } else {
          const pt = Math.max(0, Math.round(paymentTimeDays));
          sbPaymentDays.push(pt);

          if (pt <= 30) {
            sbBand0to30Count += 1;
            if (amount != null && Number.isFinite(amount))
              sbBand0to30Value += Math.abs(amount);
          } else if (pt <= 60) {
            sbBand31to60Count += 1;
            if (amount != null && Number.isFinite(amount))
              sbBand31to60Value += Math.abs(amount);
          } else {
            sbBandOver60Count += 1;
            if (amount != null && Number.isFinite(amount))
              sbBandOver60Value += Math.abs(amount);
          }

          if (termDays != null && Number.isFinite(termDays)) {
            sbWithinTermsKnownCount += 1;
            if (pt <= Math.round(termDays)) sbWithinTermsYesCount += 1;
          }
        }
      }
    }

    // Canonical-mode quality gate (non-blocking navigation):
    const canonicalQuality = {
      blocked: false,
      missing: [],
    };

    // Add missing trade credit flag counts (block gating)
    if (missingTradeCreditFlagCount > 0)
      canonicalQuality.missing.push({
        field: "trade_credit_payment",
        count: missingTradeCreditFlagCount,
      });

    if (missingExcludedTradeCreditFlagCount > 0)
      canonicalQuality.missing.push({
        field: "excluded_trade_credit_payment",
        count: missingExcludedTradeCreditFlagCount,
      });

    // For SB metrics, require: is_small_business, payment_time_days, payment_term_days, payment_amount
    if (missingSbFlagCount > 0)
      canonicalQuality.missing.push({
        field: "is_small_business",
        count: missingSbFlagCount,
      });
    if (missingDatesCount > 0)
      canonicalQuality.missing.push({
        field: "payment_time_days",
        count: missingDatesCount,
      });
    if (missingTermDaysCount > 0)
      canonicalQuality.missing.push({
        field: "payment_term_days",
        count: missingTermDaysCount,
      });
    if (missingAmountCount > 0)
      canonicalQuality.missing.push({
        field: "payment_amount",
        count: missingAmountCount,
      });

    // Block if any canonical requirements are missing and we have rows that should be measurable.
    // - If there are staged rows but none qualify as trade credit, the most likely cause is that
    //   trade_credit_payment / excluded_trade_credit_payment weren't mapped.
    if (stageRowCount > 0 && totalCount === 0) {
      canonicalQuality.blocked = true;
    }

    if (canonicalQuality.missing.length > 0) {
      // Block if there are SB rows expected (sbCount > 0) OR trade credit rows are absent (handled above).
      if (sbCount > 0) canonicalQuality.blocked = true;
    }

    const sortedSbDays = sbPaymentDays.slice().sort((a, b) => a - b);

    const avgDays =
      !canonicalQuality.blocked && sortedSbDays.length > 0
        ? sortedSbDays.reduce((acc, x) => acc + x, 0) / sortedSbDays.length
        : null;

    const medianDays =
      !canonicalQuality.blocked && sortedSbDays.length > 0
        ? percentile(sortedSbDays, 0.5)
        : null;

    const p80Days =
      !canonicalQuality.blocked && sortedSbDays.length > 0
        ? percentile(sortedSbDays, 0.8)
        : null;

    const p95Days =
      !canonicalQuality.blocked && sortedSbDays.length > 0
        ? percentile(sortedSbDays, 0.95)
        : null;

    const commonTermMode = !canonicalQuality.blocked
      ? modeInt(termDaysAll)
      : null;
    const termMin =
      !canonicalQuality.blocked && termDaysAll.length
        ? Math.min(...termDaysAll)
        : null;
    const termMax =
      !canonicalQuality.blocked && termDaysAll.length
        ? Math.max(...termDaysAll)
        : null;

    const sbWithinTermsPct =
      !canonicalQuality.blocked && sbWithinTermsKnownCount > 0
        ? (sbWithinTermsYesCount / sbWithinTermsKnownCount) * 100
        : null;

    const payments0to30Pct =
      !canonicalQuality.blocked && sbCount > 0
        ? (sbBand0to30Count / sbCount) * 100
        : null;

    const payments31to60Pct =
      !canonicalQuality.blocked && sbCount > 0
        ? (sbBand31to60Count / sbCount) * 100
        : null;

    const paymentsOver60Pct =
      !canonicalQuality.blocked && sbCount > 0
        ? (sbBandOver60Count / sbCount) * 100
        : null;

    const sbTradeCreditPaymentsPct =
      !canonicalQuality.blocked && totalValue > 0
        ? (sbValue / totalValue) * 100
        : null;
    // logger.logEvent("info", "PTRS v2 metrics debug: SB trade credit %", {
    //   action: "PtrsV2MetricsSbTradeCreditDebug",
    //   ptrsId,
    //   customerId,
    //   totals: {
    //     totalCount,
    //     sbCount,
    //     totalValue,
    //     sbValue,
    //     missingAmountCount,
    //   },
    //   rawAmountSigns: {
    //     rawNegativeAmountCount,
    //     rawPositiveAmountCount,
    //     rawZeroAmountCount,
    //   },
    //   computed: {
    //     sbTradeCreditPaymentsPct,
    //     rounded: round2(sbTradeCreditPaymentsPct),
    //   },
    //   samples: amountSample,
    // });

    // Peppol: we don’t have a reliable field yet.
    const peppolEnabledSbProcurementPct = null;

    // -------------------------
    // Compose regulator-shaped preview
    // -------------------------

    const header = {
      reportId: ptrs.id,
      businessName: ptrs.reportingEntityName || null,
      abn: ptrs?.meta?.abn || null,
      acn: ptrs?.meta?.acn || null,
      arbn: ptrs?.meta?.arbn || null,
      type: "Standard",
      reportingPeriodStartDate: ptrs.periodStart || null,
      reportingPeriodEndDate: ptrs.periodEnd || null,
      revisedReport: Boolean(draft.revisedReport),
      redactedReport: Boolean(draft.redactedReport),
      submittedDate: null,
    };

    const declarations = {
      supplyChainFinanceOffered: draft.supplyChainFinanceOffered ?? null,
      procurementFeesCharged: draft.procurementFeesCharged ?? null,
      smallBusinessPaymentObligations:
        draft.smallBusinessPaymentObligations ?? null,
      anzsicSubdivision: draft.anzsicSubdivision ?? null,
      industryDivision: draft.industryDivision ?? null,
      reportComments: safeText(draft.reportComments),
      descriptionOfChanges: safeText(draft.descriptionOfChanges),
    };

    const computed = {
      commonPaymentTermsDays: commonTermMode,
      commonPaymentTermMinimum: termMin,
      commonPaymentTermMaximum: termMax,

      forecastPaymentTerm: commonTermMode,
      forecastMinimumPaymentTerm: termMin,
      forecastMaximumPaymentTerm: termMax,

      receivableTermsComparedToCommonPaymentTerm: "Unknown",

      percentageOfSbInvoicesPaidWithinPaymentTerm: round2(sbWithinTermsPct),

      averagePaymentTimeDays: round2(avgDays),
      medianPaymentTimeDays: round2(medianDays),
      p80PaymentTimeDays: round2(p80Days),
      p95PaymentTimeDays: round2(p95Days),

      payments30DaysOrLessPct: round2(payments0to30Pct),
      payments31To60DaysPct: round2(payments31to60Pct),
      paymentsMoreThan60DaysPct: round2(paymentsOver60Pct),

      percentageOfSmallBusinessTradeCreditPayments: round2(
        sbTradeCreditPaymentsPct
      ),
      percentagePeppolEnabledSmallBusinessProcurement:
        peppolEnabledSbProcurementPct,
    };

    const quality = {
      mode,
      stageRowCount,
      basedOnRowCount: totalCount,
      sbRowCount: sbCount,
      missingInputs: makeMissingInputs(declarations),
      canonical: canonicalQuality,
      notes: [],
      dataSignals: {
        missingTermDaysCount,
        missingSbFlagCount,
        missingDatesCount,
        missingAmountCount,
      },
    };

    if (missingTermDaysCount > 0) {
      quality.notes.push(
        "Some rows are missing payment term days; within-terms and term metrics may be incomplete."
      );
    }

    if (missingSbFlagCount > 0) {
      quality.notes.push(
        "Some rows are missing small business status; SB metrics are computed only for rows where is_small_business is true."
      );
    }

    if (peppolEnabledSbProcurementPct == null) {
      quality.notes.push(
        "Peppol-enabled small business procurement is not currently captured in the dataset (metric returned as null)."
      );
    }

    if (canonicalQuality.blocked) {
      quality.notes.push(
        "Metrics are blocked until required canonical fields are populated (see quality.canonical.missing). In particular, ensure trade_credit_payment and excluded_trade_credit_payment are mapped so the system knows which rows belong to the trade credit population."
      );
    }

    await t.commit();

    return {
      header,
      declarations,
      computed,
      quality,
    };
  } catch (err) {
    try {
      await t.rollback();
    } catch (_) {
      // ignore
    }
    throw err;
  }
}

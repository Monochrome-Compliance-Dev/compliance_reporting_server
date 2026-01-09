const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

module.exports = {
  getMetrics,
  updateMetricsDraft,
};

// -------------------------
// Helpers
// -------------------------

function isExcludedRow(stageRow) {
  const meta = stageRow?.meta || {};
  return meta?.rules?.exclude === true;
}

function parseAusDate(value) {
  // Expecting dd/mm/yyyy. Returns Date (UTC) or null.
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

  const d = new Date(Date.UTC(yyyy, mm - 1, dd));

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
  if (value == null) return null;
  const s = String(value).trim();
  if (!s) return null;

  // Values can arrive with accounting sign conventions (e.g. negatives for payments).
  // For PTRS value-based metrics we treat payments as magnitudes.
  const cleaned = s.replace(/,/g, "");
  const n = Number(cleaned);
  if (!Number.isFinite(n)) return null;

  return Math.abs(n);
}

function toIsoDateOnly(d) {
  if (!(d instanceof Date)) return null;
  // d is UTC-based above
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
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

function getTermDays(data) {
  // Best-effort: support a few likely field names.
  const candidates = [
    data?.payment_term_days,
    data?.payment_terms_days,
    data?.payment_terms,
    data?.term_days,
    data?.agreed_payment_term_days,
  ];

  for (const c of candidates) {
    if (c == null) continue;
    const s = String(c).trim();
    if (!s) continue;
    const n = Number(s);
    if (Number.isFinite(n) && n >= 0) return Math.round(n);

    // Try extracting first integer (e.g. "30 days")
    const m = /(\d+)/.exec(s);
    if (m) {
      const n2 = Number(m[1]);
      if (Number.isFinite(n2) && n2 >= 0) return Math.round(n2);
    }
  }

  return null;
}

function computePaymentDays(invoiceDate, paymentDate) {
  if (!(invoiceDate instanceof Date) || !(paymentDate instanceof Date))
    return null;
  const ms = paymentDate.getTime() - invoiceDate.getTime();
  const days = ms / (1000 * 60 * 60 * 24);
  if (!Number.isFinite(days)) return null;
  // Payment days should be whole-number days for reporting.
  return Math.round(days);
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

    let minInvoiceDate = null;
    let maxInvoiceDate = null;

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

    // Debug signals for SB trade credit % (value-based)
    let rawNegativeAmountCount = 0;
    let rawPositiveAmountCount = 0;
    let rawZeroAmountCount = 0;
    const amountSample = [];

    for (const r of stageRows) {
      if (isExcludedRow(r)) continue;

      const data = r?.data || {};

      const invoiceDate = parseAusDate(data?.invoice_issue_date);
      const paymentDate = parseAusDate(data?.payment_date);
      const isSmallBusiness = data?.is_small_business;
      const amount = parseMoney(data?.payment_amount);
      // Debug: capture raw sign distribution and a few samples
      const rawAmt = data?.payment_amount;

      if (rawAmt != null && amountSample.length < 8) {
        amountSample.push({
          rowNo: r.rowNo,
          raw: String(rawAmt),
          parsedAbs: amount,
          isSmallBusiness,
        });
      }

      if (rawAmt != null) {
        const cleanedRaw = String(rawAmt).trim().replace(/,/g, "");
        const rawNum = Number(cleanedRaw);
        if (Number.isFinite(rawNum)) {
          if (rawNum < 0) rawNegativeAmountCount += 1;
          else if (rawNum > 0) rawPositiveAmountCount += 1;
          else rawZeroAmountCount += 1;
        }
      }

      totalCount += 1;
      if (amount != null) totalValue += amount;
      else missingAmountCount += 1;

      if (invoiceDate) {
        const ts = invoiceDate.getTime();
        if (!minInvoiceDate || ts < minInvoiceDate.getTime())
          minInvoiceDate = invoiceDate;
        if (!maxInvoiceDate || ts > maxInvoiceDate.getTime())
          maxInvoiceDate = invoiceDate;
      }

      const termDays = getTermDays(data);
      if (termDays != null) termDaysAll.push(termDays);
      else missingTermDaysCount += 1;

      if (isSmallBusiness == null) {
        missingSbFlagCount += 1;
      }

      // Only compute SB metrics when SB flag is true.
      if (isSmallBusiness === true) {
        sbCount += 1;
        if (amount != null) sbValue += amount;

        if (!invoiceDate || !paymentDate) {
          missingDatesCount += 1;
        }

        const paymentDays = computePaymentDays(invoiceDate, paymentDate);
        if (paymentDays != null) {
          sbPaymentDays.push(paymentDays);

          if (paymentDays <= 30) {
            sbBand0to30Count += 1;
            if (amount != null) sbBand0to30Value += amount;
          } else if (paymentDays <= 60) {
            sbBand31to60Count += 1;
            if (amount != null) sbBand31to60Value += amount;
          } else {
            sbBandOver60Count += 1;
            if (amount != null) sbBandOver60Value += amount;
          }
        }

        if (termDays != null && paymentDays != null) {
          sbWithinTermsKnownCount += 1;
          if (paymentDays <= termDays) sbWithinTermsYesCount += 1;
        }
      }

      basedOnRows.push(r);
    }

    const sortedSbDays = sbPaymentDays.slice().sort((a, b) => a - b);

    const avgDays =
      sortedSbDays.length > 0
        ? sortedSbDays.reduce((acc, x) => acc + x, 0) / sortedSbDays.length
        : null;

    const medianDays =
      sortedSbDays.length > 0 ? percentile(sortedSbDays, 0.5) : null;

    const p80Days =
      sortedSbDays.length > 0 ? percentile(sortedSbDays, 0.8) : null;

    const p95Days =
      sortedSbDays.length > 0 ? percentile(sortedSbDays, 0.95) : null;

    const commonTermMode = modeInt(termDaysAll);
    const termMin = termDaysAll.length ? Math.min(...termDaysAll) : null;
    const termMax = termDaysAll.length ? Math.max(...termDaysAll) : null;

    const sbWithinTermsPct =
      sbWithinTermsKnownCount > 0
        ? (sbWithinTermsYesCount / sbWithinTermsKnownCount) * 100
        : null;

    const payments0to30Pct =
      sbCount > 0 ? (sbBand0to30Count / sbCount) * 100 : null;

    const payments31to60Pct =
      sbCount > 0 ? (sbBand31to60Count / sbCount) * 100 : null;

    const paymentsOver60Pct =
      sbCount > 0 ? (sbBandOver60Count / sbCount) * 100 : null;

    const sbTradeCreditPaymentsPct =
      totalValue > 0 ? (sbValue / totalValue) * 100 : null;
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
      basedOnRowCount: totalCount,
      sbRowCount: sbCount,
      actualInvoiceDateRange: {
        min: minInvoiceDate ? toIsoDateOnly(minInvoiceDate) : null,
        max: maxInvoiceDate ? toIsoDateOnly(maxInvoiceDate) : null,
      },
      missingInputs: makeMissingInputs(declarations),
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

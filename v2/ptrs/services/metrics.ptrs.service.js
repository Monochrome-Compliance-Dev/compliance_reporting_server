const db = require("@/db/database");
// const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

// const {
//   PTRS_CANONICAL_CONTRACT,
// } = require("@/v2/ptrs/contracts/ptrs.canonical.contract");

module.exports = {
  getMetrics,
  updateMetricsDraft,
};

// -------------------------
// SQL aggregate helpers for metrics
// -------------------------

function qTableName(model) {
  const tn = model.getTableName();
  if (typeof tn === "string") return `"${tn}"`;
  // Sequelize may return { tableName, schema }
  const schema = tn.schema ? `"${tn.schema}".` : "";
  const tableName = tn.tableName ? `"${tn.tableName}"` : "";
  return `${schema}${tableName}`;
}

async function fetchStageMetricsAggs({ t, customerId, ptrsId }) {
  // NOTE: This query intentionally does not return raw rows.
  // It computes only what the dashboard/metrics preview needs.

  const stageTbl = qTableName(db.PtrsStageRow);

  const sql = `
    WITH base AS (
      SELECT
        data,
        meta,
        -- Exclusion flags
        COALESCE((data->>'exclude_from_metrics')::boolean, false) AS exclude_from_metrics,
        COALESCE((meta->'rules'->>'exclude')::boolean, false) AS rules_exclude,

        -- Canonical flags (safe parse: only true/false strings become booleans)
        CASE
          WHEN lower(data->>'trade_credit_payment') IN ('true','false') THEN (data->>'trade_credit_payment')::boolean
          ELSE NULL
        END AS trade_credit_payment,

        CASE
          WHEN lower(data->>'excluded_trade_credit_payment') IN ('true','false') THEN (data->>'excluded_trade_credit_payment')::boolean
          ELSE NULL
        END AS excluded_trade_credit_payment,

        CASE
          WHEN lower(data->>'is_small_business') IN ('true','false') THEN (data->>'is_small_business')::boolean
          ELSE NULL
        END AS is_small_business,

        -- Safe numeric parses
        CASE
          WHEN (data->>'payment_amount') ~ '^-?\\d+(\\.\\d+)?$' THEN (data->>'payment_amount')::numeric
          ELSE NULL
        END AS payment_amount_num,

        CASE
          WHEN (data->>'payment_time_days') ~ '^-?\\d+(\\.\\d+)?$' THEN (data->>'payment_time_days')::numeric
          ELSE NULL
        END AS payment_time_days_num,

        CASE
          WHEN (data->>'payment_term_days') ~ '^-?\\d+(\\.\\d+)?$' THEN (data->>'payment_term_days')::numeric
          ELSE NULL
        END AS payment_term_days_num

      FROM ${stageTbl}
      WHERE "customerId" = :customerId
        AND "ptrsId" = :ptrsId
        AND "deletedAt" IS NULL
    ),

    non_excluded AS (
      SELECT *
      FROM base
      WHERE NOT (exclude_from_metrics OR rules_exclude)
    ),

    population AS (
      -- Trade credit population (match existing JS logic):
      -- include when trade_credit_payment is true AND excluded_trade_credit_payment is not true (false or null)
      SELECT *
      FROM non_excluded
      WHERE trade_credit_payment IS TRUE
        AND COALESCE(excluded_trade_credit_payment, false) IS FALSE
    ),

    sb AS (
      SELECT
        *,
        GREATEST(0, ROUND(payment_time_days_num)::int) AS payment_time_days_int,
        GREATEST(0, ROUND(payment_term_days_num)::int) AS payment_term_days_int
      FROM population
      WHERE is_small_business IS TRUE
    )

    SELECT
      -- Gating counts
      (SELECT COUNT(*)::int FROM non_excluded) AS "stageRowCount",

      -- Canonical missing flags (non-excluded rows)
      (SELECT COUNT(*)::int FROM non_excluded WHERE trade_credit_payment IS NULL) AS "missingTradeCreditFlagCount",
      (SELECT COUNT(*)::int FROM non_excluded WHERE excluded_trade_credit_payment IS NULL) AS "missingExcludedTradeCreditFlagCount",

      -- Population totals
      (SELECT COUNT(*)::int FROM population) AS "totalCount",
      (SELECT COALESCE(SUM(ABS(payment_amount_num)),0)::numeric FROM population WHERE payment_amount_num IS NOT NULL) AS "totalValue",
      (SELECT COUNT(*)::int FROM population WHERE payment_amount_num IS NULL) AS "missingAmountCount",

      -- SB totals
      (SELECT COUNT(*)::int FROM population WHERE is_small_business IS TRUE) AS "sbCount",
      (SELECT COALESCE(SUM(ABS(payment_amount_num)),0)::numeric FROM population WHERE is_small_business IS TRUE AND payment_amount_num IS NOT NULL) AS "sbValue",
      (SELECT COUNT(*)::int FROM population WHERE is_small_business IS NULL) AS "missingSbFlagCount",

      -- Term day availability (population)
      (SELECT COUNT(*)::int FROM population WHERE payment_term_days_num IS NULL) AS "missingTermDaysCount",

      -- Missing payment time (SB only)
      (SELECT COUNT(*)::int FROM population WHERE is_small_business IS TRUE AND payment_time_days_num IS NULL) AS "missingDatesCount",

      -- SB payment time bands (counts)
      (SELECT COUNT(*)::int FROM sb WHERE payment_time_days_num IS NOT NULL AND payment_time_days_int <= 30) AS "sbBand0to30Count",
      (SELECT COUNT(*)::int FROM sb WHERE payment_time_days_num IS NOT NULL AND payment_time_days_int > 30 AND payment_time_days_int <= 60) AS "sbBand31to60Count",
      (SELECT COUNT(*)::int FROM sb WHERE payment_time_days_num IS NOT NULL AND payment_time_days_int > 60) AS "sbBandOver60Count",

      -- SB payment time bands (values)
      (SELECT COALESCE(SUM(ABS(payment_amount_num)),0)::numeric FROM sb WHERE payment_amount_num IS NOT NULL AND payment_time_days_num IS NOT NULL AND payment_time_days_int <= 30) AS "sbBand0to30Value",
      (SELECT COALESCE(SUM(ABS(payment_amount_num)),0)::numeric FROM sb WHERE payment_amount_num IS NOT NULL AND payment_time_days_num IS NOT NULL AND payment_time_days_int > 30 AND payment_time_days_int <= 60) AS "sbBand31to60Value",
      (SELECT COALESCE(SUM(ABS(payment_amount_num)),0)::numeric FROM sb WHERE payment_amount_num IS NOT NULL AND payment_time_days_num IS NOT NULL AND payment_time_days_int > 60) AS "sbBandOver60Value",

      -- SB within terms
      (SELECT COUNT(*)::int FROM sb WHERE payment_time_days_num IS NOT NULL AND payment_term_days_num IS NOT NULL) AS "sbWithinTermsKnownCount",
      (SELECT COUNT(*)::int FROM sb WHERE payment_time_days_num IS NOT NULL AND payment_term_days_num IS NOT NULL AND payment_time_days_int <= payment_term_days_int) AS "sbWithinTermsYesCount",

      -- SB payment time distribution stats
      (SELECT AVG(payment_time_days_int)::numeric FROM sb WHERE payment_time_days_num IS NOT NULL) AS "avgDays",
      (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY payment_time_days_int)::numeric FROM sb WHERE payment_time_days_num IS NOT NULL) AS "medianDays",
      (SELECT percentile_cont(0.8) WITHIN GROUP (ORDER BY payment_time_days_int)::numeric FROM sb WHERE payment_time_days_num IS NOT NULL) AS "p80Days",
      (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY payment_time_days_int)::numeric FROM sb WHERE payment_time_days_num IS NOT NULL) AS "p95Days",

      -- Term min/max/mode (population)
      (SELECT MIN(GREATEST(0, ROUND(payment_term_days_num)::int))::int FROM population WHERE payment_term_days_num IS NOT NULL) AS "termMin",
      (SELECT MAX(GREATEST(0, ROUND(payment_term_days_num)::int))::int FROM population WHERE payment_term_days_num IS NOT NULL) AS "termMax",
      (SELECT x.term::int
         FROM (
           SELECT GREATEST(0, ROUND(payment_term_days_num)::int) AS term, COUNT(*) AS c
           FROM population
           WHERE payment_term_days_num IS NOT NULL
           GROUP BY 1
           ORDER BY c DESC
           LIMIT 1
         ) x
      ) AS "commonTermMode";
  `;

  const [rows] = await db.sequelize.query(sql, {
    replacements: { customerId, ptrsId },
    transaction: t,
  });

  return rows && rows[0] ? rows[0] : null;
}

// -------------------------
// Helpers
// -------------------------

// function isExcludedRow(stageRow) {
//   const data = stageRow?.data || {};
//   if (data?.exclude_from_metrics === true) return true;
//   const meta = stageRow?.meta || {};
//   return meta?.rules?.exclude === true;
// }

// function clampPct(value) {
//   if (value == null) return null;
//   const n = Number(value);
//   if (!Number.isFinite(n)) return null;
//   return Math.max(0, Math.min(100, n));
// }

function round2(n) {
  if (n == null) return null;
  const x = Number(n);
  if (!Number.isFinite(x)) return null;
  return Math.round(x * 100) / 100;
}

// function percentile(sortedNums, p) {
//   // p in [0,1]. Uses linear interpolation between closest ranks.
//   if (!sortedNums.length) return null;
//   if (p <= 0) return sortedNums[0];
//   if (p >= 1) return sortedNums[sortedNums.length - 1];

//   const idx = (sortedNums.length - 1) * p;
//   const lo = Math.floor(idx);
//   const hi = Math.ceil(idx);
//   if (lo === hi) return sortedNums[lo];

//   const w = idx - lo;
//   return sortedNums[lo] * (1 - w) + sortedNums[hi] * w;
// }

// function modeInt(values) {
//   const freq = new Map();
//   for (const v of values) {
//     if (v == null) continue;
//     const n = Number(v);
//     if (!Number.isFinite(n)) continue;
//     const k = Math.round(n);
//     freq.set(k, (freq.get(k) || 0) + 1);
//   }

//   let best = null;
//   let bestCount = 0;

//   for (const [k, c] of freq.entries()) {
//     if (c > bestCount) {
//       best = k;
//       bestCount = c;
//     }
//   }

//   return best;
// }

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
        patch?.reportComments ?? current.reportComments ?? "",
      ),
      descriptionOfChanges: safeText(
        patch?.descriptionOfChanges ?? current.descriptionOfChanges ?? "",
      ),
      revisedReport: patch?.revisedReport ?? current.revisedReport ?? false,
      redactedReport: patch?.redactedReport ?? current.redactedReport ?? false,
      updatedBy: userId || null,
      updatedAt: new Date().toISOString(),
    };

    await db.Ptrs.update(
      { reportPreviewDraft: next, updatedBy: userId || null },
      { where: { id: ptrsId, customerId }, transaction: t },
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

    const draft = ptrs.reportPreviewDraft || {};

    const aggs = await fetchStageMetricsAggs({ t, customerId, ptrsId });

    // Defaults when there are no rows yet
    const stageRowCount = aggs?.stageRowCount || 0;

    const missingTradeCreditFlagCount = aggs?.missingTradeCreditFlagCount || 0;
    const missingExcludedTradeCreditFlagCount =
      aggs?.missingExcludedTradeCreditFlagCount || 0;

    const totalCount = aggs?.totalCount || 0;
    const totalValue = Number(aggs?.totalValue || 0);
    const missingAmountCount = aggs?.missingAmountCount || 0;

    const sbCount = aggs?.sbCount || 0;
    const sbValue = Number(aggs?.sbValue || 0);
    const missingSbFlagCount = aggs?.missingSbFlagCount || 0;

    const missingTermDaysCount = aggs?.missingTermDaysCount || 0;
    const missingDatesCount = aggs?.missingDatesCount || 0;

    const sbBand0to30Count = aggs?.sbBand0to30Count || 0;
    const sbBand31to60Count = aggs?.sbBand31to60Count || 0;
    const sbBandOver60Count = aggs?.sbBandOver60Count || 0;

    // const sbBand0to30Value = Number(aggs?.sbBand0to30Value || 0);
    // const sbBand31to60Value = Number(aggs?.sbBand31to60Value || 0);
    // const sbBandOver60Value = Number(aggs?.sbBandOver60Value || 0);

    const sbWithinTermsKnownCount = aggs?.sbWithinTermsKnownCount || 0;
    const sbWithinTermsYesCount = aggs?.sbWithinTermsYesCount || 0;

    const avgDays = aggs?.avgDays == null ? null : Number(aggs.avgDays);
    const medianDays =
      aggs?.medianDays == null ? null : Number(aggs.medianDays);
    const p80Days = aggs?.p80Days == null ? null : Number(aggs.p80Days);
    const p95Days = aggs?.p95Days == null ? null : Number(aggs.p95Days);

    const commonTermMode =
      aggs?.commonTermMode == null ? null : Number(aggs.commonTermMode);

    const termMinFinal = aggs?.termMin == null ? null : Number(aggs.termMin);
    const termMaxFinal = aggs?.termMax == null ? null : Number(aggs.termMax);

    // Canonical-mode quality gate
    // IMPORTANT:
    // - We only *block* metrics when we can't even define the trade credit population.
    // - Missing term days / SB flag / etc should degrade specific metrics, not blank everything.
    const canonicalQuality = {
      blocked: false,
      missing: [],
    };

    // Trade credit population definition requires these booleans to be explicit.
    if (missingTradeCreditFlagCount > 0) {
      canonicalQuality.missing.push({
        field: "trade_credit_payment",
        count: missingTradeCreditFlagCount,
      });
    }

    if (missingExcludedTradeCreditFlagCount > 0) {
      canonicalQuality.missing.push({
        field: "excluded_trade_credit_payment",
        count: missingExcludedTradeCreditFlagCount,
      });
    }

    // SB metrics quality signals (do NOT block)
    if (missingSbFlagCount > 0) {
      canonicalQuality.missing.push({
        field: "is_small_business",
        count: missingSbFlagCount,
      });
    }

    // Term days quality signal (do NOT block) – we can still compute payment-time stats without it.
    if (missingTermDaysCount > 0) {
      canonicalQuality.missing.push({
        field: "payment_term_days",
        count: missingTermDaysCount,
      });
    }

    // Payment time quality signal (do NOT block) – affected metrics will be null if we have no SB payment days.
    if (missingDatesCount > 0) {
      canonicalQuality.missing.push({
        field: "payment_time_days",
        count: missingDatesCount,
      });
    }

    // Amount quality signal (do NOT block) – percentage-of-value metrics will be null if totalValue is 0.
    if (missingAmountCount > 0) {
      canonicalQuality.missing.push({
        field: "payment_amount",
        count: missingAmountCount,
      });
    }

    // Block ONLY if we cannot define the trade credit population.
    // This happens when:
    // - the canonical booleans are not explicit for some rows, OR
    // - we have staged rows, but zero rows qualify as trade credit (likely unmapped flags).
    if (
      missingTradeCreditFlagCount > 0 ||
      missingExcludedTradeCreditFlagCount > 0 ||
      (stageRowCount > 0 && totalCount === 0)
    ) {
      canonicalQuality.blocked = true;
    }

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
      commonPaymentTermMinimum: termMinFinal,
      commonPaymentTermMaximum: termMaxFinal,

      forecastPaymentTerm: commonTermMode,
      forecastMinimumPaymentTerm: termMinFinal,
      forecastMaximumPaymentTerm: termMaxFinal,

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
        sbTradeCreditPaymentsPct,
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
        "Some rows are missing payment term days; within-terms and term metrics may be incomplete.",
      );
    }

    if (missingSbFlagCount > 0) {
      quality.notes.push(
        "Some rows are missing small business status; SB metrics are computed only for rows where is_small_business is true.",
      );
    }

    if (peppolEnabledSbProcurementPct == null) {
      quality.notes.push(
        "Peppol-enabled small business procurement is not currently captured in the dataset (metric returned as null).",
      );
    }

    if (canonicalQuality.blocked) {
      quality.notes.push(
        "Metrics are blocked because the trade credit population cannot be reliably determined (see quality.canonical.missing). Ensure trade_credit_payment and excluded_trade_credit_payment are explicitly mapped so the system knows which rows belong to the trade credit population.",
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

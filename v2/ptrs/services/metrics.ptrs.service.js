const db = require("@/db/database");
// const { logger } = require("@/helpers/logger");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  buildPaymentObservationsCte,
  getPaymentObservationReplacements,
} = require("./payment-observations.ptrs.service");

// const {
//   PTRS_CANONICAL_CONTRACT,
// } = require("@/v2/ptrs/contracts/ptrs.canonical.contract");

module.exports = {
  calculatePaymentTermMetricsFromFrequencies,
  calculateSmallBusinessTradeCreditPaymentsPct,
  fetchPaymentObservationMetricsAggs,
  getMetrics,
  updateMetricsDraft,
};

// -------------------------
// SQL aggregate helpers for metrics
// -------------------------

async function fetchPaymentObservationMetricsAggs({ t, customerId, ptrsId }) {
  // NOTE: This query intentionally does not return raw rows.
  // It computes only what the dashboard/metrics preview needs.

  const sql = `
    WITH ${buildPaymentObservationsCte()},
    base AS (
      SELECT
        data,
        meta,
        CASE
          WHEN NULLIF(
            regexp_replace(COALESCE(data->>'payer_entity_abn', ''), '\\D', '', 'g'),
            ''
          ) IS NOT NULL
            THEN 'abn:' || regexp_replace(
              COALESCE(data->>'payer_entity_abn', ''),
              '\\D',
              '',
              'g'
            )
          WHEN NULLIF(BTRIM(COALESCE(data->>'payer_entity_name', '')), '') IS NOT NULL
            THEN 'name:' || lower(
              regexp_replace(
                BTRIM(data->>'payer_entity_name'),
                '\\s+',
                ' ',
                'g'
              )
            )
          ELSE NULL
        END AS payer_entity_key,
        -- Exclusion flags
        COALESCE((data->>'exclude_from_metrics')::boolean, false) AS exclude_from_metrics,
        COALESCE((meta->'rules'->>'exclude')::boolean, false) AS rules_exclude,

        CASE
          WHEN lower(data->>'is_small_business') IN ('true','false') THEN (data->>'is_small_business')::boolean
          ELSE NULL
        END AS is_small_business,

        -- Safe numeric parses
        CASE
          WHEN REPLACE(data->>'payment_amount', ',', '') ~ '^-?\\d+(\\.\\d+)?$'
            THEN REPLACE(data->>'payment_amount', ',', '')::numeric
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

      FROM payment_observations
    ),

    non_excluded AS (
      SELECT *
      FROM base
      WHERE NOT (exclude_from_metrics OR rules_exclude)
    ),

    population AS (
      -- The derived relation has already established the reportable payment
      -- population. Raw Stage classification flags are not payment identity.
      SELECT *
      FROM non_excluded
    ),

    sb AS (
      SELECT
        *,
        GREATEST(0, ROUND(payment_time_days_num)::int) AS payment_time_days_int,
        GREATEST(0, ROUND(payment_term_days_num)::int) AS payment_term_days_int
      FROM population
      WHERE is_small_business IS TRUE
    ),

    sb_term_frequencies AS (
      SELECT
        payment_term_days_int AS term,
        COUNT(*)::int AS frequency
      FROM sb
      WHERE payment_term_days_num IS NOT NULL
      GROUP BY payment_term_days_int
    ),

    sb_entity_term_frequencies AS (
      SELECT
        payer_entity_key,
        payment_term_days_int AS term,
        COUNT(*)::int AS frequency
      FROM sb
      WHERE payer_entity_key IS NOT NULL
        AND payment_term_days_num IS NOT NULL
      GROUP BY payer_entity_key, payment_term_days_int
    )

    SELECT
      -- Gating counts
      (SELECT COUNT(*)::int FROM payment_observation_source_rows) AS "stageRowCount",
      (SELECT COUNT(*)::int FROM non_excluded) AS "paymentObservationCount",

      -- Population totals
      (SELECT COUNT(*)::int FROM population) AS "totalCount",
      (SELECT COALESCE(SUM(ABS(payment_amount_num)),0)::numeric FROM population WHERE payment_amount_num IS NOT NULL) AS "totalValue",
      (SELECT COALESCE(SUM(ABS("settlementPaymentAmount")),0)::numeric FROM payment_observation_settlement_groups WHERE "settlementPaymentAmount" IS NOT NULL) AS "tcpSettlementValue",
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

      -- Payment-term frequency aggregates. Node selects the deterministic
      -- overall and entity-level modes from these small, set-based results.
      (SELECT COALESCE(
        jsonb_agg(
          jsonb_build_object('term', term, 'count', frequency)
          ORDER BY term
        ),
        '[]'::jsonb
      ) FROM sb_term_frequencies) AS "sbTermFrequencies",
      (SELECT COALESCE(
        jsonb_agg(
          jsonb_build_object(
            'payerEntityKey', payer_entity_key,
            'term', term,
            'count', frequency
          )
          ORDER BY payer_entity_key, term
        ),
        '[]'::jsonb
      ) FROM sb_entity_term_frequencies) AS "sbEntityTermFrequencies";
  `;

  const [rows] = await db.sequelize.query(sql, {
    replacements: getPaymentObservationReplacements({ customerId, ptrsId }),
    transaction: t,
  });

  if (!rows || !rows[0]) return null;

  return {
    ...rows[0],
    ...calculatePaymentTermMetricsFromFrequencies({
      sbTermFrequencies: rows[0].sbTermFrequencies,
      sbEntityTermFrequencies: rows[0].sbEntityTermFrequencies,
    }),
  };
}

// -------------------------
// Helpers
// -------------------------

function normaliseFrequencyRows(value) {
  if (Array.isArray(value)) return value;
  if (typeof value !== "string") return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_) {
    return [];
  }
}

function selectDeterministicMode(rows) {
  let selectedTerm = null;
  let selectedCount = -1;

  for (const row of rows) {
    const term = Number(row?.term);
    const count = Number(row?.count);
    if (!Number.isInteger(term) || !Number.isFinite(count) || count <= 0) {
      continue;
    }

    if (
      count > selectedCount ||
      (count === selectedCount &&
        (selectedTerm == null || term < selectedTerm))
    ) {
      selectedTerm = term;
      selectedCount = count;
    }
  }

  return selectedTerm;
}

function calculatePaymentTermMetricsFromFrequencies({
  sbTermFrequencies,
  sbEntityTermFrequencies,
}) {
  const commonTermMode = selectDeterministicMode(
    normaliseFrequencyRows(sbTermFrequencies),
  );
  const byEntity = new Map();

  for (const row of normaliseFrequencyRows(sbEntityTermFrequencies)) {
    const payerEntityKey = String(row?.payerEntityKey || "").trim();
    if (!payerEntityKey) continue;
    if (!byEntity.has(payerEntityKey)) byEntity.set(payerEntityKey, []);
    byEntity.get(payerEntityKey).push(row);
  }

  const entityModes = Array.from(byEntity.values())
    .map(selectDeterministicMode)
    .filter((value) => value != null);

  return {
    commonTermMode,
    termMin: entityModes.length ? Math.min(...entityModes) : null,
    termMax: entityModes.length ? Math.max(...entityModes) : null,
  };
}

function calculateSmallBusinessTradeCreditPaymentsPct({
  sbValue,
  tcpSettlementValue,
  blocked = false,
}) {
  const numerator = Number(sbValue);
  const denominator = Number(tcpSettlementValue);
  if (
    blocked ||
    !Number.isFinite(numerator) ||
    !Number.isFinite(denominator) ||
    denominator <= 0
  ) {
    return null;
  }
  return (numerator / denominator) * 100;
}

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

    const aggs = await fetchPaymentObservationMetricsAggs({
      t,
      customerId,
      ptrsId,
    });

    // Defaults when there are no rows yet
    const stageRowCount = aggs?.stageRowCount || 0;
    const paymentObservationCount = aggs?.paymentObservationCount || 0;

    const totalCount = aggs?.totalCount || 0;
    const tcpSettlementValue = Number(aggs?.tcpSettlementValue || 0);
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

    // Invoice amount quality signal (do NOT block). The SBTCP numerator still
    // comes from observation amounts; the TCP denominator is settlement-based.
    if (missingAmountCount > 0) {
      canonicalQuality.missing.push({
        field: "payment_amount",
        count: missingAmountCount,
      });
    }

    // A staged dataset with no derived payments cannot produce honest metrics.
    if (stageRowCount > 0 && totalCount === 0) {
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
      calculateSmallBusinessTradeCreditPaymentsPct({
        sbValue,
        tcpSettlementValue,
        blocked: canonicalQuality.blocked,
      });
    // logger.logEvent("info", "PTRS v2 metrics debug: SB trade credit %", {
    //   action: "PtrsV2MetricsSbTradeCreditDebug",
    //   ptrsId,
    //   customerId,
    //   totals: {
    //     totalCount,
    //     sbCount,
    //     tcpSettlementValue,
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
      paymentObservationCount,
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
        "Metrics are blocked because no unambiguous payment observations could be derived from the staged source records.",
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

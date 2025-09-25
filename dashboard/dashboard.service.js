const {
  beginTransactionWithCustomerContext,
} = require("../helpers/setCustomerIdRLS");
const db = require("../db/database");

module.exports = {
  getDashboardSignals,
  getDashboardExtendedMetrics,
  // (keep other PTRS-specific helpers if any)
};

async function getDashboardSignals(ptrsId, customerId, period = {}) {
  const { start = null, end = null, daysEarlier = 6 } = period || {};
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    // Base aggregates (paid <=30, stats, small business, late rate)
    const [scalar] = await db.sequelize.query(
      `WITH all_base AS (
  SELECT
    t."paymentAmount"::numeric         AS amount,
    t."paymentTime"::int               AS pt,
    t."paymentTerm"::int               AS term,
    COALESCE(t."isSb", false)          AS is_small_business,
    COALESCE(t."peppolEnabled", false) AS is_peppol
  FROM public.tbl_tcp t
  WHERE t."ptrsId" = :ptrsId
    AND t."customerId" = :customerId
    AND t."isTcp" = true
    AND t."deletedAt" IS NULL
    AND COALESCE(t."excludedTcp", false) = false
    AND t."paymentTime" IS NOT NULL
    AND (:start::date IS NULL OR t."paymentDate" >= :start::date)
    AND (:end::date IS NULL OR t."paymentDate" < (:end::date + INTERVAL '1 day'))
),
base AS (
  SELECT * FROM all_base WHERE is_small_business = true
),
paid_30 AS (
  SELECT COUNT(*)::int AS invoices_paid_within_30,
         COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END),0)::numeric AS value_paid_within_30
  FROM base WHERE pt <= 30
),
stats AS (
  SELECT
    AVG(pt)::numeric                                                      AS avg_days,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY pt)::numeric             AS median_days,
    PERCENTILE_DISC(0.8) WITHIN GROUP (ORDER BY pt)::numeric             AS p80,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY pt)::numeric            AS p95
  FROM base
),
sb AS (
  SELECT
    COUNT(*)                                         AS sb_num,
    COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END),0)::numeric AS sb_val,
    COUNT(*) FILTER (WHERE is_peppol)               AS sb_peppol_num,
    COALESCE(SUM(CASE WHEN is_peppol AND amount > 0 THEN amount ELSE 0 END),0)::numeric AS sb_peppol_val,
    COUNT(*) FILTER (WHERE term IS NOT NULL AND pt > term)                AS sb_late,
    COUNT(*) FILTER (WHERE term IS NOT NULL)                               AS sb_with_term
  FROM base
),
tot AS (
  SELECT COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END),0)::numeric AS total_val
  FROM all_base
)
, late_stats AS (
  SELECT
    AVG((pt - term))::numeric AS avg_days_late
  FROM base
  WHERE term IS NOT NULL AND pt > term
)
, late_stats_ceil AS (
  SELECT CEIL(COALESCE(avg_days_late, 0))::int AS avg_days_late_ceil FROM late_stats
)
, what_if AS (
  SELECT
    COUNT(*) FILTER (WHERE term IS NOT NULL AND pt <= term + :daysEarlier)::int AS within_shifted,
    COUNT(*) FILTER (WHERE term IS NOT NULL)::int                                AS with_term
  FROM base
)
, what_if_avg AS (
  SELECT
    (SELECT avg_days_late_ceil FROM late_stats_ceil)                               AS days,
    COUNT(*) FILTER (WHERE term IS NOT NULL AND pt <= term + (SELECT avg_days_late_ceil FROM late_stats_ceil))::int AS within_shifted,
    COUNT(*) FILTER (WHERE term IS NOT NULL)::int                                   AS with_term
  FROM base
)
       SELECT
         (SELECT invoices_paid_within_30 FROM paid_30)                        AS invoices_paid_within_30,
         (SELECT value_paid_within_30 FROM paid_30)                           AS value_paid_within_30,
         (SELECT avg_days FROM stats)                                         AS avg_days,
         (SELECT median_days FROM stats)                                      AS median_days,
         (SELECT p80 FROM stats)                                              AS p80,
         (SELECT p95 FROM stats)                                              AS p95,
         (SELECT sb_num FROM sb)                                              AS sb_num,
         (SELECT sb_val FROM sb)                                              AS sb_val,
         (SELECT sb_peppol_num FROM sb)                                       AS sb_peppol_num,
         (SELECT sb_peppol_val FROM sb)                                       AS sb_peppol_val,
         (SELECT COALESCE(sb_late::numeric / NULLIF(sb_with_term,0), 0) FROM sb)    AS late_sb_rate
       , (SELECT COALESCE(avg_days_late, 0) FROM late_stats) AS avg_days_late
       , (SELECT COALESCE(100.0 * sb_val / NULLIF(total_val,0), 0) FROM sb, tot) AS sb_value_pct_of_total
       , (SELECT COALESCE(100.0 * sb_peppol_num::numeric / NULLIF(sb_num,0), 0) FROM sb) AS sb_peppol_pct
, (SELECT ROUND(100.0 * within_shifted::numeric / NULLIF(with_term,0), 2) FROM what_if) AS what_if_on_time_pct
, (SELECT within_shifted FROM what_if) AS what_if_on_time_count
, :daysEarlier::int AS what_if_days_earlier
, (
    SELECT ROUND(
      (100.0 * within_shifted::numeric / NULLIF(with_term,0))
      - (100.0 * (1 - late_sb_rate))
    , 2)
    FROM what_if,
         (SELECT COALESCE(sb_late::numeric / NULLIF(sb_with_term,0),0) AS late_sb_rate FROM sb) x
  ) AS what_if_delta_pp
, (SELECT avg_days_late_ceil FROM late_stats_ceil) AS what_if_avg_days
, (SELECT ROUND(100.0 * within_shifted::numeric / NULLIF(with_term,0), 2) FROM what_if_avg) AS what_if_avg_on_time_pct
, (SELECT within_shifted FROM what_if_avg) AS what_if_avg_on_time_count
, (
    SELECT ROUND(
      (100.0 * within_shifted::numeric / NULLIF(with_term,0))
      - (100.0 * (1 - late_sb_rate))
    , 2)
    FROM what_if_avg,
         (SELECT COALESCE(sb_late::numeric / NULLIF(sb_with_term,0),0) AS late_sb_rate FROM sb) x
  ) AS what_if_avg_delta_pp;`,
      {
        replacements: { ptrsId, customerId, start, end, daysEarlier },
        type: db.sequelize.QueryTypes.SELECT,
        transaction: t,
      }
    );

    // Invoice age bands as proportions
    const bands = await db.sequelize.query(
      `WITH base AS (
         SELECT t."paymentTime"::int AS pt
         FROM public.tbl_tcp t
         WHERE t."ptrsId" = :ptrsId
           AND t."customerId" = :customerId
           AND t."isTcp" = true
           AND t."isSb" = true
           AND t."deletedAt" IS NULL
           AND COALESCE(t."excludedTcp", false) = false
           AND t."paymentTime" IS NOT NULL
           AND (:start::date IS NULL OR t."paymentDate" >= :start::date)
           AND (:end::date IS NULL OR t."paymentDate" < (:end::date + INTERVAL '1 day'))
       ), counts AS (
         SELECT
           CASE
             WHEN pt <= 30 THEN '0–30'
             WHEN pt <= 60 THEN '31–60'
             WHEN pt <= 90 THEN '61–90'
             ELSE '90+'
           END AS band,
           COUNT(*)::int AS n
         FROM base
         GROUP BY 1
       )
       SELECT 
          band AS label,
          n::int AS count,
          COALESCE(n::numeric / NULLIF(SUM(n) OVER (), 0), 0) AS pct
        FROM counts
        ORDER BY CASE band WHEN '0–30' THEN 1 WHEN '31–60' THEN 2 WHEN '61–90' THEN 3 ELSE 4 END;
  `,
      {
        replacements: { ptrsId, customerId, start, end },
        type: db.sequelize.QueryTypes.SELECT,
        transaction: t,
      }
    );

    // Slowest paid suppliers (top 5 by avg days)
    const slowest = await db.sequelize.query(
      `WITH base AS (
         SELECT
           t."payeeEntityAbn" AS abn,
           t."payeeEntityName" AS name,
           t."paymentTime"::int AS pt,
           COALESCE(t."paymentTerm"::int, NULL) AS term
         FROM public.tbl_tcp t
         WHERE t."ptrsId" = :ptrsId
           AND t."customerId" = :customerId
           AND t."isTcp" = true
           AND t."isSb" = true
           AND t."deletedAt" IS NULL
           AND COALESCE(t."excludedTcp", false) = false
           AND t."paymentTime" IS NOT NULL
           AND (:start::date IS NULL OR t."paymentDate" >= :start::date)
           AND (:end::date IS NULL OR t."paymentDate" < (:end::date + INTERVAL '1 day'))
           AND t."payeeEntityAbn" IS NOT NULL
       )
       SELECT
         abn AS "payeeEntityAbn",
         name AS "payeeEntityName",
         AVG(pt)::numeric AS "avgDays",
         COUNT(*)::int AS "invoiceCount",
         MODE() WITHIN GROUP (ORDER BY COALESCE(term, NULL))::int AS "modeTerm"
       FROM base
       GROUP BY abn, name
       ORDER BY AVG(pt) DESC
       LIMIT 5;`,
      {
        replacements: { ptrsId, customerId, start, end },
        type: db.sequelize.QueryTypes.SELECT,
        transaction: t,
      }
    );

    const [terms] = await db.sequelize.query(
      `WITH s AS (
         -- rows for within-terms calculation (need both pt and term)
         SELECT t."paymentTime"::int AS pt, t."paymentTerm"::int AS term
         FROM public."tbl_tcp" t
         WHERE t."ptrsId" = :ptrsId
           AND t."customerId" = :customerId
           AND t."isTcp" = true
           AND t."isSb" = true
           AND COALESCE(t."excludedTcp", false) = false
           AND t."deletedAt" IS NULL
           AND t."paymentTime" IS NOT NULL
           AND t."paymentTerm" IS NOT NULL
           AND (:start::date IS NULL OR t."paymentDate" >= :start::date)
           AND (:end::date IS NULL OR t."paymentDate" < (:end::date + INTERVAL '1 day'))
       ),
       term_base AS (
         -- base for per-entity mode of terms; do not require paymentTime
         SELECT 
           t."payerEntityName" AS payer,
           t."paymentTerm"::int AS term
         FROM public."tbl_tcp" t
         WHERE t."ptrsId" = :ptrsId
           AND t."customerId" = :customerId
           AND t."isTcp" = true
           AND t."isSb" = true
           AND COALESCE(t."excludedTcp", false) = false
           AND t."deletedAt" IS NULL
           AND t."paymentTerm" IS NOT NULL
       ),
       per_entity_mode AS (
         SELECT payer, MODE() WITHIN GROUP (ORDER BY term) AS mode_term
         FROM term_base
         GROUP BY payer
       ),
       global_mode AS (
         SELECT MODE() WITHIN GROUP (ORDER BY term) AS mode_term FROM term_base
       )
       SELECT
         (SELECT COUNT(*) FILTER (WHERE pt <= term)::int FROM s)                           AS count_within_terms,
         (SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE pt <= term) / NULLIF(COUNT(*),0), 2) FROM s) AS pct_within_terms,
         (SELECT mode_term FROM global_mode)::int                                          AS mode_term,
         (SELECT MIN(mode_term) FROM per_entity_mode)::int                                 AS term_min,
         (SELECT MAX(mode_term) FROM per_entity_mode)::int                                 AS term_max;`,
      {
        replacements: { ptrsId, customerId, start, end },
        type: db.sequelize.QueryTypes.SELECT,
        transaction: t,
      }
    );

    await t.commit();

    return {
      invoicesPaidWithin30Days: Number(scalar?.invoices_paid_within_30 || 0),
      valuePaidWithin30Days: Number(scalar?.value_paid_within_30 || 0),
      avgDays: Number(scalar?.avg_days ?? 0),
      medianDays: Number(scalar?.median_days ?? 0),
      percentile80: Number(scalar?.p80 ?? 0),
      percentile95: Number(scalar?.p95 ?? 0),
      sbNumPayments: Number(scalar?.sb_num || 0),
      sbValuePayments: Number(scalar?.sb_val || 0),
      sbPeppolNum: Number(scalar?.sb_peppol_num || 0),
      sbPeppolValue: Number(scalar?.sb_peppol_val || 0),
      invoiceBands: bands,
      slowestPaidSuppliers: Array.isArray(slowest)
        ? slowest.map((row) => ({
            payeeEntityAbn: row.payeeEntityAbn,
            payeeEntityName: row.payeeEntityName ?? null,
            avgDays: row.avgDays != null ? Number(row.avgDays) : null,
            invoiceCount:
              row.invoiceCount != null ? Number(row.invoiceCount) : null,
            modeTerm: row.modeTerm != null ? Number(row.modeTerm) : null,
          }))
        : [],
      lateSbRate: Number(scalar?.late_sb_rate ?? 0),
      avgDaysLate: Number(scalar?.avg_days_late ?? 0),
      withinTermsCount: Number(terms?.count_within_terms || 0),
      withinTermsPct: Number(terms?.pct_within_terms ?? 0),
      modeTerm: terms?.mode_term != null ? Number(terms.mode_term) : null,
      termMin: terms?.term_min != null ? Number(terms.term_min) : null,
      termMax: terms?.term_max != null ? Number(terms.term_max) : null,
      sbValuePctOfTotal: Number(scalar?.sb_value_pct_of_total ?? 0),
      sbPeppolPct: Number(scalar?.sb_peppol_pct ?? 0),
      whatIfOnTimePct: Number(scalar?.what_if_on_time_pct ?? 0),
      whatIfOnTimeCount: Number(scalar?.what_if_on_time_count ?? 0),
      whatIfDaysEarlier: Number(scalar?.what_if_days_earlier ?? daysEarlier),
      whatIfDeltaPp: Number(scalar?.what_if_delta_pp ?? 0),
      avgDaysLateCeil: Number(scalar?.what_if_avg_days ?? 0),
      whatIfAvgOnTimePct: Number(scalar?.what_if_avg_on_time_pct ?? 0),
      whatIfAvgOnTimeCount: Number(scalar?.what_if_avg_on_time_count ?? 0),
      whatIfAvgDeltaPp: Number(scalar?.what_if_avg_delta_pp ?? 0),
    };
  } catch (e) {
    await t.rollback();
    throw e;
  }
}

async function getDashboardExtendedMetrics(ptrsId, customerId) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    // more detailed queries…
    await t.commit();
    return {
      // any extra fields you want merged by the FE (same naming style)
    };
  } catch (e) {
    await t.rollback();
    throw e;
  }
}

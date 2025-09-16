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
  const { start = null, end = null } = period || {};
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    // Base aggregates (paid <=30, stats, small business, late rate)
    const [scalar] = await db.sequelize.query(
      `WITH base AS (
         SELECT
           t."paymentAmount"::numeric               AS amount,
           -- days_to_pay: payment minus best-available invoice/receipt/supply date
           EXTRACT(DAY FROM (
             COALESCE(t."paymentDate", NOW()) - COALESCE(
               t."invoiceReceiptDate"::timestamptz,
               t."invoiceIssueDate"::timestamptz,
               t."supplyDate"::timestamptz
             )
           ))::int                                   AS days_to_pay,
           COALESCE(t."isSb", false)                AS is_small_business,
           COALESCE(t."peppolEnabled", false)       AS is_peppol,
           t."payeeEntityAbn"                        AS payee_entity_abn
         FROM public.tbl_tcp t
         WHERE t."ptrsId" = :ptrsId
           AND t."customerId" = :customerId
           AND t."isTcp" = true
           AND t."deletedAt" IS NULL
           AND COALESCE(t."excludedTcp", false) = false
           AND (:start::timestamptz IS NULL OR t."paymentDate" >= :start::timestamptz)
           AND (:end::timestamptz IS NULL OR t."paymentDate" < (:end::timestamptz + INTERVAL '1 day'))
       ),
       paid_30 AS (
         SELECT COUNT(*)::int AS invoices_paid_within_30,
                COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END),0)::numeric AS value_paid_within_30
         FROM base WHERE days_to_pay <= 30
       ),
       stats AS (
         SELECT
           AVG(days_to_pay)::numeric                                                      AS avg_days,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_pay)::numeric             AS median_days,
           PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY days_to_pay)::numeric             AS p80,
           PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY days_to_pay)::numeric            AS p95
         FROM base
       ),
       sb AS (
         SELECT
           COUNT(*) FILTER (WHERE is_small_business)                                      AS sb_num,
           COALESCE(SUM(CASE WHEN is_small_business AND amount > 0 THEN amount ELSE 0 END),0)::numeric AS sb_val,
           COUNT(*) FILTER (WHERE is_small_business AND is_peppol)                        AS sb_peppol_num,
           COALESCE(SUM(CASE WHEN is_small_business AND is_peppol AND amount > 0 THEN amount ELSE 0 END),0)::numeric AS sb_peppol_val,
           COUNT(*) FILTER (WHERE is_small_business AND days_to_pay > 30)                 AS sb_late
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
         (SELECT COALESCE(sb_late::numeric / NULLIF(sb_num,0), 0) FROM sb)    AS late_sb_rate;`,
      {
        replacements: { ptrsId, customerId, start, end },
        type: db.sequelize.QueryTypes.SELECT,
        transaction: t,
      }
    );

    // Invoice age bands as proportions
    const bands = await db.sequelize.query(
      `WITH base AS (
         SELECT EXTRACT(DAY FROM (
                  COALESCE(t."paymentDate", NOW()) - COALESCE(
                    t."invoiceReceiptDate"::timestamptz,
                    t."invoiceIssueDate"::timestamptz,
                    t."supplyDate"::timestamptz
                  )
                ))::int AS days_to_pay
         FROM public.tbl_tcp t
         WHERE t."ptrsId" = :ptrsId
           AND t."customerId" = :customerId
           AND t."isTcp" = true
           AND t."deletedAt" IS NULL
           AND COALESCE(t."excludedTcp", false) = false
           AND (:start::timestamptz IS NULL OR t."paymentDate" >= :start::timestamptz)
           AND (:end::timestamptz IS NULL OR t."paymentDate" < (:end::timestamptz + INTERVAL '1 day'))
       ), counts AS (
         SELECT
           CASE
             WHEN days_to_pay <= 30 THEN '0–30'
             WHEN days_to_pay <= 60 THEN '31–60'
             WHEN days_to_pay <= 90 THEN '61–90'
             ELSE '90+'
           END AS band,
           COUNT(*)::int AS n
         FROM base
         GROUP BY 1
       )
       SELECT band AS label,
              COALESCE(n::numeric / NULLIF(SUM(n) OVER (), 0), 0) AS pct
       FROM counts
       ORDER BY CASE band WHEN '0–30' THEN 1 WHEN '31–60' THEN 2 WHEN '61–90' THEN 3 ELSE 4 END;`,
      {
        replacements: { ptrsId, customerId, start, end },
        type: db.sequelize.QueryTypes.SELECT,
        transaction: t,
      }
    );

    // Slowest paid suppliers (top 5 by avg days)
    const slowest = await db.sequelize.query(
      `WITH base AS (
         SELECT t."payeeEntityAbn" AS abn,
                EXTRACT(DAY FROM (
                  COALESCE(t."paymentDate", NOW()) - COALESCE(
                    t."invoiceReceiptDate"::timestamptz,
                    t."invoiceIssueDate"::timestamptz,
                    t."supplyDate"::timestamptz
                  )
                ))::int AS days_to_pay
         FROM public.tbl_tcp t
         WHERE t."ptrsId" = :ptrsId
           AND t."customerId" = :customerId
           AND t."isTcp" = true
           AND t."deletedAt" IS NULL
           AND COALESCE(t."excludedTcp", false) = false
           AND (:start::timestamptz IS NULL OR t."paymentDate" >= :start::timestamptz)
           AND (:end::timestamptz IS NULL OR t."paymentDate" < (:end::timestamptz + INTERVAL '1 day'))
           AND t."payeeEntityAbn" IS NOT NULL
       )
       SELECT abn AS "payeeEntityAbn",
              AVG(days_to_pay)::numeric AS "avgDays"
       FROM base
       GROUP BY abn
       ORDER BY AVG(days_to_pay) DESC
       LIMIT 5;`,
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
      slowestPaidSuppliers: slowest,
      lateSbRate: Number(scalar?.late_sb_rate ?? 0),
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

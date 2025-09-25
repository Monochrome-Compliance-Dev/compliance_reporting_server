const db = require("../../db/database");
const { logger } = require("../../helpers/logger");
// const { sequelize } = require("../../db/database");
const {
  beginTransactionWithCustomerContext,
} = require("../../helpers/setCustomerIdRLS");

function appendComment(existing, addition) {
  if (!existing) return addition;
  if (existing.includes(addition)) return existing;
  return `${existing} | ${addition}`;
}

const DAY_MS = 24 * 60 * 60 * 1000;

// Returns integer day difference or null if inputs invalid.
// If inclusive is true, adds 1 day when diff is non-negative (for RCTI rule).
// If clampZero is true, negative values are clamped to 0 (non-RCTI rule).
function dayDiff(
  endDate,
  startDate,
  { inclusive = false, clampZero = false } = {}
) {
  if (!endDate || !startDate) return null;
  const end = new Date(endDate);
  const start = new Date(startDate);
  if (isNaN(end) || isNaN(start)) return null;
  let days = Math.round((end - start) / DAY_MS);
  if (inclusive && days >= 0) days += 1;
  if (clampZero && days < 0) days = 0;
  return Number.isFinite(days) ? days : null;
}

async function processTcpMetrics(ptrsId, customerId) {
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    // Validate ptrs belongs to customer
    const ptrs = await db.Ptrs.findOne({
      where: { id: ptrsId, customerId },
      transaction: t,
    });

    if (!ptrs) {
      throw new Error(`Ptrs ${ptrsId} not found for customer ${customerId}`);
    }

    // Set-based update for derived fields: paymentTime, paymentTerm, partialPayment, explanatoryComments1
    await db.sequelize.query(
      `
      WITH cte AS (
        SELECT 
          t.id,
          /* paymentTime per PTRS rules */
          (
            CASE
              WHEN t.rcti IS TRUE THEN /* RCTI: inclusive + clamp to 0 */
                CASE
                  WHEN t."invoiceIssueDate" IS NULL OR t."paymentDate" IS NULL THEN NULL
                  WHEN ((t."paymentDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date - (t."invoiceIssueDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date) <= 0 THEN 0
                  ELSE ((t."paymentDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date - (t."invoiceIssueDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date) + 1
                END

              WHEN t."invoiceIssueDate" IS NOT NULL OR t."invoiceReceiptDate" IS NOT NULL THEN
                /* Non-RCTI: choose the shortest valid path (num +1; clamp to 0) */
                (
                  COALESCE(
                    LEAST(
                      /* issue path (or NULL if unavailable) */
                      CASE
                        WHEN t."paymentDate" IS NULL OR t."invoiceIssueDate" IS NULL THEN NULL
                        WHEN ((t."paymentDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date - (t."invoiceIssueDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date) <= 0 THEN 0
                        ELSE ((t."paymentDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date - (t."invoiceIssueDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date) + 1
                      END,
                      /* receipt path */
                      CASE
                        WHEN t."paymentDate" IS NULL OR t."invoiceReceiptDate" IS NULL THEN NULL
                        WHEN ((t."paymentDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date - (t."invoiceReceiptDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date) <= 0 THEN 0
                        ELSE ((t."paymentDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date - (t."invoiceReceiptDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date) + 1
                      END
                    ),
                    /* if one side was NULL, pick the other */
                    CASE
                      WHEN t."paymentDate" IS NULL OR t."invoiceIssueDate" IS NULL THEN NULL
                      WHEN ((t."paymentDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date - (t."invoiceIssueDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date) <= 0 THEN 0
                      ELSE ((t."paymentDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date - (t."invoiceIssueDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date) + 1
                    END,
                    CASE
                      WHEN t."paymentDate" IS NULL OR t."invoiceReceiptDate" IS NULL THEN NULL
                      WHEN ((t."paymentDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date - (t."invoiceReceiptDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date) <= 0 THEN 0
                      ELSE ((t."paymentDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date - (t."invoiceReceiptDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date) + 1
                    END
                  )
                )

              WHEN t."noticeForPaymentIssueDate" IS NOT NULL THEN
                CASE
                  WHEN t."paymentDate" IS NULL THEN NULL
                  WHEN ((t."paymentDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date - (t."noticeForPaymentIssueDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date) <= 0 THEN 0
                  ELSE ((t."paymentDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date - (t."noticeForPaymentIssueDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date) + 1
                END

              ELSE
                CASE
                  WHEN t."paymentDate" IS NULL OR t."supplyDate" IS NULL THEN NULL
                  WHEN ((t."paymentDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date - (t."supplyDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date) <= 0 THEN 0
                  ELSE ((t."paymentDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date - (t."supplyDate"::timestamptz AT TIME ZONE 'Australia/Brisbane')::date) + 1
                END
            END
          ) AS payment_time,

          /* primary paymentTerm: inclusive days between invoiceIssueDate and invoiceDueDate when valid */
          (
            CASE
              WHEN t."invoiceIssueDate" IS NOT NULL AND t."invoiceDueDate" IS NOT NULL 
                   AND (t."invoiceDueDate"::date - t."invoiceIssueDate"::date) >= 0 THEN
                (t."invoiceDueDate"::date - t."invoiceIssueDate"::date) + 1
              ELSE NULL
            END
          ) AS primary_term,

          /* fallback term fields (as text) */
          COALESCE(
            NULLIF(t."invoicePaymentTerms"::text, ''),
            NULLIF(t."noticeForPaymentTerms"::text, ''),
            NULLIF(t."contractPoPaymentTerms"::text, '')
          ) AS fallback_term_text,

          t."paymentAmount", t."invoiceAmount",
          t."explanatoryComments1"
        FROM public."tbl_tcp" t
        WHERE t."ptrsId" = :ptrsId
          AND t."customerId" = :customerId
          AND t."isTcp" = TRUE
          AND t."excludedTcp" = FALSE
      )
      UPDATE public."tbl_tcp" u
      SET 
        "paymentTime" = c.payment_time,
        "paymentTerm" = COALESCE(
          c.primary_term,
          NULLIF(regexp_replace(c.fallback_term_text, '[^0-9]', '', 'g'), '')::int,
          31
        ),
        "partialPayment" = COALESCE(
          CASE 
            WHEN c."paymentAmount" IS NOT NULL AND c."invoiceAmount" IS NOT NULL THEN (c."paymentAmount" < c."invoiceAmount")
            ELSE NULL
          END,
          FALSE
        ),
        "explanatoryComments1" = CASE
          WHEN c.primary_term IS NULL AND NULLIF(regexp_replace(c.fallback_term_text, '[^0-9]', '', 'g'), '') IS NOT NULL THEN 
            COALESCE(NULLIF(c."explanatoryComments1", '' ) || ' | ', '') || 'Used fallback term field'
          WHEN c.primary_term IS NULL AND NULLIF(regexp_replace(c.fallback_term_text, '[^0-9]', '', 'g'), '') IS NULL THEN 
            COALESCE(NULLIF(c."explanatoryComments1", '' ) || ' | ', '') || 'Fallback to default 31 day term'
          ELSE c."explanatoryComments1"
        END
      FROM cte c
      WHERE u.id = c.id;
      `,
      { replacements: { ptrsId, customerId }, transaction: t }
    );

    // After row-level updates, compute aggregates for reporting in the same transaction
    const [aggRows] = await db.sequelize.query(
      `
WITH sb AS (
  SELECT
    t.id,
    t."paymentTime"::int AS pt,
    t."paymentTerm"::int AS term
  FROM public."tbl_tcp" t
  WHERE t."ptrsId" = :ptrsId
    AND t."customerId" = :customerId
    AND t."isSb" = TRUE
    AND t."isTcp" = TRUE
    AND t."excludedTcp" = FALSE
    AND t."paymentTime" IS NOT NULL
),
-- Base set for payment-term calculations (does not depend on paymentTime)
term_base AS (
  SELECT 
    t."payerEntityName" AS payer,
    t."paymentTerm"::int AS term
  FROM public."tbl_tcp" t
  WHERE t."ptrsId" = :ptrsId
    AND t."customerId" = :customerId
    AND t."isSb" = TRUE
    AND t."isTcp" = TRUE
    AND t."excludedTcp" = FALSE
    AND t."paymentTerm" IS NOT NULL
),
-- Per-entity modes of paymentTerm
per_entity_mode AS (
  SELECT 
    payer,
    MODE() WITHIN GROUP (ORDER BY term) AS mode_term
  FROM term_base
  GROUP BY payer
),
-- Global mode of paymentTerm
global_mode AS (
  SELECT MODE() WITHIN GROUP (ORDER BY term) AS mode_term
  FROM term_base
)
SELECT
  COUNT(*)                                                  AS sb_total_payments,
  COUNT(*) FILTER (WHERE sb.pt <= 30)                      AS count_leq_30,
  COUNT(*) FILTER (WHERE sb.pt BETWEEN 31 AND 60)          AS count_31_60,
  COUNT(*) FILTER (WHERE sb.pt > 60)                       AS count_gt_60,
  COUNT(*) FILTER (WHERE sb.term IS NOT NULL AND sb.pt <= sb.term)   AS count_within_terms,
  ROUND(
    100.0 * COUNT(*) FILTER (WHERE sb.term IS NOT NULL AND sb.pt <= sb.term)
    / NULLIF(COUNT(*),0), 2
  )                                                         AS pct_within_terms,
  ROUND(AVG(sb.pt)::numeric, 2)                             AS avg_days,
  PERCENTILE_DISC(0.5)  WITHIN GROUP (ORDER BY sb.pt)       AS median_days,
  PERCENTILE_DISC(0.8)  WITHIN GROUP (ORDER BY sb.pt)       AS p80_days,
  PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY sb.pt)       AS p95_days,
  -- Payment term outputs aligned to regulator worked example
  (SELECT mode_term FROM global_mode)::int                  AS mode_term,
  (SELECT MIN(mode_term) FROM per_entity_mode)::int         AS term_min,
  (SELECT MAX(mode_term) FROM per_entity_mode)::int         AS term_max
FROM sb;
      `,
      { replacements: { ptrsId, customerId }, transaction: t }
    );

    await t.commit();
    return aggRows && aggRows[0] ? aggRows[0] : {};
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Error processing TCP metrics", {
      action: "ProcessTcpMetrics",
      ptrsId,
      customerId,
      error,
    });
    throw error;
  }
}

module.exports = { processTcpMetrics };

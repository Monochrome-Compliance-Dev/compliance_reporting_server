const { QueryTypes } = require("sequelize");

const EXCLUSION_REASON = "PAYMENT_TERMS";
const EXCLUSION_COMMENT =
  "Excluded due to immediate/non-trade-credit payment terms (0 days).";

function buildPaymentTermsMatchSql() {
  return `
    (
      NULLIF(TRIM(COALESCE(s."data"->>'payment_term_days', '')), '') = '0'
      OR NULLIF(TRIM(COALESCE(s."data"->>'invoice_payment_terms', '')), '') = '0'
    )
  `;
}

async function applyPaymentTermsExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
}) {
  if (!sequelize) throw new Error("sequelize is required");
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const matchSql = buildPaymentTermsMatchSql();

  const sql = `
    UPDATE "tbl_ptrs_stage_row" s
    SET
      "data" = jsonb_set(
        jsonb_set(
          jsonb_set(
            jsonb_set(
              jsonb_set(
                COALESCE(s."data", '{}'::jsonb),
                '{exclude}',
                'true'::jsonb,
                true
              ),
              '{exclude_from_metrics}',
              'true'::jsonb,
              true
            ),
            '{exclude_reason}',
            to_jsonb(:reason::text),
            true
          ),
          '{exclude_comment}',
          to_jsonb(:comment::text),
          true
        ),
        '{exclude_reasons}',
        (
          SELECT to_jsonb(array_agg(DISTINCT reason_value))
          FROM (
            SELECT jsonb_array_elements_text(
              CASE
                WHEN jsonb_typeof(COALESCE(s."data"->'exclude_reasons', '[]'::jsonb)) = 'array'
                  THEN COALESCE(s."data"->'exclude_reasons', '[]'::jsonb)
                ELSE '[]'::jsonb
              END
            ) AS reason_value
            UNION ALL
            SELECT :reason::text
          ) reasons
        ),
        true
      ),
      "meta" = jsonb_set(
        jsonb_set(
          jsonb_set(
            jsonb_set(
              jsonb_set(
                COALESCE(s."meta", '{}'::jsonb),
                '{exclusions,exclude}',
                'true'::jsonb,
                true
              ),
              '{exclusions,exclude_from_metrics}',
              'true'::jsonb,
              true
            ),
            '{exclusions,reason}',
            to_jsonb(:reason::text),
            true
          ),
          '{exclusions,reasons}',
          (
            SELECT to_jsonb(array_agg(DISTINCT reason_value))
            FROM (
              SELECT jsonb_array_elements_text(
                CASE
                  WHEN jsonb_typeof(COALESCE(s."meta"->'exclusions'->'reasons', '[]'::jsonb)) = 'array'
                    THEN COALESCE(s."meta"->'exclusions'->'reasons', '[]'::jsonb)
                  ELSE '[]'::jsonb
                END
              ) AS reason_value
              UNION ALL
              SELECT :reason::text
            ) reasons
          ),
          true
        ),
        '{exclusions,comments}',
        (
          SELECT to_jsonb(array_agg(DISTINCT comment_value))
          FROM (
            SELECT jsonb_array_elements_text(
              CASE
                WHEN jsonb_typeof(COALESCE(s."meta"->'exclusions'->'comments', '[]'::jsonb)) = 'array'
                  THEN COALESCE(s."meta"->'exclusions'->'comments', '[]'::jsonb)
                ELSE '[]'::jsonb
              END
            ) AS comment_value
            UNION ALL
            SELECT :comment::text
          ) comments
        ),
        true
      ),
      "updatedAt" = now()
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND ${matchSql}
  `;

  const [_, meta] = await sequelize.query(sql, {
    replacements: {
      customerId,
      ptrsId,
      reason: EXCLUSION_REASON,
      comment: EXCLUSION_COMMENT,
    },
    transaction,
  });

  return Number(meta?.rowCount || 0);
}

async function previewPaymentTermsExclusion({
  sequelize,
  transaction,
  customerId,
  ptrsId,
  effectiveLimit = 10,
}) {
  if (!sequelize) throw new Error("sequelize is required");
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const matchSql = buildPaymentTermsMatchSql();

  const countSql = `
    SELECT
      COUNT(*)::int AS matched,
      COUNT(*) FILTER (
        WHERE COALESCE((s."data"->>'exclude_from_metrics')::boolean, false) = true
           OR COALESCE((s."meta"->'exclusions'->>'exclude')::boolean, false) = true
      )::int AS "alreadyExcluded"
    FROM "tbl_ptrs_stage_row" s
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND ${matchSql}
  `;

  const sampleSql = `
    SELECT
      s."rowNo" AS row_no,
      s."data"->>'payee_entity_name' AS payee_entity_name,
      s."data"->>'payee_entity_abn' AS payee_entity_abn,
      s."data"->>'invoice_reference_number' AS invoice_reference_number,
      s."data"->>'account_code' AS account_code,
      s."data"->>'payment_date' AS payment_date,
      s."data"->>'payment_amount' AS payment_amount,
      s."data"->>'description' AS description,
      s."data"->>'invoice_payment_terms' AS invoice_payment_terms,
      s."data"->>'payment_term_days' AS payment_term_days,
      (
        COALESCE((s."data"->>'exclude_from_metrics')::boolean, false)
        OR COALESCE((s."meta"->'exclusions'->>'exclude')::boolean, false)
      ) AS "alreadyExcluded",
      :comment AS exclude_comment
    FROM "tbl_ptrs_stage_row" s
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND ${matchSql}
    ORDER BY s."rowNo" ASC
    LIMIT :limit
  `;

  const [countRows, sampleRows] = await Promise.all([
    sequelize.query(countSql, {
      type: QueryTypes.SELECT,
      replacements: { customerId, ptrsId },
      transaction,
    }),
    sequelize.query(sampleSql, {
      type: QueryTypes.SELECT,
      replacements: {
        customerId,
        ptrsId,
        limit: Number(effectiveLimit) || 10,
        comment: EXCLUSION_COMMENT,
      },
      transaction,
    }),
  ]);

  return {
    matched: Number(countRows?.[0]?.matched || 0),
    alreadyExcluded: Number(countRows?.[0]?.alreadyExcluded || 0),
    sampleRows: Array.isArray(sampleRows) ? sampleRows : [],
  };
}

module.exports = {
  applyPaymentTermsExclusion,
  previewPaymentTermsExclusion,
};

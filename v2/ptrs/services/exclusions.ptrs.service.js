const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { slog } = require("./ptrs.service");
const { QueryTypes, Op } = require("sequelize");

// Helper functions
function normaliseKeyword(raw) {
  return String(raw || "").trim();
}

function getKeywordModel(sequelize) {
  return sequelize?.models?.PtrsExclusionKeywordCustomerRef;
}

/**
 * Exclusions are eligibility decisions, not transformations.
 * Canonical pattern: SQL-first updates against tbl_ptrs_stage_row (jsonb),
 * never destroy/rebuild stage rows for exclusions.
 */

async function applyExclusionsAndPersist({
  customerId,
  ptrsId,
  profileId = null,
  category = "all",
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const started = Date.now();

  slog.info("PTRS v2 exclusions apply: starting", {
    action: "PtrsV2ExclusionsApplyStart",
    customerId,
    ptrsId,
    category,
  });

  const sequelize = db?.sequelize;
  if (!sequelize) {
    throw new Error("Database not initialised: db.sequelize missing");
  }

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const stats = { checksRun: 0, rowsExcluded: 0 };

    // Gov Entities (global ref table)
    if (category === "all" || category === "gov") {
      stats.checksRun += 1;

      const sql = `
        UPDATE "tbl_ptrs_stage_row" s
        SET
          "data" = jsonb_set(
            jsonb_set(
              jsonb_set(
                jsonb_set(
                  s."data",
                  '{exclude}',
                  'true'::jsonb,
                  true
                ),
                '{exclude_from_metrics}',
                'true'::jsonb,
                true
              ),
              '{exclude_reason}',
              to_jsonb('GOV_ENTITY'::text),
              true
            ),
            '{exclude_comment}',
            to_jsonb(
              (
                'Government entity' ||
                CASE WHEN COALESCE(g."name",'') <> '' THEN ' — ' || g."name" ELSE '' END ||
                CASE WHEN COALESCE(g."category",'') <> '' THEN ' — ' || g."category" ELSE '' END
              )::text
            ),
            true
          ),
          "meta" = jsonb_set(
            jsonb_set(
              jsonb_set(
                COALESCE(s."meta",'{}'::jsonb),
                '{_stage}',
                to_jsonb('ptrs.v2.exclusionsApply'::text),
                true
              ),
              '{at}',
              to_jsonb(now()::text),
              true
            ),
            '{exclusions}',
            jsonb_build_object(
              'excluded', true,
              'reason', 'GOV_ENTITY',
              'comment',
                (
                  'Government entity' ||
                  CASE WHEN COALESCE(g."name",'') <> '' THEN ' — ' || g."name" ELSE '' END ||
                  CASE WHEN COALESCE(g."category",'') <> '' THEN ' — ' || g."category" ELSE '' END
                )::text
            ),
            true
          ),
          "updatedAt" = now()
        FROM "tbl_ptrs_gov_entity_ref" g
        WHERE
          s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
          AND g."deletedAt" IS NULL
          AND (
            (
              NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NOT NULL
              AND NULLIF(regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g'), '') IS NOT NULL
              AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
                = regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g')
            )
            OR
            (
              -- Fallback: name match when either side has no usable ABN
              (
                NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NULL
                OR NULLIF(regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g'), '') IS NULL
              )
              AND regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g')
                = regexp_replace(lower(trim(COALESCE(g."name",''))), '\\s+', ' ', 'g')
              AND trim(COALESCE(s."data"->>'payee_entity_name','')) <> ''
              AND trim(COALESCE(g."name",'')) <> ''
            )
          )
          AND COALESCE((s."data"->>'exclude')::boolean, false) = false
      `;

      const [, meta] = await sequelize.query(sql, {
        replacements: { customerId, ptrsId },
        transaction: t,
      });

      const affected = Number(meta?.rowCount ?? 0) || 0;
      stats.rowsExcluded += affected;
    }

    // Intra-company (profile-scoped entity registry)
    if (category === "all" || category === "intra_company") {
      if (!profileId)
        throw new Error("profileId is required for intra_company");
      stats.checksRun += 1;

      const sql = `
        UPDATE "tbl_ptrs_stage_row" s
        SET
          "data" = jsonb_set(
            jsonb_set(
              jsonb_set(
                jsonb_set(
                  s."data",
                  '{exclude}',
                  'true'::jsonb,
                  true
                ),
                '{exclude_from_metrics}',
                'true'::jsonb,
                true
              ),
              '{exclude_reason}',
              to_jsonb('INTRA_COMPANY'::text),
              true
            ),
            '{exclude_comment}',
            to_jsonb(
              (
                'Intra-company' ||
                CASE
                  WHEN COALESCE(e."entityName",'') <> '' THEN ' — ' || e."entityName"
                  WHEN COALESCE(e."payerEntityName",'') <> '' THEN ' — ' || e."payerEntityName"
                  ELSE ''
                END
              )::text
            ),
            true
          ),
          "meta" = jsonb_set(
            jsonb_set(
              jsonb_set(
                COALESCE(s."meta",'{}'::jsonb),
                '{_stage}',
                to_jsonb('ptrs.v2.exclusionsApply'::text),
                true
              ),
              '{at}',
              to_jsonb(now()::text),
              true
            ),
            '{exclusions}',
            jsonb_build_object(
              'excluded', true,
              'reason', 'INTRA_COMPANY',
              'comment',
                (
                  'Intra-company' ||
                  CASE
                    WHEN COALESCE(e."entityName",'') <> '' THEN ' — ' || e."entityName"
                    WHEN COALESCE(e."payerEntityName",'') <> '' THEN ' — ' || e."payerEntityName"
                    ELSE ''
                  END
                )::text
            ),
            true
          ),
          "updatedAt" = now()
        FROM "tbl_ptrs_entity_ref" e
        WHERE
          s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
          AND e."deletedAt" IS NULL
          AND e."customerId" = :customerId
          AND e."profileId" = :profileId
          AND (
            (
              NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NOT NULL
              AND NULLIF(regexp_replace(COALESCE(e."abn",''), '\\D', '', 'g'), '') IS NOT NULL
              AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
                = regexp_replace(COALESCE(e."abn",''), '\\D', '', 'g')
            )
            OR
            (
              (
                NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NULL
                OR NULLIF(regexp_replace(COALESCE(e."abn",''), '\\D', '', 'g'), '') IS NULL
              )
              AND (
                regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g')
                  = regexp_replace(lower(trim(COALESCE(e."entityName",''))), '\\s+', ' ', 'g')
                OR
                regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g')
                  = regexp_replace(lower(trim(COALESCE(e."payerEntityName",''))), '\\s+', ' ', 'g')
              )
              AND trim(COALESCE(s."data"->>'payee_entity_name','')) <> ''
              AND (
                trim(COALESCE(e."entityName",'')) <> ''
                OR trim(COALESCE(e."payerEntityName",'')) <> ''
              )
            )
          )
          AND COALESCE((s."data"->>'exclude')::boolean, false) = false
      `;

      const [, meta] = await sequelize.query(sql, {
        replacements: { customerId, ptrsId, profileId },
        transaction: t,
      });

      const affected = Number(meta?.rowCount ?? 0) || 0;
      stats.rowsExcluded += affected;
    }

    // Employee & expense payments (profile-scoped ref list; keyword match)
    if (category === "all" || category === "employee") {
      if (!profileId) throw new Error("profileId is required for employee");
      stats.checksRun += 1;

      const sql = `
WITH matches AS (
  SELECT
    s."id" AS stage_id,
    MIN(r."name") AS matched_name
  FROM "tbl_ptrs_stage_row" s
  JOIN "tbl_ptrs_employee_ref" r
    ON r."customerId" = :customerId
    AND r."profileId" = :profileId
    AND r."deletedAt" IS NULL
  WHERE
    s."customerId" = :customerId
    AND s."ptrsId" = :ptrsId
    AND s."deletedAt" IS NULL
    AND COALESCE((s."data"->>'exclude')::boolean, false) = false
    AND (
      -- ABN match (strong)
      (
        NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NOT NULL
        AND NULLIF(regexp_replace(COALESCE(r."abn",''), '\\D', '', 'g'), '') IS NOT NULL
        AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
          = regexp_replace(COALESCE(r."abn",''), '\\D', '', 'g')
      )
      OR
      -- Fuzzy match: payee name contains ref name
      (
        trim(COALESCE(s."data"->>'payee_entity_name','')) <> ''
        AND trim(COALESCE(r."name",'')) <> ''
        AND (
          regexp_replace(
            regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g'),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          LIKE '%' ||
          regexp_replace(
            regexp_replace(lower(trim(COALESCE(r."name",''))), '\\s+', ' ', 'g'),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          || '%'
          OR
          regexp_replace(
            regexp_replace(
              regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g'),
              '^(er\\s*-\\s*)',
              '',
              'g'
            ),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          LIKE '%' ||
          regexp_replace(
            regexp_replace(
              regexp_replace(lower(trim(COALESCE(r."name",''))), '\\s+', ' ', 'g'),
              '^(er\\s*-\\s*)',
              '',
              'g'
            ),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          || '%'
        )
      )
      OR
      -- Fuzzy match: description contains ref name
      (
        trim(COALESCE(s."data"->>'description','')) <> ''
        AND trim(COALESCE(r."name",'')) <> ''
        AND (
          regexp_replace(
            regexp_replace(lower(trim(COALESCE(s."data"->>'description',''))), '\\s+', ' ', 'g'),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          LIKE '%' ||
          regexp_replace(
            regexp_replace(lower(trim(COALESCE(r."name",''))), '\\s+', ' ', 'g'),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          || '%'
        )
      )
    )
  GROUP BY s."id"
)
UPDATE "tbl_ptrs_stage_row" s
SET
  "data" = jsonb_set(
    jsonb_set(
      jsonb_set(
        jsonb_set(
          s."data",
          '{exclude}',
          'true'::jsonb,
          true
        ),
        '{exclude_from_metrics}',
        'true'::jsonb,
        true
      ),
      '{exclude_reason}',
      to_jsonb('EMPLOYEE'::text),
      true
    ),
    '{exclude_comment}',
    to_jsonb(
      (
        'Employee / payroll' ||
        CASE WHEN COALESCE(m.matched_name,'') <> '' THEN ' — ' || m.matched_name ELSE '' END
      )::text
    ),
    true
  ),
  "meta" = jsonb_set(
    jsonb_set(
      jsonb_set(
        COALESCE(s."meta",'{}'::jsonb),
        '{_stage}',
        to_jsonb('ptrs.v2.exclusionsApply'::text),
        true
      ),
      '{at}',
      to_jsonb(now()::text),
      true
    ),
    '{exclusions}',
    jsonb_build_object(
      'excluded', true,
      'reason', 'EMPLOYEE',
      'comment',
        (
          'Employee / payroll' ||
          CASE WHEN COALESCE(m.matched_name,'') <> '' THEN ' — ' || m.matched_name ELSE '' END
        )::text
    ),
    true
  ),
  "updatedAt" = now()
FROM matches m
WHERE
  s."id" = m.stage_id;
`;

      const [, meta] = await sequelize.query(sql, {
        replacements: { customerId, ptrsId, profileId },
        transaction: t,
      });

      const affected = Number(meta?.rowCount ?? 0) || 0;
      stats.rowsExcluded += affected;
    }

    // Keyword exclusions (profile-scoped keyword list)
    if (category === "all" || category === "keyword") {
      if (!profileId) throw new Error("profileId is required for keyword");
      stats.checksRun += 1;

      const sql = `
        UPDATE "tbl_ptrs_stage_row" s
        SET
          "data" = jsonb_set(
            jsonb_set(
              jsonb_set(
                jsonb_set(
                  s."data",
                  '{exclude}',
                  'true'::jsonb,
                  true
                ),
                '{exclude_from_metrics}',
                'true'::jsonb,
                true
              ),
              '{exclude_reason}',
              to_jsonb('KEYWORD'::text),
              true
            ),
            '{exclude_comment}',
            to_jsonb(('Keyword exclusion — ' || k."keyword")::text),
            true
          ),
          "meta" = jsonb_set(
            jsonb_set(
              jsonb_set(
                COALESCE(s."meta",'{}'::jsonb),
                '{_stage}',
                to_jsonb('ptrs.v2.exclusionsApply'::text),
                true
              ),
              '{at}',
              to_jsonb(now()::text),
              true
            ),
            '{exclusions}',
            jsonb_build_object(
              'excluded', true,
              'reason', 'KEYWORD',
              'comment', ('Keyword exclusion — ' || k."keyword")::text,
              'keyword', k."keyword"
            ),
            true
          ),
          "updatedAt" = now()
        FROM "tbl_ptrs_exclusion_keyword_customer_ref" k
        WHERE
          s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
          AND k."deletedAt" IS NULL
          AND k."customerId" = :customerId
          AND k."profileId" = :profileId
          AND (
            s."data"->>'payee_entity_name' ILIKE '%'||k."keyword"||'%' OR
            s."data"->>'description' ILIKE '%'||k."keyword"||'%' OR
            s."data"->>'invoice_reference_number' ILIKE '%'||k."keyword"||'%' OR
            s."data"->>'account_name' ILIKE '%'||k."keyword"||'%' OR
            s."data"->>'account_code' ILIKE '%'||k."keyword"||'%'
          )
          AND COALESCE((s."data"->>'exclude')::boolean, false) = false
      `;

      const [, meta] = await sequelize.query(sql, {
        replacements: { customerId, ptrsId, profileId },
        transaction: t,
      });

      const affected = Number(meta?.rowCount ?? 0) || 0;
      stats.rowsExcluded += affected;
    }

    // Partial payments (SQL grouping; Mitch-style for "Multiple")
    if (category === "all" || category === "partial") {
      stats.checksRun += 1;

      const sql = `
        WITH base AS (
          SELECT
            s."id",
            s."rowNo",
            s."data",
            -- Parse payment_date robustly (YYYY-MM-DD or DD/MM/YYYY)
            CASE
              WHEN COALESCE(s."data"->>'payment_date','') LIKE '%/%'
                THEN to_date(s."data"->>'payment_date', 'DD/MM/YYYY')
              WHEN COALESCE(s."data"->>'payment_date','') LIKE '%-%'
                THEN (s."data"->>'payment_date')::date
              ELSE NULL
            END AS pay_date,
            -- Numeric amounts (abs, tolerate blanks)
            NULLIF(regexp_replace(COALESCE(s."data"->>'payment_amount',''), '[^0-9\\.-]', '', 'g'), '')::numeric AS pay_amt_raw,
            NULLIF(regexp_replace(COALESCE(s."data"->>'invoice_amount',''), '[^0-9\\.-]', '', 'g'), '')::numeric AS inv_amt_raw,
            -- Normalised payer / payee identifiers (prefer ABN digits else name)
            COALESCE(
              NULLIF(regexp_replace(COALESCE(s."data"->>'payer_entity_abn',''), '\\D', '', 'g'), ''),
              regexp_replace(lower(trim(COALESCE(s."data"->>'payer_entity_name',''))), '\\s+', ' ', 'g')
            ) AS payer_key,
            COALESCE(
              NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), ''),
              regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g')
            ) AS payee_key,
            regexp_replace(lower(trim(COALESCE(s."data"->>'invoice_reference_number',''))), '\\s+', ' ', 'g') AS inv_ref_norm,
            regexp_replace(lower(trim(COALESCE(s."data"->>'account_code',''))), '\\s+', ' ', 'g') AS acct_code_norm
          FROM "tbl_ptrs_stage_row" s
          WHERE
            s."customerId" = :customerId
            AND s."ptrsId" = :ptrsId
            AND s."deletedAt" IS NULL
        ),
        keyed AS (
          SELECT
            b.*,
            abs(COALESCE(b.pay_amt_raw, 0)) AS pay_amt,
            abs(COALESCE(b.inv_amt_raw, 0)) AS inv_amt,
            CASE
              WHEN COALESCE(b.inv_ref_norm,'') <> '' AND b.inv_ref_norm <> 'multiple'
                THEN (b.payer_key || '|' || b.payee_key || '|' || b.inv_ref_norm)
              ELSE (b.payer_key || '|' || b.acct_code_norm)
            END AS group_key,
            CASE
              WHEN COALESCE(b.inv_ref_norm,'') <> '' AND b.inv_ref_norm <> 'multiple'
                THEN false
              ELSE true
            END AS is_multiple_mode
          FROM base b
        ),
        groups AS (
          SELECT k.*
          FROM keyed k
          WHERE k.group_key IS NOT NULL AND k.group_key <> '|'
        ),
        grouped AS (
          SELECT
            g.*,
            COUNT(*) OVER (PARTITION BY g.group_key) AS grp_count
          FROM groups g
        ),
        eligible AS (
          SELECT
            g.*,
            SUM(g.pay_amt_raw) OVER (PARTITION BY g.group_key) AS net_sum,
            COUNT(DISTINCT SIGN(g.pay_amt_raw)) OVER (PARTITION BY g.group_key) AS sign_count
          FROM grouped g
          WHERE g.grp_count > 1
        ),
        filtered AS (
          SELECT *
          FROM eligible
          WHERE
            net_sum <> 0               -- exclude reversal pairs (net zero)
            AND sign_count = 1         -- all payments must have same sign
        ),
        ordered AS (
          SELECT
            e.*,
            ROW_NUMBER() OVER (
              PARTITION BY e.group_key
              ORDER BY e.pay_date NULLS LAST, e."rowNo" ASC
            ) AS rn_asc,
            ROW_NUMBER() OVER (
              PARTITION BY e.group_key
              ORDER BY e.pay_date NULLS LAST, e."rowNo" DESC
            ) AS rn_desc,
            SUM(abs(COALESCE(e.pay_amt_raw, 0))) OVER (
              PARTITION BY e.group_key
              ORDER BY e.pay_date NULLS LAST, e."rowNo" ASC
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_paid
          FROM filtered e
        ),
        keep_row AS (
          SELECT DISTINCT ON (o.group_key)
            o.group_key,
            COALESCE(
              (
                SELECT oo."rowNo"
                FROM ordered oo
                WHERE
                  oo.group_key = o.group_key
                  AND oo.inv_amt > 0
                  AND oo.cum_paid >= oo.inv_amt
                ORDER BY oo.rn_asc ASC
                LIMIT 1
              ),
              (
                SELECT oo."rowNo"
                FROM ordered oo
                WHERE oo.group_key = o.group_key
                ORDER BY oo.rn_desc ASC
                LIMIT 1
              )
            ) AS keep_row_no,
            COALESCE(
              (
                SELECT oo.inv_amt
                FROM ordered oo
                WHERE
                  oo.group_key = o.group_key
                  AND oo.inv_amt > 0
                  AND oo.cum_paid >= oo.inv_amt
                ORDER BY oo.rn_asc ASC
                LIMIT 1
              ),
              0
            ) AS keep_inv_amt
          FROM ordered o
        ),
        candidates AS (
          SELECT
            o."id",
            o."rowNo",
            o.group_key,
            o.is_multiple_mode,
            k.keep_row_no,
            k.keep_inv_amt
          FROM ordered o
          JOIN keep_row k
            ON k.group_key = o.group_key
          WHERE o."rowNo" <> k.keep_row_no
        )
        UPDATE "tbl_ptrs_stage_row" s
        SET
          "data" = jsonb_set(
            jsonb_set(
              jsonb_set(
                jsonb_set(
                  s."data",
                  '{exclude}',
                  'true'::jsonb,
                  true
                ),
                '{exclude_from_metrics}',
                'true'::jsonb,
                true
              ),
              '{exclude_reason}',
              to_jsonb(
                CASE
                  WHEN c.keep_inv_amt > 0 THEN 'PARTIAL_PAYMENT'
                  ELSE 'PARTIAL_PAYMENT_HEURISTIC'
                END::text
              ),
              true
            ),
            '{exclude_comment}',
            to_jsonb(
              (
                'Partial payment — earlier instalment (kept row ' ||
                c.keep_row_no::text ||
                CASE WHEN c.is_multiple_mode THEN ', multiple-mode' ELSE '' END ||
                ')'
              )::text
            ),
            true
          ),
          "meta" = jsonb_set(
            jsonb_set(
              jsonb_set(
                COALESCE(s."meta",'{}'::jsonb),
                '{_stage}',
                to_jsonb('ptrs.v2.exclusionsApply'::text),
                true
              ),
              '{at}',
              to_jsonb(now()::text),
              true
            ),
            '{exclusions}',
            jsonb_build_object(
              'excluded', true,
              'reason',
                CASE
                  WHEN c.keep_inv_amt > 0 THEN 'PARTIAL_PAYMENT'
                  ELSE 'PARTIAL_PAYMENT_HEURISTIC'
                END,
              'comment',
                (
                  'Partial payment — earlier instalment (kept row ' ||
                  c.keep_row_no::text ||
                  CASE WHEN c.is_multiple_mode THEN ', multiple-mode' ELSE '' END ||
                  ')'
                )::text
            ),
            true
          ),
          "updatedAt" = now()
        FROM candidates c
        WHERE
          s."id" = c."id"
          AND s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
          AND COALESCE((s."data"->>'exclude')::boolean, false) = false
      `;

      const [, meta] = await sequelize.query(sql, {
        replacements: { customerId, ptrsId },
        transaction: t,
      });

      const affected = Number(meta?.rowCount ?? 0) || 0;
      stats.rowsExcluded += affected;
    }

    // Pre-payments (heuristic match on payment terms OR description)
    if (category === "all" || category === "prepaid") {
      stats.checksRun += 1;

      const sql = `
    UPDATE "tbl_ptrs_stage_row" s
    SET
      "data" = jsonb_set(
        jsonb_set(
          jsonb_set(
            jsonb_set(
              s."data",
              '{exclude}',
              'true'::jsonb,
              true
            ),
            '{exclude_from_metrics}',
            'true'::jsonb,
            true
          ),
          '{exclude_reason}',
          to_jsonb('PREPAID'::text),
          true
        ),
        '{exclude_comment}',
        to_jsonb('Prepayment — matched payment terms / description'::text),
        true
      ),
      "meta" = jsonb_set(
        jsonb_set(
          jsonb_set(
            COALESCE(s."meta",'{}'::jsonb),
            '{_stage}',
            to_jsonb('ptrs.v2.exclusionsApply'::text),
            true
          ),
          '{at}',
          to_jsonb(now()::text),
          true
        ),
        '{exclusions}',
        jsonb_build_object(
          'excluded', true,
          'reason', 'PREPAID',
          'comment', 'Prepayment — matched payment terms / description'
        ),
        true
      ),
      "updatedAt" = now()
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND COALESCE((s."data"->>'exclude')::boolean, false) = false
      AND (
        COALESCE(s."data"->>'payment_terms','') ILIKE '%prepaid%'
        OR COALESCE(s."data"->>'payment_terms','') ILIKE '%pre-pay%'
        OR COALESCE(s."data"->>'payment_terms','') ILIKE '%prepay%'
        OR COALESCE(s."data"->>'description','') ILIKE '%prepaid%'
        OR COALESCE(s."data"->>'description','') ILIKE '%pre-pay%'
        OR COALESCE(s."data"->>'description','') ILIKE '%prepay%'
      )
  `;

      const [, meta] = await sequelize.query(sql, {
        replacements: { customerId, ptrsId },
        transaction: t,
      });

      const affected = Number(meta?.rowCount ?? 0) || 0;
      stats.rowsExcluded += affected;
    }

    // Stamp profileId onto meta for this run (only if provided), without touching data.
    if (profileId) {
      const profileSql = `
        UPDATE "tbl_ptrs_stage_row" s
        SET
          "meta" = jsonb_set(
            COALESCE(s."meta",'{}'::jsonb),
            '{profileId}',
            to_jsonb(:profileId::text),
            true
          ),
          "updatedAt" = now()
        WHERE
          s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
      `;

      await sequelize.query(profileSql, {
        replacements: { customerId, ptrsId, profileId },
        transaction: t,
      });
    }

    await t.commit();

    const tookMs = Date.now() - started;

    slog.info("PTRS v2 exclusions apply: done", {
      action: "PtrsV2ExclusionsApplyDone",
      customerId,
      ptrsId,
      category,
      rowsExcluded: stats.rowsExcluded,
      tookMs,
    });

    return {
      // Backward compatible response shape for FE: persisted is "rows affected"
      persisted: stats.rowsExcluded,
      tookMs,
      stats,
    };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function previewExclusions({
  customerId,
  ptrsId,
  profileId = null, // accepted for API consistency (not required for gov)
  category = "all",
  limit = 10,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const effectiveLimit = Math.min(Number(limit) || 10, 50);

  const started = Date.now();

  slog.info("PTRS v2 exclusions preview: starting", {
    action: "PtrsV2ExclusionsPreviewStart",
    customerId,
    ptrsId,
    category,
    effectiveLimit,
  });

  const sequelize = db?.sequelize;
  if (!sequelize) {
    throw new Error("Database not initialised: db.sequelize missing");
  }

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const stats = { checksRun: 0, rowsExcluded: 0 };
    const result = {
      category,
      counts: {},
      alreadyExcludedCounts: {},
      samples: {},
    };

    if (category === "all" || category === "gov") {
      stats.checksRun += 1;

      const countSql = `
  SELECT
    COUNT(*)::int AS "matchedCount",
    SUM(CASE WHEN COALESCE((s."data"->>'exclude')::boolean, false) = true THEN 1 ELSE 0 END)::int AS "alreadyExcludedCount"
  FROM "tbl_ptrs_stage_row" s
  JOIN "tbl_ptrs_gov_entity_ref" g
    ON (
      (
        NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NOT NULL
        AND NULLIF(regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g'), '') IS NOT NULL
        AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
          = regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g')
      )
      OR
      (
        (
          NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NULL
          OR NULLIF(regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g'), '') IS NULL
        )
        AND regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g')
          = regexp_replace(lower(trim(COALESCE(g."name",''))), '\\s+', ' ', 'g')
        AND trim(COALESCE(s."data"->>'payee_entity_name','')) <> ''
        AND trim(COALESCE(g."name",'')) <> ''
      )
    )
  WHERE
    s."customerId" = :customerId
    AND s."ptrsId" = :ptrsId
    AND s."deletedAt" IS NULL
    AND g."deletedAt" IS NULL
`;

      const [countRows] = await sequelize.query(countSql, {
        replacements: { customerId, ptrsId },
        transaction: t,
      });

      const matched = Number(countRows?.[0]?.matchedCount ?? 0) || 0;
      const alreadyExcluded =
        Number(countRows?.[0]?.alreadyExcludedCount ?? 0) || 0;

      result.counts.gov = matched;
      result.alreadyExcludedCounts.gov = alreadyExcluded;

      const sampleSql = `
        SELECT
          (s."data"->>'row_no')::int AS "row_no",
          s."data"->>'payer_entity_abn' AS "payer_entity_abn",
          s."data"->>'payer_entity_name' AS "payer_entity_name",
          s."data"->>'payee_entity_abn' AS "payee_entity_abn",
          s."data"->>'payee_entity_name' AS "payee_entity_name",
          s."data"->>'invoice_reference_number' AS "invoice_reference_number",
          s."data"->>'payment_date' AS "payment_date",
          s."data"->>'payment_amount' AS "payment_amount",
          (
            'Government entity' ||
            CASE WHEN COALESCE(g."name",'') <> '' THEN ' — ' || g."name" ELSE '' END ||
            CASE WHEN COALESCE(g."category",'') <> '' THEN ' — ' || g."category" ELSE '' END
          )::text AS "exclude_comment",
          COALESCE((s."data"->>'exclude')::boolean, false) AS "alreadyExcluded"
        FROM "tbl_ptrs_stage_row" s
        JOIN "tbl_ptrs_gov_entity_ref" g
          ON (
            (
              NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NOT NULL
              AND NULLIF(regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g'), '') IS NOT NULL
              AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
                = regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g')
            )
            OR
            (
              (
                NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NULL
                OR NULLIF(regexp_replace(COALESCE(g."abn",''), '\\D', '', 'g'), '') IS NULL
              )
              AND regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g')
                = regexp_replace(lower(trim(COALESCE(g."name",''))), '\\s+', ' ', 'g')
              AND trim(COALESCE(s."data"->>'payee_entity_name','')) <> ''
              AND trim(COALESCE(g."name",'')) <> ''
            )
          )
        WHERE
          s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
          AND g."deletedAt" IS NULL
        ORDER BY s."rowNo" ASC
        LIMIT :limit
      `;

      const [sampleRows] = await sequelize.query(sampleSql, {
        replacements: { customerId, ptrsId, limit: effectiveLimit },
        transaction: t,
      });

      result.samples.gov = Array.isArray(sampleRows) ? sampleRows : [];
    }

    if (category === "all" || category === "intra_company") {
      if (!profileId)
        throw new Error("profileId is required for intra_company");
      stats.checksRun += 1;

      const countSql = `
        SELECT
          COUNT(*)::int AS "matchedCount",
          SUM(CASE WHEN COALESCE((s."data"->>'exclude')::boolean, false) = true THEN 1 ELSE 0 END)::int AS "alreadyExcludedCount"
        FROM "tbl_ptrs_stage_row" s
        JOIN "tbl_ptrs_entity_ref" e
          ON e."customerId" = :customerId
          AND e."profileId" = :profileId
          AND e."deletedAt" IS NULL
          AND (
            (
              NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NOT NULL
              AND NULLIF(regexp_replace(COALESCE(e."abn",''), '\\D', '', 'g'), '') IS NOT NULL
              AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
                = regexp_replace(COALESCE(e."abn",''), '\\D', '', 'g')
            )
            OR
            (
              (
                NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NULL
                OR NULLIF(regexp_replace(COALESCE(e."abn",''), '\\D', '', 'g'), '') IS NULL
              )
              AND (
                regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g')
                  = regexp_replace(lower(trim(COALESCE(e."entityName",''))), '\\s+', ' ', 'g')
                OR
                regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g')
                  = regexp_replace(lower(trim(COALESCE(e."payerEntityName",''))), '\\s+', ' ', 'g')
              )
              AND trim(COALESCE(s."data"->>'payee_entity_name','')) <> ''
              AND (
                trim(COALESCE(e."entityName",'')) <> ''
                OR trim(COALESCE(e."payerEntityName",'')) <> ''
              )
            )
          )
        WHERE
          s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
      `;

      const [countRows] = await sequelize.query(countSql, {
        replacements: { customerId, ptrsId, profileId },
        transaction: t,
      });

      const matched = Number(countRows?.[0]?.matchedCount ?? 0) || 0;
      const alreadyExcluded =
        Number(countRows?.[0]?.alreadyExcludedCount ?? 0) || 0;

      result.counts.intra_company = matched;
      result.alreadyExcludedCounts.intra_company = alreadyExcluded;

      const sampleSql = `
        SELECT
          (s."data"->>'row_no')::int AS "row_no",
          s."data"->>'payer_entity_abn' AS "payer_entity_abn",
          s."data"->>'payer_entity_name' AS "payer_entity_name",
          s."data"->>'payee_entity_abn' AS "payee_entity_abn",
          s."data"->>'payee_entity_name' AS "payee_entity_name",
          s."data"->>'invoice_reference_number' AS "invoice_reference_number",
          s."data"->>'payment_date' AS "payment_date",
          s."data"->>'payment_amount' AS "payment_amount",
          (
            'Intra-company' ||
            CASE
              WHEN COALESCE(e."entityName",'') <> '' THEN ' — ' || e."entityName"
              WHEN COALESCE(e."payerEntityName",'') <> '' THEN ' — ' || e."payerEntityName"
              ELSE ''
            END
          )::text AS "exclude_comment",
          COALESCE((s."data"->>'exclude')::boolean, false) AS "alreadyExcluded"
        FROM "tbl_ptrs_stage_row" s
        JOIN "tbl_ptrs_entity_ref" e
          ON e."customerId" = :customerId
          AND e."profileId" = :profileId
          AND e."deletedAt" IS NULL
          AND (
            (
              NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NOT NULL
              AND NULLIF(regexp_replace(COALESCE(e."abn",''), '\\D', '', 'g'), '') IS NOT NULL
              AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
                = regexp_replace(COALESCE(e."abn",''), '\\D', '', 'g')
            )
            OR
            (
              (
                NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NULL
                OR NULLIF(regexp_replace(COALESCE(e."abn",''), '\\D', '', 'g'), '') IS NULL
              )
              AND (
                regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g')
                  = regexp_replace(lower(trim(COALESCE(e."entityName",''))), '\\s+', ' ', 'g')
                OR
                regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g')
                  = regexp_replace(lower(trim(COALESCE(e."payerEntityName",''))), '\\s+', ' ', 'g')
              )
              AND trim(COALESCE(s."data"->>'payee_entity_name','')) <> ''
              AND (
                trim(COALESCE(e."entityName",'')) <> ''
                OR trim(COALESCE(e."payerEntityName",'')) <> ''
              )
            )
          )
        WHERE
          s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
        ORDER BY s."rowNo" ASC
        LIMIT :limit
      `;

      const [sampleRows] = await sequelize.query(sampleSql, {
        replacements: { customerId, ptrsId, profileId, limit: effectiveLimit },
        transaction: t,
      });

      result.samples.intra_company = Array.isArray(sampleRows)
        ? sampleRows
        : [];
    }

    if (category === "all" || category === "employee") {
      if (!profileId) throw new Error("profileId is required for employee");
      stats.checksRun += 1;

      const countSql = `
WITH matched AS (
  SELECT
    s."id" AS stage_id,
    MAX(CASE WHEN COALESCE((s."data"->>'exclude')::boolean, false) = true THEN 1 ELSE 0 END) AS already_excluded
  FROM "tbl_ptrs_stage_row" s
  JOIN "tbl_ptrs_employee_ref" r
    ON r."customerId" = :customerId
    AND r."profileId" = :profileId
    AND r."deletedAt" IS NULL
  WHERE
    s."customerId" = :customerId
    AND s."ptrsId" = :ptrsId
    AND s."deletedAt" IS NULL
    AND (
      (
        NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NOT NULL
        AND NULLIF(regexp_replace(COALESCE(r."abn",''), '\\D', '', 'g'), '') IS NOT NULL
        AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
          = regexp_replace(COALESCE(r."abn",''), '\\D', '', 'g')
      )
      OR
      (
        trim(COALESCE(s."data"->>'payee_entity_name','')) <> ''
        AND trim(COALESCE(r."name",'')) <> ''
        AND (
          regexp_replace(
            regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g'),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          LIKE '%' ||
          regexp_replace(
            regexp_replace(lower(trim(COALESCE(r."name",''))), '\\s+', ' ', 'g'),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          || '%'
          OR
          regexp_replace(
            regexp_replace(
              regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g'),
              '^(er\\s*-\\s*)',
              '',
              'g'
            ),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          LIKE '%' ||
          regexp_replace(
            regexp_replace(
              regexp_replace(lower(trim(COALESCE(r."name",''))), '\\s+', ' ', 'g'),
              '^(er\\s*-\\s*)',
              '',
              'g'
            ),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          || '%'
        )
      )
      OR
      (
        trim(COALESCE(s."data"->>'description','')) <> ''
        AND trim(COALESCE(r."name",'')) <> ''
        AND (
          regexp_replace(
            regexp_replace(lower(trim(COALESCE(s."data"->>'description',''))), '\\s+', ' ', 'g'),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          LIKE '%' ||
          regexp_replace(
            regexp_replace(lower(trim(COALESCE(r."name",''))), '\\s+', ' ', 'g'),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          || '%'
        )
      )
    )
  GROUP BY s."id"
)
SELECT
  COUNT(*)::int AS "matchedCount",
  SUM(already_excluded)::int AS "alreadyExcludedCount"
FROM matched;
`;

      const [countRows] = await sequelize.query(countSql, {
        replacements: { customerId, ptrsId, profileId },
        transaction: t,
      });

      result.counts.employee = Number(countRows?.[0]?.matchedCount ?? 0) || 0;
      result.alreadyExcludedCounts.employee =
        Number(countRows?.[0]?.alreadyExcludedCount ?? 0) || 0;

      const sampleSql = `
WITH matched AS (
  SELECT
    s."id" AS stage_id,
    MIN(r."name") AS matched_name
  FROM "tbl_ptrs_stage_row" s
  JOIN "tbl_ptrs_employee_ref" r
    ON r."customerId" = :customerId
    AND r."profileId" = :profileId
    AND r."deletedAt" IS NULL
  WHERE
    s."customerId" = :customerId
    AND s."ptrsId" = :ptrsId
    AND s."deletedAt" IS NULL
    AND (
      (
        NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), '') IS NOT NULL
        AND NULLIF(regexp_replace(COALESCE(r."abn",''), '\\D', '', 'g'), '') IS NOT NULL
        AND regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g')
          = regexp_replace(COALESCE(r."abn",''), '\\D', '', 'g')
      )
      OR
      (
        trim(COALESCE(s."data"->>'payee_entity_name','')) <> ''
        AND trim(COALESCE(r."name",'')) <> ''
        AND (
          regexp_replace(
            regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g'),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          LIKE '%' ||
          regexp_replace(
            regexp_replace(lower(trim(COALESCE(r."name",''))), '\\s+', ' ', 'g'),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          || '%'
          OR
          regexp_replace(
            regexp_replace(
              regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g'),
              '^(er\\s*-\\s*)',
              '',
              'g'
            ),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          LIKE '%' ||
          regexp_replace(
            regexp_replace(
              regexp_replace(lower(trim(COALESCE(r."name",''))), '\\s+', ' ', 'g'),
              '^(er\\s*-\\s*)',
              '',
              'g'
            ),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          || '%'
        )
      )
      OR
      (
        trim(COALESCE(s."data"->>'description','')) <> ''
        AND trim(COALESCE(r."name",'')) <> ''
        AND (
          regexp_replace(
            regexp_replace(lower(trim(COALESCE(s."data"->>'description',''))), '\\s+', ' ', 'g'),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          LIKE '%' ||
          regexp_replace(
            regexp_replace(lower(trim(COALESCE(r."name",''))), '\\s+', ' ', 'g'),
            '[^a-z0-9 ]',
            '',
            'g'
          )
          || '%'
        )
      )
    )
  GROUP BY s."id"
)
SELECT
  (s."data"->>'row_no')::int AS "row_no",
  s."data"->>'payee_entity_abn' AS "payee_entity_abn",
  s."data"->>'payee_entity_name' AS "payee_entity_name",
  s."data"->>'description' AS "description",
  s."data"->>'invoice_reference_number' AS "invoice_reference_number",
  s."data"->>'payment_date' AS "payment_date",
  s."data"->>'payment_amount' AS "payment_amount",
  (
    'Employee / payroll' ||
    CASE WHEN COALESCE(m.matched_name,'') <> '' THEN ' — ' || m.matched_name ELSE '' END
  )::text AS "exclude_comment",
  COALESCE((s."data"->>'exclude')::boolean, false) AS "alreadyExcluded"
FROM matched m
JOIN "tbl_ptrs_stage_row" s
  ON s."id" = m.stage_id
ORDER BY s."rowNo" ASC
LIMIT :limit;
`;

      const [sampleRows] = await sequelize.query(sampleSql, {
        replacements: { customerId, ptrsId, profileId, limit: effectiveLimit },
        transaction: t,
      });

      result.samples.employee = Array.isArray(sampleRows) ? sampleRows : [];
    }

    // Keyword exclusions preview (profile-scoped keyword list)
    if (category === "all" || category === "keyword") {
      if (!profileId) throw new Error("profileId is required for keyword");
      stats.checksRun += 1;

      const countSql = `
        SELECT
          COUNT(*)::int AS "matchedCount",
          SUM(CASE WHEN COALESCE((s."data"->>'exclude')::boolean, false) = true THEN 1 ELSE 0 END)::int AS "alreadyExcludedCount"
        FROM "tbl_ptrs_stage_row" s
        JOIN "tbl_ptrs_exclusion_keyword_customer_ref" k
          ON k."customerId" = :customerId
          AND k."profileId" = :profileId
          AND k."deletedAt" IS NULL
          AND (
            s."data"->>'payee_entity_name' ILIKE '%'||k."keyword"||'%' OR
            s."data"->>'description' ILIKE '%'||k."keyword"||'%' OR
            s."data"->>'invoice_reference_number' ILIKE '%'||k."keyword"||'%' OR
            s."data"->>'account_name' ILIKE '%'||k."keyword"||'%' OR
            s."data"->>'account_code' ILIKE '%'||k."keyword"||'%'
          )
        WHERE
          s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
      `;

      const [countRows] = await sequelize.query(countSql, {
        replacements: { customerId, ptrsId, profileId },
        transaction: t,
      });

      const matched = Number(countRows?.[0]?.matchedCount ?? 0) || 0;
      const alreadyExcluded =
        Number(countRows?.[0]?.alreadyExcludedCount ?? 0) || 0;

      result.counts.keyword = matched;
      result.alreadyExcludedCounts.keyword = alreadyExcluded;

      const sampleSql = `
        SELECT
          (s."data"->>'row_no')::int AS "row_no",
          s."data"->>'payee_entity_abn' AS "payee_entity_abn",
          s."data"->>'payee_entity_name' AS "payee_entity_name",
          s."data"->>'description' AS "description",
          s."data"->>'invoice_reference_number' AS "invoice_reference_number",
          s."data"->>'account_code' AS "account_code",
          s."data"->>'account_name' AS "account_name",
          s."data"->>'payment_date' AS "payment_date",
          s."data"->>'payment_amount' AS "payment_amount",
          ('Keyword exclusion — ' || k."keyword")::text AS "exclude_comment",
          k."keyword" AS "matched_keyword",
          COALESCE((s."data"->>'exclude')::boolean, false) AS "alreadyExcluded"
        FROM "tbl_ptrs_stage_row" s
        JOIN "tbl_ptrs_exclusion_keyword_customer_ref" k
          ON k."customerId" = :customerId
          AND k."profileId" = :profileId
          AND k."deletedAt" IS NULL
          AND (
            s."data"->>'payee_entity_name' ILIKE '%'||k."keyword"||'%' OR
            s."data"->>'description' ILIKE '%'||k."keyword"||'%' OR
            s."data"->>'invoice_reference_number' ILIKE '%'||k."keyword"||'%' OR
            s."data"->>'account_name' ILIKE '%'||k."keyword"||'%' OR
            s."data"->>'account_code' ILIKE '%'||k."keyword"||'%'
          )
        WHERE
          s."customerId" = :customerId
          AND s."ptrsId" = :ptrsId
          AND s."deletedAt" IS NULL
        ORDER BY s."rowNo" ASC
        LIMIT :limit
      `;

      const [sampleRows] = await sequelize.query(sampleSql, {
        replacements: { customerId, ptrsId, profileId, limit: effectiveLimit },
        transaction: t,
      });

      result.samples.keyword = Array.isArray(sampleRows) ? sampleRows : [];
    }

    if (category === "all" || category === "partial") {
      stats.checksRun += 1;

      const countSql = `
        WITH base AS (
          SELECT
            s."rowNo",
            s."data",
            COALESCE((s."data"->>'exclude')::boolean, false) AS already_excluded,
            CASE
              WHEN COALESCE(s."data"->>'payment_date','') LIKE '%/%'
                THEN to_date(s."data"->>'payment_date', 'DD/MM/YYYY')
              WHEN COALESCE(s."data"->>'payment_date','') LIKE '%-%'
                THEN (s."data"->>'payment_date')::date
              ELSE NULL
            END AS pay_date,
            NULLIF(regexp_replace(COALESCE(s."data"->>'payment_amount',''), '[^0-9\\.-]', '', 'g'), '')::numeric AS pay_amt_raw,
            NULLIF(regexp_replace(COALESCE(s."data"->>'invoice_amount',''), '[^0-9\\.-]', '', 'g'), '')::numeric AS inv_amt_raw,
            COALESCE(
              NULLIF(regexp_replace(COALESCE(s."data"->>'payer_entity_abn',''), '\\D', '', 'g'), ''),
              regexp_replace(lower(trim(COALESCE(s."data"->>'payer_entity_name',''))), '\\s+', ' ', 'g')
            ) AS payer_key,
            COALESCE(
              NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), ''),
              regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g')
            ) AS payee_key,
            regexp_replace(lower(trim(COALESCE(s."data"->>'invoice_reference_number',''))), '\\s+', ' ', 'g') AS inv_ref_norm,
            regexp_replace(lower(trim(COALESCE(s."data"->>'account_code',''))), '\\s+', ' ', 'g') AS acct_code_norm
          FROM "tbl_ptrs_stage_row" s
          WHERE
            s."customerId" = :customerId
            AND s."ptrsId" = :ptrsId
            AND s."deletedAt" IS NULL
        ),
        keyed AS (
          SELECT
            b.*,
            abs(COALESCE(b.pay_amt_raw, 0)) AS pay_amt,
            abs(COALESCE(b.inv_amt_raw, 0)) AS inv_amt,
            CASE
              WHEN COALESCE(b.inv_ref_norm,'') <> '' AND b.inv_ref_norm <> 'multiple'
                THEN (b.payer_key || '|' || b.payee_key || '|' || b.inv_ref_norm)
              ELSE (b.payer_key || '|' || b.acct_code_norm)
            END AS group_key,
            CASE
              WHEN COALESCE(b.inv_ref_norm,'') <> '' AND b.inv_ref_norm <> 'multiple'
                THEN false
              ELSE true
            END AS is_multiple_mode
          FROM base b
        ),
        grouped AS (
          SELECT
            k.*,
            COUNT(*) OVER (PARTITION BY k.group_key) AS grp_count
          FROM keyed k
          WHERE k.group_key IS NOT NULL AND k.group_key <> '|'
        ),
        eligible AS (
          SELECT
            g.*,
            SUM(g.pay_amt_raw) OVER (PARTITION BY g.group_key) AS net_sum,
            COUNT(DISTINCT SIGN(g.pay_amt_raw)) OVER (PARTITION BY g.group_key) AS sign_count
          FROM grouped g
          WHERE g.grp_count > 1
        ),
        filtered AS (
          SELECT *
          FROM eligible
          WHERE
            net_sum <> 0
            AND sign_count = 1
        ),
        ordered AS (
          SELECT
            e.*,
            ROW_NUMBER() OVER (
              PARTITION BY e.group_key
              ORDER BY e.pay_date NULLS LAST, e."rowNo" ASC
            ) AS rn_asc,
            ROW_NUMBER() OVER (
              PARTITION BY e.group_key
              ORDER BY e.pay_date NULLS LAST, e."rowNo" DESC
            ) AS rn_desc,
            SUM(abs(COALESCE(e.pay_amt_raw, 0))) OVER (
              PARTITION BY e.group_key
              ORDER BY e.pay_date NULLS LAST, e."rowNo" ASC
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_paid
          FROM filtered e
        ),
        keep_row AS (
          SELECT DISTINCT ON (o.group_key)
            o.group_key,
            COALESCE(
              (
                SELECT oo."rowNo"
                FROM ordered oo
                WHERE
                  oo.group_key = o.group_key
                  AND oo.inv_amt > 0
                  AND oo.cum_paid >= oo.inv_amt
                ORDER BY oo.rn_asc ASC
                LIMIT 1
              ),
              (
                SELECT oo."rowNo"
                FROM ordered oo
                WHERE oo.group_key = o.group_key
                ORDER BY oo.rn_desc ASC
                LIMIT 1
              )
            ) AS keep_row_no,
            COALESCE(
              (
                SELECT oo.inv_amt
                FROM ordered oo
                WHERE
                  oo.group_key = o.group_key
                  AND oo.inv_amt > 0
                  AND oo.cum_paid >= oo.inv_amt
                ORDER BY oo.rn_asc ASC
                LIMIT 1
              ),
              0
            ) AS keep_inv_amt
          FROM ordered o
        ),
        candidates AS (
          SELECT
            o.*,
            k.keep_row_no,
            k.keep_inv_amt
          FROM ordered o
          JOIN keep_row k ON k.group_key = o.group_key
          WHERE o."rowNo" <> k.keep_row_no
        )
        SELECT
          COUNT(*)::int AS "matchedCount",
          SUM(CASE WHEN c.already_excluded = true THEN 1 ELSE 0 END)::int AS "alreadyExcludedCount"
        FROM candidates c
      `;

      const [countRows] = await sequelize.query(countSql, {
        replacements: { customerId, ptrsId },
        transaction: t,
      });

      result.counts.partial = Number(countRows?.[0]?.matchedCount ?? 0) || 0;
      result.alreadyExcludedCounts.partial =
        Number(countRows?.[0]?.alreadyExcludedCount ?? 0) || 0;

      const sampleSql = `
        WITH base AS (
          SELECT
            s."rowNo",
            s."data",
            COALESCE((s."data"->>'exclude')::boolean, false) AS already_excluded,
            CASE
              WHEN COALESCE(s."data"->>'payment_date','') LIKE '%/%'
                THEN to_date(s."data"->>'payment_date', 'DD/MM/YYYY')
              WHEN COALESCE(s."data"->>'payment_date','') LIKE '%-%'
                THEN (s."data"->>'payment_date')::date
              ELSE NULL
            END AS pay_date,
            NULLIF(regexp_replace(COALESCE(s."data"->>'payment_amount',''), '[^0-9\\.-]', '', 'g'), '')::numeric AS pay_amt_raw,
            NULLIF(regexp_replace(COALESCE(s."data"->>'invoice_amount',''), '[^0-9\\.-]', '', 'g'), '')::numeric AS inv_amt_raw,
            COALESCE(
              NULLIF(regexp_replace(COALESCE(s."data"->>'payer_entity_abn',''), '\\D', '', 'g'), ''),
              regexp_replace(lower(trim(COALESCE(s."data"->>'payer_entity_name',''))), '\\s+', ' ', 'g')
            ) AS payer_key,
            COALESCE(
              NULLIF(regexp_replace(COALESCE(s."data"->>'payee_entity_abn',''), '\\D', '', 'g'), ''),
              regexp_replace(lower(trim(COALESCE(s."data"->>'payee_entity_name',''))), '\\s+', ' ', 'g')
            ) AS payee_key,
            regexp_replace(lower(trim(COALESCE(s."data"->>'invoice_reference_number',''))), '\\s+', ' ', 'g') AS inv_ref_norm,
            regexp_replace(lower(trim(COALESCE(s."data"->>'account_code',''))), '\\s+', ' ', 'g') AS acct_code_norm
          FROM "tbl_ptrs_stage_row" s
          WHERE
            s."customerId" = :customerId
            AND s."ptrsId" = :ptrsId
            AND s."deletedAt" IS NULL
        ),
        keyed AS (
          SELECT
            b.*,
            abs(COALESCE(b.pay_amt_raw, 0)) AS pay_amt,
            abs(COALESCE(b.inv_amt_raw, 0)) AS inv_amt,
            CASE
              WHEN COALESCE(b.inv_ref_norm,'') <> '' AND b.inv_ref_norm <> 'multiple'
                THEN (b.payer_key || '|' || b.payee_key || '|' || b.inv_ref_norm)
              ELSE (b.payer_key || '|' || b.acct_code_norm)
            END AS group_key,
            CASE
              WHEN COALESCE(b.inv_ref_norm,'') <> '' AND b.inv_ref_norm <> 'multiple'
                THEN false
              ELSE true
            END AS is_multiple_mode
          FROM base b
        ),
        grouped AS (
          SELECT
            k.*,
            COUNT(*) OVER (PARTITION BY k.group_key) AS grp_count
          FROM keyed k
          WHERE k.group_key IS NOT NULL AND k.group_key <> '|'
        ),
        eligible AS (
          SELECT
            g.*,
            SUM(g.pay_amt_raw) OVER (PARTITION BY g.group_key) AS net_sum,
            COUNT(DISTINCT SIGN(g.pay_amt_raw)) OVER (PARTITION BY g.group_key) AS sign_count
          FROM grouped g
          WHERE g.grp_count > 1
        ),
        filtered AS (
          SELECT *
          FROM eligible
          WHERE
            net_sum <> 0
            AND sign_count = 1
        ),
        ordered AS (
          SELECT
            e.*,
            ROW_NUMBER() OVER (
              PARTITION BY e.group_key
              ORDER BY e.pay_date NULLS LAST, e."rowNo" ASC
            ) AS rn_asc,
            ROW_NUMBER() OVER (
              PARTITION BY e.group_key
              ORDER BY e.pay_date NULLS LAST, e."rowNo" DESC
            ) AS rn_desc,
            SUM(abs(COALESCE(e.pay_amt_raw, 0))) OVER (
              PARTITION BY e.group_key
              ORDER BY e.pay_date NULLS LAST, e."rowNo" ASC
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cum_paid
          FROM filtered e
        ),
        keep_row AS (
          SELECT DISTINCT ON (o.group_key)
            o.group_key,
            COALESCE(
              (
                SELECT oo."rowNo"
                FROM ordered oo
                WHERE
                  oo.group_key = o.group_key
                  AND oo.inv_amt > 0
                  AND oo.cum_paid >= oo.inv_amt
                ORDER BY oo.rn_asc ASC
                LIMIT 1
              ),
              (
                SELECT oo."rowNo"
                FROM ordered oo
                WHERE oo.group_key = o.group_key
                ORDER BY oo.rn_desc ASC
                LIMIT 1
              )
            ) AS keep_row_no,
            COALESCE(
              (
                SELECT oo.inv_amt
                FROM ordered oo
                WHERE
                  oo.group_key = o.group_key
                  AND oo.inv_amt > 0
                  AND oo.cum_paid >= oo.inv_amt
                ORDER BY oo.rn_asc ASC
                LIMIT 1
              ),
              0
            ) AS keep_inv_amt
          FROM ordered o
        ),
        candidates AS (
          SELECT
            o.*,
            k.keep_row_no,
            k.keep_inv_amt
          FROM ordered o
          JOIN keep_row k ON k.group_key = o.group_key
          WHERE o."rowNo" <> k.keep_row_no
        )
        SELECT
          (c."data"->>'row_no')::int AS "row_no",
          c."data"->>'payer_entity_abn' AS "payer_entity_abn",
          c."data"->>'payer_entity_name' AS "payer_entity_name",
          c."data"->>'payee_entity_abn' AS "payee_entity_abn",
          c."data"->>'payee_entity_name' AS "payee_entity_name",
          c."data"->>'invoice_reference_number' AS "invoice_reference_number",
          c."data"->>'account_code' AS "account_code",
          c."data"->>'payment_date' AS "payment_date",
          c."data"->>'payment_amount' AS "payment_amount",
          (
            'Partial payment — earlier instalment (kept row ' ||
            c.keep_row_no::text ||
            CASE WHEN c.is_multiple_mode THEN ', multiple-mode' ELSE '' END ||
            ')'
          )::text AS "exclude_comment",
          c.already_excluded AS "alreadyExcluded"
        FROM candidates c
        ORDER BY c.pay_date NULLS LAST, c."rowNo" ASC
        LIMIT :limit
      `;

      const [sampleRows] = await sequelize.query(sampleSql, {
        replacements: { customerId, ptrsId, limit: effectiveLimit },
        transaction: t,
      });

      result.samples.partial = Array.isArray(sampleRows) ? sampleRows : [];
    }

    if (category === "all" || category === "prepaid") {
      stats.checksRun += 1;

      const countSql = `
    SELECT
      COUNT(*)::int AS "matchedCount",
      SUM(CASE WHEN COALESCE((s."data"->>'exclude')::boolean, false) = true THEN 1 ELSE 0 END)::int AS "alreadyExcludedCount"
    FROM "tbl_ptrs_stage_row" s
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND (
        COALESCE(s."data"->>'payment_terms','') ILIKE '%prepaid%'
        OR COALESCE(s."data"->>'payment_terms','') ILIKE '%pre-pay%'
        OR COALESCE(s."data"->>'payment_terms','') ILIKE '%prepay%'
        OR COALESCE(s."data"->>'description','') ILIKE '%prepaid%'
        OR COALESCE(s."data"->>'description','') ILIKE '%pre-pay%'
        OR COALESCE(s."data"->>'description','') ILIKE '%prepay%'
      )
  `;

      const [countRows] = await sequelize.query(countSql, {
        replacements: { customerId, ptrsId },
        transaction: t,
        type: QueryTypes.SELECT,
      });

      const matchedCount = Number(countRows?.matchedCount ?? 0) || 0;
      const alreadyExcludedCount =
        Number(countRows?.alreadyExcludedCount ?? 0) || 0;

      result.counts.prepaid = matchedCount;
      result.alreadyExcludedCounts.prepaid = alreadyExcludedCount;

      const sampleSql = `
    SELECT
      s."rowNo" AS "rowNo",
      s."data"->>'payee_entity_name' AS "payee_entity_name",
      s."data"->>'payee_entity_abn' AS "payee_entity_abn",
      s."data"->>'invoice_reference_number' AS "invoice_reference_number",
      s."data"->>'account_code' AS "account_code",
      s."data"->>'payment_date' AS "payment_date",
      s."data"->>'payment_amount' AS "payment_amount",
      s."data"->>'payment_terms' AS "payment_terms",
      s."data"->>'description' AS "description",
      COALESCE((s."data"->>'exclude')::boolean, false) AS "alreadyExcluded"
    FROM "tbl_ptrs_stage_row" s
    WHERE
      s."customerId" = :customerId
      AND s."ptrsId" = :ptrsId
      AND s."deletedAt" IS NULL
      AND (
        COALESCE(s."data"->>'payment_terms','') ILIKE '%prepaid%'
        OR COALESCE(s."data"->>'payment_terms','') ILIKE '%pre-pay%'
        OR COALESCE(s."data"->>'payment_terms','') ILIKE '%prepay%'
        OR COALESCE(s."data"->>'description','') ILIKE '%prepaid%'
        OR COALESCE(s."data"->>'description','') ILIKE '%pre-pay%'
        OR COALESCE(s."data"->>'description','') ILIKE '%prepay%'
      )
    ORDER BY s."rowNo" ASC
    LIMIT :limit
  `;

      const sampleRows = await sequelize.query(sampleSql, {
        replacements: { customerId, ptrsId, limit: effectiveLimit },
        transaction: t,
        type: QueryTypes.SELECT,
      });

      result.samples.prepaid = Array.isArray(sampleRows) ? sampleRows : [];
    }

    await t.commit();

    const tookMs = Date.now() - started;

    slog.info("PTRS v2 exclusions preview: done", {
      action: "PtrsV2ExclusionsPreviewDone",
      customerId,
      ptrsId,
      category,
      tookMs,
    });

    return { tookMs, stats, result };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function listKeywordExclusions({ customerId, profileId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!profileId) throw new Error("profileId is required");

  const sequelize = db?.sequelize;
  if (!sequelize)
    throw new Error("Database not initialised: db.sequelize missing");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const rows = await sequelize.query(
      `
        SELECT
          k."id",
          k."keyword",
          k."field",
          k."matchType",
          k."notes",
          k."createdAt",
          k."updatedAt"
        FROM "tbl_ptrs_exclusion_keyword_customer_ref" k
        WHERE
          k."customerId" = :customerId
          AND k."profileId" = :profileId
          AND k."deletedAt" IS NULL
        ORDER BY lower(k."keyword") ASC
      `,
      {
        type: QueryTypes.SELECT,
        replacements: { customerId, profileId },
        transaction: t,
      },
    );

    await t.commit();
    return Array.isArray(rows) ? rows : [];
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function createKeywordExclusion({
  customerId,
  profileId,
  keyword,
  field,
  matchType,
  notes,
  userId,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!keyword) throw new Error("keyword is required");
  if (!field) throw new Error("field is required");
  if (!matchType) throw new Error("matchType is required");

  const cleaned = normaliseKeyword(keyword);
  if (!cleaned) throw new Error("keyword is required");
  if (cleaned.length > 200) throw new Error("keyword is too long (max 200)");

  const sequelize = db?.sequelize;
  if (!sequelize)
    throw new Error("Database not initialised: db.sequelize missing");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const Model = getKeywordModel(sequelize);
    if (!Model) {
      throw new Error(
        "Exclusion keyword model not registered in Sequelize: PtrsExclusionKeywordCustomerRef",
      );
    }

    const cleanedKeyword = cleaned;
    const trimmedUserId = userId ? String(userId).slice(0, 10) : null;

    // Find existing keyword (case-insensitive), scoped to THIS customer/profile, incl soft-deleted
    const existing = await Model.findOne({
      where: {
        customerId,
        profileId,
        [Op.and]: [
          sequelize.where(
            sequelize.fn("lower", sequelize.col("keyword")),
            sequelize.fn("lower", cleanedKeyword),
          ),
        ],
      },
      transaction: t,
      paranoid: false,
    });

    if (existing) {
      existing.keyword = cleanedKeyword;
      existing.field = field;
      existing.matchType = matchType;
      existing.notes = notes ?? null;
      existing.deletedAt = null;
      existing.updatedBy = trimmedUserId;

      await existing.save({ transaction: t });
      await t.commit();

      return {
        id: existing.id,
        keyword: existing.keyword,
        field: existing.field,
        matchType: existing.matchType,
        notes: existing.notes,
        createdAt: existing.createdAt,
        updatedAt: existing.updatedAt,
      };
    }

    const created = await Model.create(
      {
        customerId,
        profileId,
        keyword: cleanedKeyword,
        field,
        matchType,
        notes: notes ?? null,
        createdBy: trimmedUserId,
        updatedBy: trimmedUserId,
      },
      { transaction: t },
    );

    await t.commit();

    return {
      id: created.id,
      keyword: created.keyword,
      field: created.field,
      matchType: created.matchType,
      notes: created.notes,
      createdAt: created.createdAt,
      updatedAt: created.updatedAt,
    };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function updateKeywordExclusion({
  customerId,
  profileId,
  keywordId,
  keyword,
  field,
  matchType,
  notes = null,
  userId,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!keywordId) throw new Error("keywordId is required");
  if (!field) throw new Error("field is required");
  if (!matchType) throw new Error("matchType is required");

  const sequelize = db?.sequelize;
  if (!sequelize)
    throw new Error("Database not initialised: db.sequelize missing");

  const Model = getKeywordModel(sequelize);
  if (!Model)
    throw new Error(
      "Keyword model not registered in Sequelize: PtrsExclusionKeywordCustomerRef",
    );

  // RLS: ensure we can SEE the row (customer context is set on the same connection/transaction)
  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const row = await Model.findOne({
      where: {
        id: keywordId,
        customerId,
        profileId,
        deletedAt: null,
      },
      transaction: t,
    });

    if (!row) {
      const err = new Error("Keyword not found");
      err.statusCode = 404;
      throw err;
    }

    row.keyword = normaliseKeyword(keyword);
    row.field = field;
    row.matchType = matchType;
    row.notes = notes ?? null;
    if (userId) row.updatedBy = String(userId).slice(0, 10);

    await row.save({ transaction: t });
    await t.commit();

    return typeof row.toJSON === "function" ? row.toJSON() : row;
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function deleteKeywordExclusion({
  customerId,
  profileId,
  keywordId,
  userId,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!keywordId) throw new Error("keywordId is required");

  const sequelize = db?.sequelize;
  if (!sequelize)
    throw new Error("Database not initialised: db.sequelize missing");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const row = await sequelize.query(
      `
        UPDATE "tbl_ptrs_exclusion_keyword_customer_ref"
        SET
          "deletedAt" = now(),
          "updatedAt" = now(),
          "updatedBy" = :userId
        WHERE
          "id" = :keywordId
          AND "customerId" = :customerId
          AND "profileId" = :profileId
          AND "deletedAt" IS NULL
        RETURNING "id"
      `,
      {
        type: QueryTypes.SELECT,
        replacements: { keywordId, customerId, profileId, userId },
        transaction: t,
      },
    );

    await t.commit();
    return { deleted: true, id: row?.[0]?.id || keywordId };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

module.exports = {
  applyExclusionsAndPersist,
  previewExclusions,
  listKeywordExclusions,
  createKeywordExclusion,
  updateKeywordExclusion,
  deleteKeywordExclusion,
};

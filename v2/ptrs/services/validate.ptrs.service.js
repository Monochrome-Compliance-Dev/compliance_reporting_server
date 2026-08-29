const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  buildPaymentObservationsCte,
  getPaymentObservationReplacements,
  setPaymentObservationWorkMem,
} = require("./payment-observations.ptrs.service");

module.exports = {
  validate,
  getValidate,
  buildProcessValidateSummarySql,
  getProcessValidateSummary,
  getValidateSummary,
  setStageRowExclusion,
  getStageRow,
};

function buildValidDateSql(valueSql) {
  const text = `BTRIM(COALESCE(${valueSql}, ''))`;
  const isoYear = `substring(${text} from 1 for 4)::int`;
  const isoMonth = `substring(${text} from 6 for 2)::int`;
  const isoDay = `substring(${text} from 9 for 2)::int`;
  const auParts = `regexp_match(${text}, '^(\\d{1,2})/(\\d{1,2})/(\\d{4})$')`;
  const auDay = `(${auParts})[1]::int`;
  const auMonth = `(${auParts})[2]::int`;
  const auYear = `(${auParts})[3]::int`;
  const daysInMonth = (year, month) => `CASE
    WHEN ${month} IN (4, 6, 9, 11) THEN 30
    WHEN ${month} = 2 THEN CASE
      WHEN (${year} % 400 = 0) OR (${year} % 4 = 0 AND ${year} % 100 <> 0)
        THEN 29
      ELSE 28
    END
    ELSE 31
  END`;

  return `CASE
    WHEN ${text} ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN
      ${isoMonth} BETWEEN 1 AND 12
      AND ${isoDay} BETWEEN 1 AND ${daysInMonth(isoYear, isoMonth)}
    WHEN ${text} ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN
      ${auMonth} BETWEEN 1 AND 12
      AND ${auDay} BETWEEN 1 AND ${daysInMonth(auYear, auMonth)}
    ELSE false
  END`;
}

function buildComparableDateSql(valueSql) {
  const text = `BTRIM(COALESCE(${valueSql}, ''))`;
  return `CASE
    WHEN ${text} ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN
      substring(${text} from 1 for 4)::int * 10000
      + substring(${text} from 6 for 2)::int * 100
      + substring(${text} from 9 for 2)::int
    WHEN ${text} ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN
      (regexp_match(${text}, '^(\\d{1,2})/(\\d{1,2})/(\\d{4})$'))[3]::int * 10000
      + (regexp_match(${text}, '^(\\d{1,2})/(\\d{1,2})/(\\d{4})$'))[2]::int * 100
      + (regexp_match(${text}, '^(\\d{1,2})/(\\d{1,2})/(\\d{4})$'))[1]::int
    ELSE NULL
  END`;
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

    const result = await queryBoundedValidation({
      customerId,
      ptrsId,
      mode,
      transaction: t,
    });
    await t.commit();
    return result;
  } catch (err) {
    try {
      await t.rollback();
    } catch (_) {
      // ignore rollback errors
    }
    throw err;
  }
}

function buildProcessValidateSummarySql() {
  const paymentDateSql = `source.payment_date_raw`;
  const referenceDateSql = `source.reference_date_raw`;
  const invoiceDateSql = `source.invoice_date_raw`;
  const paymentDateValidSql = buildValidDateSql(paymentDateSql);
  const referenceDateValidSql = buildValidDateSql(referenceDateSql);
  const invoiceDateValidSql = buildValidDateSql(invoiceDateSql);

  return `
    WITH ${buildPaymentObservationsCte()},
    validation_source AS MATERIALIZED (
      SELECT
        'payment-observation:' || invoice."id" AS "observationId",
        invoice."id" AS "primarySourceStageRowId",
        invoice."id" AS "sourceInvoiceStageRowId",
        ARRAY[invoice."id", invoice.settlement_stage_row_id]
          AS "sourceStageRowIds",
        invoice."rowNo",
        invoice_payload."data"->>'payee_entity_abn' AS payee_abn_raw,
        invoice_payload."data"->>'payer_entity_abn' AS payer_abn_raw,
        invoice.settlement_payment_date::text AS payment_date_raw,
        invoice_payload."data"->>'payment_time_reference_date'
          AS reference_date_raw,
        invoice_payload."data"->>'payment_time_reference_kind'
          AS reference_kind_raw,
        invoice_payload."data"->>'invoice_issue_date' AS invoice_date_raw,
        invoice_payload."data"->>'payment_amount' AS payment_amount_raw,
        invoice_payload."data"->>'payment_term_days' AS payment_term_days_raw,
        invoice_payload."data"->>'payment_time_days' AS payment_time_days_raw,
        invoice_payload."data"->>'vlookup' AS vlookup_raw,
        invoice_payload."data"->>'company_code' AS company_code_raw,
        invoice_payload."data"->>'invoice_reference_number'
          AS invoice_reference_number_raw,
        invoice_payload."data"->'is_small_business'
          AS is_small_business_value
      FROM payment_observation_accounting_keys invoice
      JOIN "tbl_ptrs_stage_row" invoice_payload
        ON invoice_payload."id" = invoice."id"
       AND invoice_payload."customerId" = :customerId
       AND invoice_payload."ptrsId" = :ptrsId
       AND invoice_payload."deletedAt" IS NULL

      UNION ALL

      SELECT
        'payment-observation:' || direct."id" AS "observationId",
        direct."id" AS "primarySourceStageRowId",
        NULL::varchar AS "sourceInvoiceStageRowId",
        ARRAY[direct."id"] AS "sourceStageRowIds",
        direct."rowNo",
        direct_payload."data"->>'payee_entity_abn' AS payee_abn_raw,
        direct_payload."data"->>'payer_entity_abn' AS payer_abn_raw,
        direct_payload."data"->>'payment_date' AS payment_date_raw,
        direct_payload."data"->>'payment_time_reference_date'
          AS reference_date_raw,
        direct_payload."data"->>'payment_time_reference_kind'
          AS reference_kind_raw,
        direct_payload."data"->>'invoice_issue_date' AS invoice_date_raw,
        direct_payload."data"->>'payment_amount' AS payment_amount_raw,
        direct_payload."data"->>'payment_term_days' AS payment_term_days_raw,
        direct_payload."data"->>'payment_time_days' AS payment_time_days_raw,
        direct_payload."data"->>'vlookup' AS vlookup_raw,
        direct_payload."data"->>'company_code' AS company_code_raw,
        direct_payload."data"->>'invoice_reference_number'
          AS invoice_reference_number_raw,
        direct_payload."data"->'is_small_business'
          AS is_small_business_value
      FROM payment_observation_direct_keys direct
      JOIN "tbl_ptrs_stage_row" direct_payload
        ON direct_payload."id" = direct."id"
       AND direct_payload."customerId" = :customerId
       AND direct_payload."ptrsId" = :ptrsId
       AND direct_payload."deletedAt" IS NULL
    ),
    classified AS (
      SELECT
        source."observationId",
        source."primarySourceStageRowId",
        source."sourceInvoiceStageRowId",
        source."sourceStageRowIds",
        source."rowNo",
        source.is_small_business_value,
        source.payee_abn_raw,
        source.payer_abn_raw,
        source.payment_date_raw,
        source.reference_date_raw,
        source.reference_kind_raw,
        source.invoice_date_raw,
        source.payment_amount_raw,
        source.payment_term_days_raw,
        source.payment_time_days_raw,
        NULLIF(
          regexp_replace(
            COALESCE(source.payee_abn_raw, ''),
            '\\D',
            '',
            'g'
          ),
          ''
        ) AS payee_abn,
        NULLIF(
          regexp_replace(
            COALESCE(source.payer_abn_raw, ''),
            '\\D',
            '',
            'g'
          ),
          ''
        ) AS payer_abn,
        BTRIM(COALESCE(source.payment_date_raw, '')) AS payment_date,
        BTRIM(COALESCE(source.reference_date_raw, ''))
          AS reference_date,
        BTRIM(COALESCE(source.invoice_date_raw, ''))
          AS invoice_date,
        BTRIM(COALESCE(source.payment_amount_raw, ''))
          AS payment_amount,
        BTRIM(COALESCE(source.payment_term_days_raw, ''))
          AS payment_term_days,
        BTRIM(COALESCE(source.payment_time_days_raw, ''))
          AS payment_time_days,
        ${paymentDateValidSql} AS payment_date_valid,
        ${referenceDateValidSql} AS reference_date_valid,
        ${invoiceDateValidSql} AS invoice_date_valid,
        ${buildComparableDateSql(paymentDateSql)} AS payment_date_sort,
        ${buildComparableDateSql(referenceDateSql)} AS reference_date_sort,
        CASE
          WHEN NULLIF(BTRIM(source.vlookup_raw), '') IS NOT NULL
            THEN 'vlookup:' || BTRIM(source.vlookup_raw)
          ELSE 'cc:' || BTRIM(COALESCE(source.company_code_raw, ''))
            || '|abn:' || regexp_replace(
              COALESCE(source.payee_abn_raw, ''),
              '\\D',
              '',
              'g'
            )
            || '|ref:' || BTRIM(
              COALESCE(source.invoice_reference_number_raw, '')
            )
            || '|inv:' || BTRIM(
              COALESCE(source.invoice_date_raw, '')
            )
            || '|amt:' || BTRIM(
              COALESCE(source.payment_amount_raw, '')
            )
        END AS duplicate_key
      FROM validation_source source
    ),
    numbered AS MATERIALIZED (
      SELECT
        classified.*,
        ROW_NUMBER() OVER (
          PARTITION BY duplicate_key
          ORDER BY "rowNo", "observationId"
        ) AS duplicate_number,
        FIRST_VALUE("rowNo") OVER (
          PARTITION BY duplicate_key
          ORDER BY "rowNo", "observationId"
        ) AS duplicate_of_row_no
      FROM classified
    ),
    validation_counts AS (
      SELECT
        COUNT(*)::int AS "totalRows",
        COUNT(*) FILTER (WHERE payee_abn IS NULL)::int
          AS "missingPayeeAbnCount",
        COUNT(*) FILTER (
          WHERE payee_abn IS NOT NULL AND payee_abn !~ '^\\d{11}$'
        )::int AS "invalidPayeeAbnCount",
        COUNT(*) FILTER (WHERE payer_abn IS NULL)::int
          AS "missingPayerAbnCount",
        COUNT(*) FILTER (
          WHERE payer_abn IS NOT NULL AND payer_abn !~ '^\\d{11}$'
        )::int AS "invalidPayerAbnCount",
        COUNT(*) FILTER (WHERE reference_date = '')::int
          AS "missingPaymentTimeReferenceDateCount",
        COUNT(*) FILTER (
          WHERE reference_date <> '' AND NOT reference_date_valid
        )::int AS "invalidPaymentTimeReferenceDateCount",
        COUNT(*) FILTER (
          WHERE invoice_date <> '' AND NOT invoice_date_valid
        )::int AS "invalidInvoiceIssueDateCount",
        COUNT(*) FILTER (WHERE payment_date = '')::int
          AS "missingPaymentDateCount",
        COUNT(*) FILTER (
          WHERE payment_date <> '' AND NOT payment_date_valid
        )::int AS "invalidPaymentDateCount",
        COUNT(*) FILTER (
          WHERE payment_date_valid
            AND reference_date_valid
            AND payment_date_sort < reference_date_sort
        )::int AS "paymentBeforeInvoiceCount",
        COUNT(*) FILTER (WHERE payment_amount = '')::int
          AS "missingPaymentAmountCount",
        COUNT(*) FILTER (
          WHERE payment_amount <> ''
            AND replace(payment_amount, ',', '') !~ :numericPattern
        )::int AS "invalidPaymentAmountCount",
        COUNT(*) FILTER (WHERE duplicate_number > 1)::int
          AS "duplicatesSuspectedCount",
        COUNT(*) FILTER (
          WHERE is_small_business_value IS NULL
            OR is_small_business_value = 'null'::jsonb
        )::int AS "smallBusinessUnknownCount",
        COUNT(*) FILTER (WHERE payment_term_days = '')::int
          AS missing_term_days,
        COUNT(*) FILTER (
          WHERE payment_term_days <> ''
            AND (
              payment_term_days !~ :numericPattern
              OR CASE
                WHEN payment_term_days ~ :numericPattern
                  THEN payment_term_days::numeric < 0
                    OR payment_term_days::numeric <>
                      trunc(payment_term_days::numeric)
                ELSE false
              END
            )
        )::int AS invalid_term_days,
        COUNT(*) FILTER (WHERE payment_time_days = '')::int
          AS missing_time_days,
        COUNT(*) FILTER (
          WHERE payment_time_days <> ''
            AND (
              payment_time_days !~ :numericPattern
              OR CASE
                WHEN payment_time_days ~ :numericPattern
                  THEN payment_time_days::numeric < 0
                    OR payment_time_days::numeric <>
                      trunc(payment_time_days::numeric)
                ELSE false
              END
            )
        )::int AS invalid_time_days
      FROM numbered
    ),
    issue_rows AS MATERIALIZED (
      SELECT
        numbered."observationId",
        numbered."rowNo",
        issue.priority,
        issue.severity,
        jsonb_build_object(
          'paymentObservationId', numbered."observationId",
          'stageRowId', COALESCE(
            numbered."primarySourceStageRowId",
            numbered."sourceInvoiceStageRowId"
          ),
          'sourceStageRowIds', to_jsonb(numbered."sourceStageRowIds"),
          'rowNo', numbered."rowNo",
          'code', issue.code,
          'message', issue.message
        ) || issue.extra AS issue
      FROM numbered
      CROSS JOIN LATERAL (
        SELECT *
        FROM (VALUES
          (10, 'blocker', 'PAYEE_ABN_MISSING',
            numbered.payee_abn IS NULL,
            'Missing payee_entity_abn',
            jsonb_build_object('field', 'payee_entity_abn')),
          (20, 'blocker', 'PAYEE_ABN_INVALID',
            numbered.payee_abn IS NOT NULL
              AND numbered.payee_abn !~ '^\\d{11}$',
            'payee_entity_abn is not a valid 11-digit ABN',
            jsonb_build_object(
              'field', 'payee_entity_abn',
              'value', numbered.payee_abn_raw
            )),
          (30, 'blocker', 'PAYER_ABN_MISSING',
            numbered.payer_abn IS NULL,
            'Missing payer_entity_abn',
            jsonb_build_object('field', 'payer_entity_abn')),
          (40, 'blocker', 'PAYER_ABN_INVALID',
            numbered.payer_abn IS NOT NULL
              AND numbered.payer_abn !~ '^\\d{11}$',
            'payer_entity_abn is not a valid 11-digit ABN',
            jsonb_build_object(
              'field', 'payer_entity_abn',
              'value', numbered.payer_abn_raw
            )),
          (50, 'blocker', 'PAYMENT_DATE_MISSING',
            numbered.payment_date = '',
            'Missing payment_date',
            jsonb_build_object('field', 'payment_date')),
          (60, 'blocker', 'PAYMENT_DATE_INVALID',
            numbered.payment_date <> '' AND NOT numbered.payment_date_valid,
            'payment_date is not a valid date (expected yyyy-mm-dd or dd/mm/yyyy)',
            jsonb_build_object(
              'field', 'payment_date',
              'value', numbered.payment_date_raw
            )),
          (70, 'blocker', 'PAYMENT_TIME_REFERENCE_DATE_MISSING',
            numbered.reference_date = '',
            'Missing payment_time_reference_date',
            jsonb_build_object('field', 'payment_time_reference_date')),
          (80, 'blocker', 'PAYMENT_TIME_REFERENCE_DATE_INVALID',
            numbered.reference_date <> '' AND NOT numbered.reference_date_valid,
            'payment_time_reference_date is not a valid date (expected yyyy-mm-dd or dd/mm/yyyy)',
            jsonb_build_object(
              'field', 'payment_time_reference_date',
              'value', numbered.reference_date_raw
            )),
          (90, 'blocker', 'PAYMENT_AMOUNT_MISSING',
            numbered.payment_amount = '',
            'Missing payment_amount',
            jsonb_build_object('field', 'payment_amount')),
          (100, 'blocker', 'PAYMENT_AMOUNT_INVALID',
            numbered.payment_amount <> ''
              AND replace(numbered.payment_amount, ',', '') !~ :numericPattern,
            'payment_amount is not a valid number',
            jsonb_build_object(
              'field', 'payment_amount',
              'value', numbered.payment_amount_raw
            )),
          (110, 'blocker', 'PAYMENT_TERM_DAYS_MISSING',
            numbered.payment_term_days = '',
            'Missing payment_term_days',
            jsonb_build_object('field', 'payment_term_days')),
          (120, 'blocker', 'PAYMENT_TERM_DAYS_INVALID',
            numbered.payment_term_days <> '' AND (
              numbered.payment_term_days !~ :numericPattern
              OR CASE WHEN numbered.payment_term_days ~ :numericPattern THEN
                numbered.payment_term_days::numeric < 0
                OR numbered.payment_term_days::numeric <>
                  trunc(numbered.payment_term_days::numeric)
              ELSE false END
            ),
            'payment_term_days is not a valid non-negative integer',
            jsonb_build_object(
              'field', 'payment_term_days',
              'value', numbered.payment_term_days_raw
            )),
          (130, 'blocker', 'PAYMENT_TIME_DAYS_MISSING',
            numbered.payment_time_days = '',
            'Missing payment_time_days',
            jsonb_build_object('field', 'payment_time_days')),
          (140, 'blocker', 'PAYMENT_TIME_DAYS_INVALID',
            numbered.payment_time_days <> '' AND (
              numbered.payment_time_days !~ :numericPattern
              OR CASE WHEN numbered.payment_time_days ~ :numericPattern THEN
                numbered.payment_time_days::numeric < 0
                OR numbered.payment_time_days::numeric <>
                  trunc(numbered.payment_time_days::numeric)
              ELSE false END
            ),
            'payment_time_days is not a valid non-negative integer',
            jsonb_build_object(
              'field', 'payment_time_days',
              'value', numbered.payment_time_days_raw
            )),
          (150, 'blocker', 'SMALL_BUSINESS_MISSING',
            numbered.is_small_business_value IS NULL
              OR numbered.is_small_business_value = 'null'::jsonb,
            'Missing is_small_business (required for report/metrics)',
            jsonb_build_object('field', 'is_small_business')),
          (10, 'warning', 'INVOICE_ISSUE_DATE_INVALID',
            numbered.invoice_date <> '' AND NOT numbered.invoice_date_valid,
            'invoice_issue_date is not a valid date (expected yyyy-mm-dd or dd/mm/yyyy)',
            jsonb_build_object(
              'field', 'invoice_issue_date',
              'value', numbered.invoice_date_raw
            )),
          (20, 'warning', 'PAYMENT_BEFORE_REFERENCE_DATE',
            numbered.payment_date_valid
              AND numbered.reference_date_valid
              AND numbered.payment_date_sort < numbered.reference_date_sort,
            'payment_date is earlier than payment_time_reference_date (check for credit notes/adjustments)',
            jsonb_build_object(
              'payment_date', numbered.payment_date_raw,
              'payment_time_reference_date', numbered.reference_date_raw,
              'payment_time_reference_kind', numbered.reference_kind_raw
            )),
          (30, 'warning', 'DUPLICATE_SUSPECTED',
            numbered.duplicate_number > 1,
            'Duplicate-suspected row based on key heuristic',
            jsonb_build_object(
              'duplicateOfRowNo', numbered.duplicate_of_row_no,
              'key', numbered.duplicate_key
            ))
        ) AS issues(priority, severity, code, matches, message, extra)
        WHERE issues.matches
      ) issue
    ),
    ranked_issues AS (
      SELECT
        issue_rows.*,
        ROW_NUMBER() OVER (
          PARTITION BY severity
          ORDER BY "rowNo", "observationId", priority
        ) AS sample_rank
      FROM issue_rows
    ),
    issue_summary AS (
      SELECT
        COUNT(*) FILTER (WHERE severity = 'blocker')::int
          AS "blockerCount",
        COUNT(*) FILTER (WHERE severity = 'warning')::int
          AS "warningCount",
        COALESCE(
          jsonb_agg(issue ORDER BY "rowNo", "observationId", priority)
            FILTER (
              WHERE severity = 'blocker' AND sample_rank <= :sampleLimit
            ),
          '[]'::jsonb
        ) AS blockers,
        COALESCE(
          jsonb_agg(issue ORDER BY "rowNo", "observationId", priority)
            FILTER (
              WHERE severity = 'warning' AND sample_rank <= :sampleLimit
            ),
          '[]'::jsonb
        ) AS warnings
      FROM ranked_issues
    )
    SELECT validation_counts.*, issue_summary.*
    FROM validation_counts
    CROSS JOIN issue_summary
  `;
}

function parseIssueArray(value) {
  if (Array.isArray(value)) return value;
  if (typeof value !== "string") return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_) {
    return [];
  }
}

async function queryBoundedValidation({
  customerId,
  ptrsId,
  mode,
  transaction,
}) {
  const numericPattern =
    "^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$";
  await setPaymentObservationWorkMem({ transaction });
  const rows = await db.sequelize.query(buildProcessValidateSummarySql(), {
    transaction,
    replacements: {
      ...getPaymentObservationReplacements({ customerId, ptrsId }),
      numericPattern,
      sampleLimit: 200,
    },
    type: db.sequelize.QueryTypes.SELECT,
  });
  const counts = rows?.[0] || {};
  const blockerCount = Number(counts.blockerCount) || 0;
  const warningCount = Number(counts.warningCount) || 0;
  return {
    status:
      blockerCount > 0
        ? "BLOCKED"
        : warningCount > 0
          ? "PASSED_WITH_WARNINGS"
          : "PASSED",
    ptrsId,
    mode,
    counts: {
      totalRows: Number(counts.totalRows) || 0,
      paymentObservationRows: Number(counts.totalRows) || 0,
      excludedRows: 0,
      blockers: blockerCount,
      warnings: warningCount,
      missingPayeeAbnCount: Number(counts.missingPayeeAbnCount) || 0,
      invalidPayeeAbnCount: Number(counts.invalidPayeeAbnCount) || 0,
      missingPayerAbnCount: Number(counts.missingPayerAbnCount) || 0,
      invalidPayerAbnCount: Number(counts.invalidPayerAbnCount) || 0,
      missingPaymentTimeReferenceDateCount:
        Number(counts.missingPaymentTimeReferenceDateCount) || 0,
      invalidPaymentTimeReferenceDateCount:
        Number(counts.invalidPaymentTimeReferenceDateCount) || 0,
      invalidInvoiceIssueDateCount:
        Number(counts.invalidInvoiceIssueDateCount) || 0,
      missingPaymentDateCount: Number(counts.missingPaymentDateCount) || 0,
      invalidPaymentDateCount: Number(counts.invalidPaymentDateCount) || 0,
      paymentBeforeInvoiceCount: Number(counts.paymentBeforeInvoiceCount) || 0,
      missingPaymentAmountCount: Number(counts.missingPaymentAmountCount) || 0,
      invalidPaymentAmountCount: Number(counts.invalidPaymentAmountCount) || 0,
      duplicatesSuspectedCount: Number(counts.duplicatesSuspectedCount) || 0,
      smallBusinessUnknownCount: Number(counts.smallBusinessUnknownCount) || 0,
    },
    blockers: parseIssueArray(counts.blockers),
    warnings: parseIssueArray(counts.warnings),
  };
}

async function getProcessValidateSummary({ customerId, ptrsId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const result = await queryBoundedValidation({
      customerId,
      ptrsId,
      mode: "read",
      transaction: t,
    });
    await t.commit();
    return result;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

// Aggregated Validate summary endpoint for PTRS v2
async function getValidateSummary({ customerId, ptrsId, profileId = null }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    await setPaymentObservationWorkMem({ transaction: t });
    const paymentObservationCte = buildPaymentObservationsCte();
    const replacements = getPaymentObservationReplacements({
      customerId,
      ptrsId,
    });

    // Exclusion logic: canonical exclude flag OR rule meta flag
    const excludedExpr = `(
      COALESCE((data->>'exclude_from_metrics')::boolean, false) = true
      OR COALESCE((meta->'rules'->>'exclude')::boolean, false) = true
    )`;

    // Payment identity is established by the derived relation, not by raw
    // Stage classification flags.
    const tradeCreditIncludedExpr = `(NOT ${excludedExpr})`;
    const tradeCreditExcludedExpr = "false";

    const countsResult = await db.sequelize.query(
      `
      WITH ${paymentObservationCte}
      SELECT
        (SELECT COUNT(*)::int FROM payment_observation_source_rows) AS "stageRowCount",
        (SELECT COUNT(*)::int FROM payment_observation_source_rows WHERE excluded) AS "excludedRowCount",
        (SELECT COUNT(*)::int FROM payment_observation_source_rows WHERE NOT excluded) AS "includedRowCount",
        COUNT(*)::int AS "paymentObservationCount",

        SUM(CASE WHEN ${tradeCreditIncludedExpr} THEN 1 ELSE 0 END)::int AS "tradeCreditIncludedCount",
        SUM(CASE WHEN ${tradeCreditExcludedExpr} THEN 1 ELSE 0 END)::int AS "tradeCreditExcludedCount",

        SUM(CASE WHEN ${tradeCreditIncludedExpr} AND COALESCE((data->>'is_small_business')::boolean, NULL) = true THEN 1 ELSE 0 END)::int AS "sbTrueCount",
        SUM(CASE WHEN ${tradeCreditIncludedExpr} AND COALESCE((data->>'is_small_business')::boolean, NULL) = false THEN 1 ELSE 0 END)::int AS "sbFalseCount",
        SUM(CASE WHEN ${tradeCreditIncludedExpr} AND (data->>'is_small_business') IS NULL THEN 1 ELSE 0 END)::int AS "sbUnknownCount"
      FROM payment_observations
      `,
      {
        transaction: t,
        replacements,
        type: db.sequelize.QueryTypes.SELECT,
      },
    );

    const countsRow = Array.isArray(countsResult) ? countsResult[0] : null;

    const stageRowCount = Number(countsRow?.stageRowCount) || 0;
    const excludedRowCount = Number(countsRow?.excludedRowCount) || 0;
    const includedRowCount = Number(countsRow?.includedRowCount) || 0;
    const paymentObservationCount =
      Number(countsRow?.paymentObservationCount) || 0;
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
      WITH ${paymentObservationCte}
      SELECT
        CASE
          WHEN (data->>'payment_time_reference_kind') IS NULL OR (data->>'payment_time_reference_kind') = '' THEN 'missing'
          ELSE (data->>'payment_time_reference_kind')
        END AS kind,
        COUNT(*)::int AS count
      FROM payment_observations
      WHERE ${tradeCreditIncludedExpr}
      GROUP BY 1
      ORDER BY 1
      `,
      {
        transaction: t,
        replacements,
        type: db.sequelize.QueryTypes.SELECT,
      },
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
      ([kind, count]) => ({ kind, count }),
    );

    const missingTimeRows = await db.sequelize.query(
      `
      WITH ${paymentObservationCte}
      SELECT
        SUM(CASE WHEN (data->>'payment_date') IS NULL OR (data->>'payment_date') = '' THEN 1 ELSE 0 END)::int AS missing_payment_date,
        SUM(CASE WHEN (data->>'payment_time_reference_date') IS NULL OR (data->>'payment_time_reference_date') = '' THEN 1 ELSE 0 END)::int AS missing_reference_date,
        SUM(CASE WHEN ((data->>'payment_date') IS NULL OR (data->>'payment_date') = '') AND ((data->>'payment_time_reference_date') IS NULL OR (data->>'payment_time_reference_date') = '') THEN 1 ELSE 0 END)::int AS missing_both
      FROM payment_observations
      WHERE ${tradeCreditIncludedExpr}
      `,
      {
        transaction: t,
        replacements,
        type: db.sequelize.QueryTypes.SELECT,
      },
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
      WITH ${paymentObservationCte}
      SELECT
        data->>'invoice_reference_number' AS invoice_reference_number,
        data->>'payment_date' AS payment_date,
        data->>'payment_time_reference_date' AS payment_time_reference_date,
        data->>'payment_time_reference_kind' AS payment_time_reference_kind,
        (data->>'payment_time_days')::int AS payment_time_days
      FROM payment_observations
      WHERE ${tradeCreditIncludedExpr}
      ORDER BY "rowNo" ASC
      LIMIT 5
      `,
      {
        transaction: t,
        replacements,
        type: db.sequelize.QueryTypes.SELECT,
      },
    );

    // Payment terms dedupe table
    const paymentTermsRows = await db.sequelize.query(
      `
      WITH ${paymentObservationCte}
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
      FROM payment_observations
      WHERE ${tradeCreditIncludedExpr}
      GROUP BY 1,2,3
      ORDER BY 4 DESC, 1 ASC
      `,
      {
        transaction: t,
        replacements,
        type: db.sequelize.QueryTypes.SELECT,
      },
    );

    const unmappedRawRows = await db.sequelize.query(
      `
      WITH ${paymentObservationCte}
      SELECT
        (data->>'payment_term') AS raw,
        COUNT(*)::int AS count
      FROM payment_observations
      WHERE ${tradeCreditIncludedExpr}
        AND (data->>'payment_term') IS NOT NULL
        AND (data->>'payment_term') <> ''
        AND ((data->>'payment_term_days') IS NULL OR (data->>'payment_term_days') = '')
      GROUP BY 1
      ORDER BY 2 DESC, 1 ASC
      `,
      {
        transaction: t,
        replacements,
        type: db.sequelize.QueryTypes.SELECT,
      },
    );

    const unmappedRawValues = (unmappedRawRows || [])
      .map((r) => String(r.raw))
      .filter((v) => v != null && v.trim() !== "");

    const unmappedCount = (unmappedRawRows || []).reduce(
      (acc, r) => acc + (Number(r.count) || 0),
      0,
    );

    // Canonical missing counts within the trade credit included population
    const missingCanonRows = await db.sequelize.query(
      `
      WITH ${paymentObservationCte}
      SELECT
        SUM(CASE WHEN (data->>'payment_term_days') IS NULL OR (data->>'payment_term_days') = '' THEN 1 ELSE 0 END)::int AS missing_payment_term_days,
        SUM(CASE WHEN (data->>'is_small_business') IS NULL OR (data->>'is_small_business') = '' THEN 1 ELSE 0 END)::int AS missing_is_small_business,
        SUM(CASE WHEN (data->>'payment_time_days') IS NULL OR (data->>'payment_time_days') = '' THEN 1 ELSE 0 END)::int AS missing_payment_time_days,
        SUM(CASE WHEN (data->>'payment_amount') IS NULL OR (data->>'payment_amount') = '' THEN 1 ELSE 0 END)::int AS missing_payment_amount,
        SUM(CASE WHEN (data->>'payment_time_reference_date') IS NULL OR (data->>'payment_time_reference_date') = '' THEN 1 ELSE 0 END)::int AS missing_payment_time_reference_date,
        SUM(CASE WHEN (data->>'payment_date') IS NULL OR (data->>'payment_date') = '' THEN 1 ELSE 0 END)::int AS missing_payment_date
      FROM payment_observations
      WHERE ${tradeCreditIncludedExpr}
      `,
      {
        transaction: t,
        replacements,
        type: db.sequelize.QueryTypes.SELECT,
      },
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
      missingCanonRow?.missing_payment_term_days,
    );
    pushMissing(
      "is_small_business",
      missingCanonRow?.missing_is_small_business,
    );
    pushMissing(
      "payment_time_days",
      missingCanonRow?.missing_payment_time_days,
    );
    pushMissing("payment_amount", missingCanonRow?.missing_payment_amount);
    pushMissing(
      "payment_time_reference_date",
      missingCanonRow?.missing_payment_time_reference_date,
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
        paymentObservationCount,
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
            "unambiguous derived payment observation with no Stage exclusion",
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

async function setStageRowExclusion({
  customerId,
  ptrsId,
  stageRowId,
  exclude,
  comment = null,
  userId = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!stageRowId) throw new Error("stageRowId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const row = await db.PtrsStageRow.findOne({
      where: { id: stageRowId, customerId, ptrsId, deletedAt: null },
      transaction: t,
    });

    if (!row) {
      const e = new Error("Stage row not found");
      e.statusCode = 404;
      throw e;
    }

    const nextData = { ...(row.data || {}) };
    const nextMeta = { ...(row.meta || {}) };

    // Canonical exclusion fields used by staging/metrics.
    nextData.exclude_from_metrics = exclude === true;
    nextData.exclude_set_at =
      exclude === true ? new Date().toISOString() : null;
    nextData.exclude_set_by = exclude === true ? userId || null : null;
    nextData.exclude_comment =
      exclude === true
        ? comment && String(comment).trim()
          ? String(comment).trim()
          : "Excluded by user"
        : null;

    // Legacy/compat flag used by some older helpers.
    nextMeta.rules = { ...(nextMeta.rules || {}) };
    nextMeta.rules.exclude = exclude === true;
    nextMeta.rules.exclude_comment = nextData.exclude_comment;

    // Minimal audit trail in meta (append-only)
    const ev = {
      at: new Date().toISOString(),
      by: userId || null,
      exclude: exclude === true,
      comment: nextData.exclude_comment || null,
    };

    const hist = Array.isArray(nextMeta.exclusions) ? nextMeta.exclusions : [];
    nextMeta.exclusions = hist.concat([ev]).slice(-25); // keep last 25

    row.data = nextData;
    row.meta = nextMeta;

    await row.save({ transaction: t });

    await t.commit();

    return {
      stageRowId,
      ptrsId,
      excluded: exclude === true,
      comment: nextData.exclude_comment,
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

/**
 * Fetch a single staged row by id within the customer + ptrs scope.
 * Used by Validate UI to show the exact offending record.
 */
async function getStageRow({ customerId, ptrsId, stageRowId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  if (!stageRowId) throw new Error("stageRowId is required");

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const row = await db.PtrsStageRow.findOne({
      where: { id: stageRowId, customerId, ptrsId, deletedAt: null },
      transaction: t,
    });

    if (!row) {
      const e = new Error("Stage row not found");
      e.statusCode = 404;
      throw e;
    }

    await t.commit();

    return {
      id: row.id,
      customerId: row.customerId,
      ptrsId: row.ptrsId,
      rowNo: row.rowNo,
      data: row.data || {},
      errors: row.errors || null,
      meta: row.meta || {},
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
      deletedAt: row.deletedAt || null,
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

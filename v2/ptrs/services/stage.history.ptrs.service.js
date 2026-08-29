const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  appendTransformationHistoryEventsSql,
} = require("./stage.transformation-history");
const {
  buildPaymentObservationsCte,
  getPaymentObservationReplacements,
  setPaymentObservationWorkMem,
} = require("./payment-observations.ptrs.service");

function buildStageTransformationHistorySql() {
  const nextMetaSql = appendTransformationHistoryEventsSql(
    'stage_row."meta"',
    "grouped.events",
  );

  return `
    WITH ${buildPaymentObservationsCte()},
    stage_history_source AS MATERIALIZED (
      SELECT
        stage_row."id",
        stage_row."rowNo",
        stage_row."datasetId",
        stage_row."canonicalRevisionId",
        stage_row."data",
        stage_row."meta"
      FROM "tbl_ptrs_stage_row" stage_row
      WHERE stage_row."customerId" = :customerId
        AND stage_row."ptrsId" = :ptrsId
        AND stage_row."deletedAt" IS NULL
    ),
    exclusion_events AS (
      SELECT
        source."id" AS target_id,
        jsonb_build_object(
          'key', 'exclusion:' || rtrim(
            translate(
              replace(
                encode(convert_to(comment.value, 'UTF8'), 'base64'),
                E'\\n',
                ''
              ),
              '/+',
              '_-'
            ),
            '='
          ),
          'kind', 'exclusion',
          'comment', comment.value,
          'sourceStageRowIds', jsonb_build_array(source."id"),
          'targetStageRowIds', jsonb_build_array(source."id"),
          'details', jsonb_build_object(
            'reasons', CASE
              WHEN jsonb_typeof(source."meta"->'exclusions'->'reasons') = 'array'
                THEN source."meta"->'exclusions'->'reasons'
              ELSE '[]'::jsonb
            END
          )
        ) AS event,
        '1:exclusion:' || comment.ordinality::text AS sort_key
      FROM stage_history_source source
      CROSS JOIN LATERAL jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(source."meta"->'exclusions'->'comments') = 'array'
            THEN source."meta"->'exclusions'->'comments'
          ELSE '[]'::jsonb
        END
      ) WITH ORDINALITY comment(value, ordinality)
      WHERE jsonb_typeof(source."meta"->'exclusions'->'comments') = 'array'
        AND jsonb_array_length(source."meta"->'exclusions'->'comments') > 0
    ),
    payment_term_events AS (
      SELECT
        source."id" AS target_id,
        jsonb_build_object(
          'key', 'payment-term-change:'
            || COALESCE(
              NULLIF(
                source."data"->>'contract_po_payment_terms_effective_changed_at',
                ''
              ),
              'unknown'
            )
            || ':' || (source."data"->>'contract_po_payment_terms_effective'),
          'kind', 'payment_term_override',
          'comment', 'Payment term overridden to '
            || (source."data"->>'contract_po_payment_terms_effective')
            || ' by supplier term change effective on '
            || COALESCE(
              NULLIF(
                source."data"->>'contract_po_payment_terms_effective_changed_at',
                ''
              ),
              'an unspecified date'
            ),
          'sourceStageRowIds', jsonb_build_array(source."id"),
          'targetStageRowIds', jsonb_build_array(source."id"),
          'details', jsonb_build_object(
            'effectiveTerm',
              source."data"->'contract_po_payment_terms_effective',
            'effectiveDate',
              source."data"->'contract_po_payment_terms_effective_changed_at',
            'source', 'TERM_CHANGES'
          )
        ) AS event,
        '2:payment-term' AS sort_key
      FROM stage_history_source source
      WHERE source."data"->>'contract_po_payment_terms_effective_source'
          = 'TERM_CHANGES'
        AND NULLIF(
          source."data"->>'contract_po_payment_terms_effective',
          ''
        ) IS NOT NULL
    ),
    payment_time_events AS (
      SELECT
        source."id" AS target_id,
        jsonb_build_object(
          'key', 'payment-time:'
            || (source."data"->>'payment_time_reference_kind')
            || ':' || COALESCE(
              NULLIF(source."data"->>'payment_time_reference_date', ''),
              'unknown'
            )
            || ':' || (source."data"->>'payment_time_days'),
          'kind', 'payment_time',
          'comment', 'Payment-time reference chosen from '
            || replace(
              source."data"->>'payment_time_reference_kind',
              '_',
              ' '
            )
            || ' (' || COALESCE(
              NULLIF(source."data"->>'payment_time_reference_date', ''),
              'date unavailable'
            )
            || '); derived payment time '
            || (source."data"->>'payment_time_days') || ' day(s)',
          'sourceStageRowIds', jsonb_build_array(source."id"),
          'targetStageRowIds', jsonb_build_array(source."id"),
          'details', jsonb_build_object(
            'referenceKind', source."data"->'payment_time_reference_kind',
            'referenceDate', source."data"->'payment_time_reference_date',
            'paymentTimeDays', source."data"->'payment_time_days'
          )
        ) AS event,
        '3:payment-time' AS sort_key
      FROM stage_history_source source
      WHERE source."data"->'payment_time_days' IS NOT NULL
        AND source."data"->'payment_time_days' <> 'null'::jsonb
        AND NULLIF(source."data"->>'payment_time_reference_kind', '') IS NOT NULL
    ),
    direct_observation_events AS (
      SELECT
        direct."id" AS target_id,
        jsonb_build_object(
          'key', 'direct-payment-observation:payment-observation:' || direct."id",
          'kind', 'payment_observation_direct',
          'comment', 'Stage row ' || COALESCE(
            direct."rowNo"::text,
            direct."id"
          ) || ' produced a direct payment observation without SAP event reconstruction',
          'sourceStageRowIds', jsonb_build_array(direct."id"),
          'targetStageRowIds', jsonb_build_array(direct."id"),
          'details', jsonb_build_object(
            'observationId', 'payment-observation:' || direct."id",
            'sourceDatasetId', direct."datasetId",
            'canonicalRevisionId', direct."canonicalRevisionId"
          )
        ) AS event,
        '4:direct:' || direct."id" AS sort_key
      FROM payment_observation_direct_keys direct
    ),
    anchor_matches AS MATERIALIZED (
      SELECT
        invoice."id" AS invoice_id,
        invoice."rowNo" AS invoice_row_no,
        invoice.settlement_stage_row_id AS settlement_id,
        invoice.settlement_row_no,
        invoice.company_code,
        invoice.source_account_code,
        invoice.clearing_document
      FROM payment_observation_accounting_keys invoice
    ),
    anchor_events AS (
      SELECT
        target.target_id,
        jsonb_build_object(
          'key', 'payment-observation-anchor:' || anchor.invoice_id
            || ':' || anchor.settlement_id,
          'kind', 'payment_observation_anchor',
          'comment', 'ZP row ' || COALESCE(
            anchor.settlement_row_no::text,
            anchor.settlement_id
          ) || ' used as payment anchor for RE row ' || COALESCE(
            anchor.invoice_row_no::text,
            anchor.invoice_id
          ),
          'sourceStageRowIds', jsonb_build_array(anchor.settlement_id),
          'targetStageRowIds', jsonb_build_array(anchor.invoice_id),
          'details', jsonb_build_object(
            'companyCode', anchor.company_code,
            'sourceAccountCode', anchor.source_account_code,
            'clearingDocument', anchor.clearing_document,
            'observationId', 'payment-observation:' || anchor.invoice_id
          )
        ) AS event,
        '5:anchor:' || anchor.invoice_id || ':' || anchor.settlement_id AS sort_key
      FROM anchor_matches anchor
      CROSS JOIN LATERAL (
        VALUES (anchor.invoice_id), (anchor.settlement_id)
      ) target(target_id)
    ),
    earlytrade_matches AS MATERIALIZED (
      SELECT
        match.invoice_id,
        match.invoice_row_no,
        match.earlytrade_id,
        match.earlytrade_row_no,
        match.company_code,
        match.source_account_code,
        match.clearing_document,
        match.description_reference
      FROM payment_observation_earlytrade_matches match
    ),
    earlytrade_events AS (
      SELECT
        earlytrade.earlytrade_id AS target_id,
        jsonb_build_object(
          'key', 'earlytrade-observation-omission:' || earlytrade.invoice_id
            || ':' || earlytrade.earlytrade_id,
          'kind', 'earlytrade_match',
          'comment', 'ET row ' || COALESCE(
            earlytrade.earlytrade_row_no::text,
            earlytrade.earlytrade_id
          ) || ' matched to RE row ' || COALESCE(
            earlytrade.invoice_row_no::text,
            earlytrade.invoice_id
          ) || ' using Company Code + Account + Clearing Document + Reference',
          'sourceStageRowIds', jsonb_build_array(earlytrade.earlytrade_id),
          'targetStageRowIds', jsonb_build_array(earlytrade.invoice_id),
          'details', jsonb_build_object(
            'companyCode', earlytrade.company_code,
            'sourceAccountCode', earlytrade.source_account_code,
            'clearingDocument', earlytrade.clearing_document,
            'descriptionReference', earlytrade.description_reference
          )
        ) AS event,
        '6:earlytrade-match:' || earlytrade.invoice_id
          || ':' || earlytrade.earlytrade_id AS sort_key
      FROM earlytrade_matches earlytrade
      UNION ALL
      SELECT
        earlytrade.invoice_id AS target_id,
        jsonb_build_object(
          'key', 'earlytrade-observation-omission:' || earlytrade.invoice_id
            || ':' || earlytrade.earlytrade_id,
          'kind', 'payment_observation_omission',
          'comment', 'RE row ' || COALESCE(
            earlytrade.invoice_row_no::text,
            earlytrade.invoice_id
          ) || ' omitted from derived payment observations because matched ET row '
            || COALESCE(
              earlytrade.earlytrade_row_no::text,
              earlytrade.earlytrade_id
            ) || ' treatment applies',
          'sourceStageRowIds', jsonb_build_array(earlytrade.earlytrade_id),
          'targetStageRowIds', jsonb_build_array(earlytrade.invoice_id),
          'details', jsonb_build_object(
            'companyCode', earlytrade.company_code,
            'sourceAccountCode', earlytrade.source_account_code,
            'clearingDocument', earlytrade.clearing_document,
            'descriptionReference', earlytrade.description_reference
          )
        ) AS event,
        '7:earlytrade-omission:' || earlytrade.invoice_id
          || ':' || earlytrade.earlytrade_id AS sort_key
      FROM earlytrade_matches earlytrade
    ),
    all_events AS (
      SELECT * FROM exclusion_events
      UNION ALL SELECT * FROM payment_term_events
      UNION ALL SELECT * FROM payment_time_events
      UNION ALL SELECT * FROM direct_observation_events
      UNION ALL SELECT * FROM anchor_events
      UNION ALL SELECT * FROM earlytrade_events
    ),
    grouped AS NOT MATERIALIZED (
      SELECT
        all_events.target_id,
        jsonb_agg(all_events.event ORDER BY all_events.sort_key) AS events
      FROM all_events
      GROUP BY all_events.target_id
    ),
    prepared AS NOT MATERIALIZED (
      SELECT
        grouped.target_id,
        ${nextMetaSql} AS next_meta
      FROM grouped
      JOIN stage_history_source stage_row
        ON stage_row."id" = grouped.target_id
    ),
    updated AS (
      UPDATE "tbl_ptrs_stage_row" stage_row
      SET "meta" = prepared.next_meta, "updatedAt" = now()
      FROM prepared
      WHERE stage_row."id" = prepared.target_id
        AND stage_row."customerId" = :customerId
        AND stage_row."ptrsId" = :ptrsId
        AND stage_row."deletedAt" IS NULL
        AND stage_row."meta" IS DISTINCT FROM prepared.next_meta
      RETURNING stage_row."id"
    )
    SELECT
      (SELECT COUNT(*)::int FROM updated) AS "rowsUpdated",
      (
        (SELECT COUNT(*) FROM payment_observation_accounting_keys)
        + (SELECT COUNT(*) FROM payment_observation_direct_keys)
      )::int AS "paymentObservationLinks",
      (SELECT COUNT(*)::int FROM earlytrade_matches) AS "earlytradeMatches"
  `;
}

async function recordStageTransformationHistory({
  customerId,
  ptrsId,
  transaction: suppliedTransaction = null,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const transaction =
    suppliedTransaction || (await beginTransactionWithCustomerContext(customerId));
  const ownsTransaction = !suppliedTransaction;

  try {
    await setPaymentObservationWorkMem({ transaction });
    const rows = await db.sequelize.query(buildStageTransformationHistorySql(), {
      transaction,
      replacements: getPaymentObservationReplacements({ customerId, ptrsId }),
      type: db.sequelize.QueryTypes.SELECT,
    });
    if (ownsTransaction) await transaction.commit();
    return rows?.[0] || {
      rowsUpdated: 0,
      paymentObservationLinks: 0,
      earlytradeMatches: 0,
    };
  } catch (error) {
    if (ownsTransaction && !transaction.finished) await transaction.rollback();
    throw error;
  }
}

module.exports = {
  buildStageTransformationHistorySql,
  recordStageTransformationHistory,
};

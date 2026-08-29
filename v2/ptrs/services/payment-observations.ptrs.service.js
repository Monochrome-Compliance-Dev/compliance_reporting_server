const db = require("@/db/database");

const PAYMENT_OBSERVATION_DOCUMENT_TYPES = Object.freeze({
  invoice: "RE",
  settlement: "ZP",
  earlytrade: "ET",
});

const PAYMENT_OBSERVATION_REPLACEMENTS = Object.freeze({
  paymentObservationInvoiceType: PAYMENT_OBSERVATION_DOCUMENT_TYPES.invoice,
  paymentObservationSettlementType:
    PAYMENT_OBSERVATION_DOCUMENT_TYPES.settlement,
  paymentObservationEarlytradeType:
    PAYMENT_OBSERVATION_DOCUMENT_TYPES.earlytrade,
});

const PAYMENT_OBSERVATION_WORK_MEM = "128MB";

function buildPaymentObservationsCte() {
  return `
    payment_observation_source_rows AS NOT MATERIALIZED (
      SELECT
        s."id", s."sourceGroupScope", s."datasetId",
        s."canonicalRevisionId",
        s."semanticKind", s."rowNo", s."paymentAmount",
        COALESCE(
          NULLIF(BTRIM(s."sourceGroupScope"), ''),
          'dataset:' || s."datasetId"
        ) AS source_group_key,
        s."documentType" AS document_type,
        NULLIF(BTRIM(s."data"->>'company_code'), '') AS company_code,
        NULLIF(BTRIM(s."sourceAccountCode"), '') AS source_account_code,
        NULLIF(BTRIM(s."clearingDocument"), '') AS clearing_document,
        NULLIF(BTRIM(s."description"), '') AS description_reference,
        s."paymentDate" AS payment_date,
        CASE
          WHEN lower(s."data"->>'exclude_from_metrics') IN ('true', 'false')
            THEN (s."data"->>'exclude_from_metrics')::boolean
          ELSE false
        END
        OR CASE
          WHEN lower(s."meta"->'rules'->>'exclude') IN ('true', 'false')
            THEN (s."meta"->'rules'->>'exclude')::boolean
          ELSE false
        END AS excluded
      FROM "tbl_ptrs_stage_row" s
      WHERE s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
    ),
    payment_observation_settlement_keys AS MATERIALIZED (
      SELECT
        source_group_key,
        company_code,
        source_account_code,
        clearing_document,
        COUNT(*)::int AS settlement_row_count,
        MAX("id") AS settlement_stage_row_id,
        MAX("sourceGroupScope") AS settlement_source_group_scope,
        MAX("rowNo") AS settlement_row_no,
        MAX(payment_date) AS settlement_payment_date,
        MAX("paymentAmount") AS settlement_payment_amount
      FROM payment_observation_source_rows settlement
      WHERE settlement."semanticKind" = 'accounting_event'
        AND settlement.document_type = :paymentObservationSettlementType
        AND NOT settlement.excluded
        AND settlement.company_code IS NOT NULL
        AND settlement.source_account_code IS NOT NULL
        AND settlement.clearing_document IS NOT NULL
      GROUP BY
        source_group_key,
        company_code,
        source_account_code,
        clearing_document
    ),
    payment_observation_invoice_keys AS MATERIALIZED (
      SELECT invoice.*
      FROM payment_observation_source_rows invoice
      WHERE invoice."semanticKind" = 'accounting_event'
        AND invoice.document_type = :paymentObservationInvoiceType
    ),
    payment_observation_earlytrade_keys AS MATERIALIZED (
      SELECT earlytrade.*
      FROM payment_observation_source_rows earlytrade
      WHERE earlytrade."semanticKind" = 'accounting_event'
        AND earlytrade.document_type = :paymentObservationEarlytradeType
        AND earlytrade.company_code IS NOT NULL
        AND earlytrade.source_account_code IS NOT NULL
        AND earlytrade.clearing_document IS NOT NULL
        AND earlytrade.description_reference IS NOT NULL
    ),
    payment_observation_earlytrade_matches AS MATERIALIZED (
      SELECT
        invoice."id" AS invoice_id,
        invoice."rowNo" AS invoice_row_no,
        earlytrade."id" AS earlytrade_id,
        earlytrade."rowNo" AS earlytrade_row_no,
        invoice.source_group_key,
        invoice.company_code,
        invoice.source_account_code,
        invoice.clearing_document,
        invoice.description_reference
      FROM payment_observation_invoice_keys invoice
      JOIN payment_observation_earlytrade_keys earlytrade
        ON earlytrade.source_group_key = invoice.source_group_key
       AND earlytrade.company_code = invoice.company_code
       AND earlytrade.source_account_code = invoice.source_account_code
       AND earlytrade.clearing_document = invoice.clearing_document
       AND earlytrade.description_reference = invoice.description_reference
    ),
    payment_observation_eligible_invoice_keys AS MATERIALIZED (
      SELECT invoice.*
      FROM payment_observation_invoice_keys invoice
      LEFT JOIN payment_observation_earlytrade_keys earlytrade
        ON earlytrade.source_group_key = invoice.source_group_key
       AND earlytrade.company_code = invoice.company_code
       AND earlytrade.source_account_code = invoice.source_account_code
       AND earlytrade.clearing_document = invoice.clearing_document
       AND earlytrade.description_reference = invoice.description_reference
      WHERE NOT invoice.excluded
        AND earlytrade."id" IS NULL
    ),
    payment_observation_accounting_keys AS MATERIALIZED (
      SELECT
        invoice.*,
        settlement.settlement_row_count,
        settlement.settlement_stage_row_id,
        settlement.settlement_source_group_scope,
        settlement.settlement_row_no,
        settlement.settlement_payment_date,
        settlement.settlement_payment_amount
      FROM payment_observation_eligible_invoice_keys invoice
      JOIN payment_observation_settlement_keys settlement
        ON settlement.source_group_key = invoice.source_group_key
       AND settlement.company_code = invoice.company_code
       AND settlement.source_account_code = invoice.source_account_code
       AND settlement.clearing_document = invoice.clearing_document
       AND settlement.settlement_row_count = 1
    ),
    payment_observation_direct_keys AS MATERIALIZED (
      SELECT direct.*
      FROM payment_observation_source_rows direct
      WHERE direct."semanticKind" = 'direct_payment'
        AND NOT direct.excluded
    ),
    payment_observation_accounting_observations AS (
      SELECT
        'payment-observation:' || invoice."id" AS "observationId",
        'accounting_event'::text AS "observationSourceType",
        invoice_payload."customerId", invoice_payload."ptrsId",
        invoice_payload."profileId",
        invoice_payload."datasetId" AS "sourceDatasetId",
        invoice_payload."canonicalRevisionId",
        invoice_payload."canonicalSourceRowId",
        invoice_payload."sourceRawRowId", invoice_payload."sourceRowNo",
        invoice_payload."adapterType", invoice_payload."adapterVersion",
        invoice_payload."semanticKind", invoice_payload."sourceGroupScope",
        invoice.source_group_key AS "sourceGroupKey",
        COALESCE(
          invoice_payload."meta"->'canonical'->'lineage'->'joinedReferences',
          '{}'::jsonb
        ) AS "joinedReferenceLineage",
        jsonb_build_array(
          jsonb_build_object(
            'stageRowId', invoice."id",
            'canonicalRevisionId', invoice_payload."canonicalRevisionId",
            'canonicalSourceRowId', invoice_payload."canonicalSourceRowId",
            'datasetId', invoice_payload."datasetId",
            'sourceRawRowId', invoice_payload."sourceRawRowId",
            'sourceRowNo', invoice_payload."sourceRowNo",
            'adapterType', invoice_payload."adapterType",
            'adapterVersion', invoice_payload."adapterVersion",
            'semanticKind', invoice_payload."semanticKind",
            'sourceGroupScope', invoice_payload."sourceGroupScope",
            'joinedReferences', COALESCE(
              invoice_payload."meta"->'canonical'->'lineage'->'joinedReferences',
              '{}'::jsonb
            )
          ),
          jsonb_build_object(
            'stageRowId', settlement_payload."id",
            'canonicalRevisionId', settlement_payload."canonicalRevisionId",
            'canonicalSourceRowId', settlement_payload."canonicalSourceRowId",
            'datasetId', settlement_payload."datasetId",
            'sourceRawRowId', settlement_payload."sourceRawRowId",
            'sourceRowNo', settlement_payload."sourceRowNo",
            'adapterType', settlement_payload."adapterType",
            'adapterVersion', settlement_payload."adapterVersion",
            'semanticKind', settlement_payload."semanticKind",
            'sourceGroupScope', settlement_payload."sourceGroupScope",
            'joinedReferences', COALESCE(
              settlement_payload."meta"->'canonical'->'lineage'->'joinedReferences',
              '{}'::jsonb
            )
          )
        ) AS "sourceProvenance",
        invoice_payload."rowNo",
        invoice."id" AS "primarySourceStageRowId",
        invoice."id" AS "sourceInvoiceStageRowId",
        ARRAY[invoice.settlement_stage_row_id] AS "settlementStageRowIds",
        ARRAY[invoice."id", invoice.settlement_stage_row_id]
          AS "sourceStageRowIds",
        invoice.company_code AS "sourceCompanyCode",
        invoice.source_account_code AS "sourceAccountCode",
        invoice.clearing_document AS "clearingDocument",
        invoice_payload."paymentAmount",
        invoice.settlement_payment_date::text AS "paymentDate",
        jsonb_set(
          COALESCE(invoice_payload."data", '{}'::jsonb),
          '{payment_date}',
          COALESCE(to_jsonb(invoice.settlement_payment_date), 'null'::jsonb),
          true
        ) AS "data",
        jsonb_set(
          COALESCE(invoice_payload."meta", '{}'::jsonb),
          '{paymentObservation}',
          jsonb_build_object(
            'observationSourceType', 'accounting_event',
            'sourceGroupKey', invoice.source_group_key,
            'sourceInvoiceStageRowId', invoice."id",
            'settlementStageRowIds', ARRAY[invoice.settlement_stage_row_id],
            'companyCode', invoice.company_code,
            'sourceAccountCode', invoice.source_account_code,
            'clearingDocument', invoice.clearing_document
          ),
          true
        ) AS "meta"
      FROM payment_observation_accounting_keys invoice
      JOIN "tbl_ptrs_stage_row" invoice_payload
        ON invoice_payload."id" = invoice."id"
       AND invoice_payload."customerId" = :customerId
       AND invoice_payload."ptrsId" = :ptrsId
       AND invoice_payload."deletedAt" IS NULL
      JOIN "tbl_ptrs_stage_row" settlement_payload
        ON settlement_payload."id" = invoice.settlement_stage_row_id
       AND settlement_payload."customerId" = :customerId
       AND settlement_payload."ptrsId" = :ptrsId
       AND settlement_payload."deletedAt" IS NULL
    ),
    payment_observation_direct_observations AS (
      SELECT
        'payment-observation:' || direct."id" AS "observationId",
        'direct_payment'::text AS "observationSourceType",
        direct_payload."customerId", direct_payload."ptrsId",
        direct_payload."profileId",
        direct_payload."datasetId" AS "sourceDatasetId",
        direct_payload."canonicalRevisionId",
        direct_payload."canonicalSourceRowId",
        direct_payload."sourceRawRowId", direct_payload."sourceRowNo",
        direct_payload."adapterType", direct_payload."adapterVersion",
        direct_payload."semanticKind", direct_payload."sourceGroupScope",
        direct.source_group_key AS "sourceGroupKey",
        COALESCE(
          direct_payload."meta"->'canonical'->'lineage'->'joinedReferences',
          '{}'::jsonb
        ) AS "joinedReferenceLineage",
        jsonb_build_array(jsonb_build_object(
          'stageRowId', direct."id",
          'canonicalRevisionId', direct_payload."canonicalRevisionId",
          'canonicalSourceRowId', direct_payload."canonicalSourceRowId",
          'datasetId', direct_payload."datasetId",
          'sourceRawRowId', direct_payload."sourceRawRowId",
          'sourceRowNo', direct_payload."sourceRowNo",
          'adapterType', direct_payload."adapterType",
          'adapterVersion', direct_payload."adapterVersion",
          'semanticKind', direct_payload."semanticKind",
          'sourceGroupScope', direct_payload."sourceGroupScope",
          'joinedReferences', COALESCE(
            direct_payload."meta"->'canonical'->'lineage'->'joinedReferences',
            '{}'::jsonb
          )
        )) AS "sourceProvenance",
        direct_payload."rowNo",
        direct."id" AS "primarySourceStageRowId",
        NULL::varchar AS "sourceInvoiceStageRowId",
        ARRAY[]::varchar[] AS "settlementStageRowIds",
        ARRAY[direct."id"] AS "sourceStageRowIds",
        direct.company_code AS "sourceCompanyCode",
        direct.source_account_code AS "sourceAccountCode",
        direct.clearing_document AS "clearingDocument",
        direct_payload."paymentAmount",
        direct.payment_date::text AS "paymentDate",
        direct_payload."data" AS "data",
        jsonb_set(
          COALESCE(direct_payload."meta", '{}'::jsonb),
          '{paymentObservation}',
          jsonb_build_object(
            'observationSourceType', 'direct_payment',
            'paymentAmountSemantic', 'actual_settlement_amount',
            'primarySourceStageRowId', direct."id",
            'sourceGroupKey', direct.source_group_key
          ),
          true
        ) AS "meta"
      FROM payment_observation_direct_keys direct
      JOIN "tbl_ptrs_stage_row" direct_payload
        ON direct_payload."id" = direct."id"
       AND direct_payload."customerId" = :customerId
       AND direct_payload."ptrsId" = :ptrsId
       AND direct_payload."deletedAt" IS NULL
    ),
    payment_observations AS (
      SELECT * FROM payment_observation_accounting_observations
      UNION ALL
      SELECT * FROM payment_observation_direct_observations
    ),
    payment_observation_accounting_settlement_groups AS (
      SELECT DISTINCT
        'accounting_event'::text AS "observationSourceType",
        observation.source_group_key AS "sourceGroupKey",
        observation.settlement_source_group_scope AS "sourceGroupScope",
        observation.company_code AS "sourceCompanyCode",
        observation.source_account_code AS "sourceAccountCode",
        observation.clearing_document AS "clearingDocument",
        observation.source_group_key || ':' || observation.company_code || ':'
          || observation.source_account_code || ':' || observation.clearing_document
          AS "settlementIdentity",
        observation.settlement_payment_amount AS "settlementPaymentAmount"
      FROM payment_observation_accounting_keys observation
    ),
    payment_observation_settlement_groups AS (
      SELECT * FROM payment_observation_accounting_settlement_groups
      UNION ALL
      SELECT
        'direct_payment'::text AS "observationSourceType",
        direct.source_group_key AS "sourceGroupKey",
        direct."sourceGroupScope",
        direct.company_code AS "sourceCompanyCode",
        direct.source_account_code AS "sourceAccountCode",
        direct.clearing_document AS "clearingDocument",
        'payment-observation:' || direct."id" AS "settlementIdentity",
        direct."paymentAmount" AS "settlementPaymentAmount"
      FROM payment_observation_direct_keys direct
    )
  `;
}

function buildPaymentObservationSummaryCte() {
  return `
    payment_observation_summary_source AS (
      SELECT
        s."semanticKind",
        COALESCE(
          NULLIF(BTRIM(s."sourceGroupScope"), ''),
          'dataset:' || s."datasetId"
        ) AS source_group_key,
        NULLIF(BTRIM(s."documentType"), '') AS document_type,
        NULLIF(BTRIM(s."data"->>'company_code'), '') AS company_code,
        NULLIF(BTRIM(s."sourceAccountCode"), '') AS source_account_code,
        NULLIF(BTRIM(s."clearingDocument"), '') AS clearing_document,
        NULLIF(BTRIM(s."description"), '') AS description_reference,
        s."paymentDate" AS payment_date,
        lower(s."data"->>'is_small_business') = 'true' AS is_small_business,
        CASE
          WHEN lower(s."data"->>'exclude_from_metrics') IN ('true', 'false')
            THEN (s."data"->>'exclude_from_metrics')::boolean
          ELSE false
        END
        OR CASE
          WHEN lower(s."meta"->'rules'->>'exclude') IN ('true', 'false')
            THEN (s."meta"->'rules'->>'exclude')::boolean
          ELSE false
        END AS excluded
      FROM "tbl_ptrs_stage_row" s
      WHERE s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
    ),
    payment_observation_summary_annotated AS (
      SELECT
        source.*,
        COUNT(*) FILTER (
          WHERE "semanticKind" = 'accounting_event'
            AND document_type = :paymentObservationSettlementType
            AND NOT excluded
            AND company_code IS NOT NULL
            AND source_account_code IS NOT NULL
            AND clearing_document IS NOT NULL
        ) OVER settlement_group AS settlement_row_count,
        MAX(payment_date) FILTER (
          WHERE "semanticKind" = 'accounting_event'
            AND document_type = :paymentObservationSettlementType
            AND NOT excluded
            AND company_code IS NOT NULL
            AND source_account_code IS NOT NULL
            AND clearing_document IS NOT NULL
        ) OVER settlement_group AS settlement_payment_date,
        BOOL_OR(
          "semanticKind" = 'accounting_event'
          AND document_type = :paymentObservationEarlytradeType
          AND company_code IS NOT NULL
          AND source_account_code IS NOT NULL
          AND clearing_document IS NOT NULL
          AND description_reference IS NOT NULL
        ) OVER earlytrade_group AS has_matching_earlytrade
      FROM payment_observation_summary_source source
      WINDOW
        settlement_group AS (
          PARTITION BY
            source_group_key, company_code, source_account_code,
            clearing_document
        ),
        earlytrade_group AS (
          PARTITION BY
            source_group_key, company_code, source_account_code,
            clearing_document, description_reference
        )
    ),
    payment_observation_summary_classified AS (
      SELECT
        annotated.*,
        (
          "semanticKind" = 'accounting_event'
          AND document_type = :paymentObservationInvoiceType
          AND NOT excluded
          AND settlement_row_count = 1
          AND NOT has_matching_earlytrade
        ) AS accounting_observation,
        (
          "semanticKind" = 'direct_payment'
          AND NOT excluded
        ) AS direct_observation,
        (
          "semanticKind" = 'accounting_event'
          AND document_type = :paymentObservationInvoiceType
          AND has_matching_earlytrade
        ) AS earlytrade_match
      FROM payment_observation_summary_annotated annotated
    ),
    payment_observation_summary_counts AS (
      SELECT
        COUNT(*)::int AS "sourceStageRows",
        COUNT(*) FILTER (WHERE excluded)::int AS "excludedStageRows",
        COUNT(*) FILTER (WHERE NOT excluded)::int AS "survivingStageRows",
        COUNT(*) FILTER (
          WHERE accounting_observation OR direct_observation
        )::int AS "derivedPaymentObservations",
        COUNT(*) FILTER (
          WHERE (accounting_observation OR direct_observation)
            AND is_small_business
        )::int AS "sbiPositiveObservations",
        COUNT(*) FILTER (WHERE earlytrade_match)::int AS "earlytradeMatches",
        COUNT(*) FILTER (
          WHERE is_small_business
            AND (
              (accounting_observation AND settlement_payment_date IS NULL)
              OR (direct_observation AND payment_date IS NULL)
            )
        )::int AS "sbiObservationsMissingPaymentDate",
        0::int AS "sbiObservationsMissingSourceTrace"
      FROM payment_observation_summary_classified
    )
  `;
}

function getPaymentObservationReplacements({ customerId, ptrsId }) {
  return { customerId, ptrsId, ...PAYMENT_OBSERVATION_REPLACEMENTS };
}

async function setPaymentObservationWorkMem({ transaction }) {
  if (!transaction) return;
  await db.sequelize.query(
    `SET LOCAL work_mem = '${PAYMENT_OBSERVATION_WORK_MEM}'`,
    { transaction },
  );
}

async function listPaymentObservations({ customerId, ptrsId, transaction }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  await setPaymentObservationWorkMem({ transaction });
  const sql = `
    WITH ${buildPaymentObservationsCte()}
    SELECT * FROM payment_observations
    ORDER BY "rowNo" ASC, "observationId" ASC
  `;
  return db.sequelize.query(sql, {
    transaction,
    replacements: getPaymentObservationReplacements({ customerId, ptrsId }),
    type: db.sequelize.QueryTypes.SELECT,
  });
}

async function listPaymentObservationLinks({ customerId, ptrsId, transaction }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  await setPaymentObservationWorkMem({ transaction });
  const sql = `
    WITH ${buildPaymentObservationsCte()}
    SELECT
      'payment-observation:' || invoice."id" AS "observationId",
      'accounting_event'::text AS "observationSourceType",
      invoice."id" AS "primarySourceStageRowId",
      invoice."id" AS "sourceInvoiceStageRowId",
      ARRAY[invoice.settlement_stage_row_id] AS "settlementStageRowIds",
      invoice.company_code AS "sourceCompanyCode",
      invoice.source_account_code AS "sourceAccountCode",
      invoice.clearing_document AS "clearingDocument",
      invoice."rowNo"
    FROM payment_observation_accounting_keys invoice
    UNION ALL
    SELECT
      'payment-observation:' || direct."id" AS "observationId",
      'direct_payment'::text AS "observationSourceType",
      direct."id" AS "primarySourceStageRowId",
      NULL::varchar AS "sourceInvoiceStageRowId",
      ARRAY[]::varchar[] AS "settlementStageRowIds",
      direct.company_code AS "sourceCompanyCode",
      direct.source_account_code AS "sourceAccountCode",
      direct.clearing_document AS "clearingDocument",
      direct."rowNo"
    FROM payment_observation_direct_keys direct
    ORDER BY "rowNo" ASC, "observationId" ASC
  `;
  return db.sequelize.query(sql, {
    transaction,
    replacements: getPaymentObservationReplacements({ customerId, ptrsId }),
    type: db.sequelize.QueryTypes.SELECT,
  });
}

async function getPaymentObservationSummary({
  customerId,
  ptrsId,
  transaction,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  await setPaymentObservationWorkMem({ transaction });
  const sql = `
    WITH ${buildPaymentObservationSummaryCte()}
    SELECT * FROM payment_observation_summary_counts
  `;
  const rows = await db.sequelize.query(sql, {
    transaction,
    replacements: getPaymentObservationReplacements({ customerId, ptrsId }),
    type: db.sequelize.QueryTypes.SELECT,
  });
  return rows?.[0] || {
    sourceStageRows: 0,
    excludedStageRows: 0,
    survivingStageRows: 0,
    derivedPaymentObservations: 0,
    sbiPositiveObservations: 0,
    earlytradeMatches: 0,
    sbiObservationsMissingPaymentDate: 0,
    sbiObservationsMissingSourceTrace: 0,
  };
}

module.exports = {
  PAYMENT_OBSERVATION_DOCUMENT_TYPES,
  PAYMENT_OBSERVATION_WORK_MEM,
  buildPaymentObservationSummaryCte,
  buildPaymentObservationsCte,
  getPaymentObservationReplacements,
  getPaymentObservationSummary,
  listPaymentObservationLinks,
  listPaymentObservations,
  setPaymentObservationWorkMem,
};

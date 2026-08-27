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

function buildPaymentObservationsCte() {
  return `
    payment_observation_source_rows AS (
      SELECT
        s."id", s."customerId", s."ptrsId", s."profileId", s."datasetId",
        s."canonicalRevisionId", s."canonicalSourceRowId",
        s."sourceRawRowId", s."sourceRowNo",
        s."adapterType", s."adapterVersion", s."sourceGroupScope",
        s."semanticKind", s."rowNo", s."paymentAmount", s."data", s."meta",
        COALESCE(
          NULLIF(BTRIM(s."sourceGroupScope"), ''),
          'dataset:' || s."datasetId"
        ) AS source_group_key,
        COALESCE(
          NULLIF(BTRIM(s."documentType"), ''),
          NULLIF(BTRIM(s."data"->>'document_type'), '')
        ) AS document_type,
        NULLIF(BTRIM(s."data"->>'company_code'), '') AS company_code,
        COALESCE(
          NULLIF(BTRIM(s."sourceAccountCode"), ''),
          NULLIF(BTRIM(s."data"->>'source_account_code'), '')
        ) AS source_account_code,
        COALESCE(
          NULLIF(BTRIM(s."clearingDocument"), ''),
          NULLIF(BTRIM(s."data"->>'clearing_document'), '')
        ) AS clearing_document,
        NULLIF(BTRIM(s."data"->>'description'), '') AS description_reference,
        COALESCE(
          s."paymentDate"::text,
          NULLIF(BTRIM(s."data"->>'payment_date'), '')
        ) AS payment_date,
        COALESCE(
          s."meta"->'canonical'->'lineage'->'joinedReferences',
          '{}'::jsonb
        ) AS joined_reference_lineage,
        jsonb_build_object(
          'stageRowId', s."id",
          'canonicalRevisionId', s."canonicalRevisionId",
          'canonicalSourceRowId', s."canonicalSourceRowId",
          'datasetId', s."datasetId",
          'sourceRawRowId', s."sourceRawRowId",
          'sourceRowNo', s."sourceRowNo",
          'adapterType', s."adapterType",
          'adapterVersion', s."adapterVersion",
          'semanticKind', s."semanticKind",
          'sourceGroupScope', s."sourceGroupScope",
          'joinedReferences', COALESCE(
            s."meta"->'canonical'->'lineage'->'joinedReferences',
            '{}'::jsonb
          )
        ) AS source_provenance,
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
    payment_observation_earlytrade_keys AS (
      SELECT DISTINCT
        source_group_key, company_code, source_account_code,
        clearing_document, description_reference
      FROM payment_observation_source_rows
      WHERE "semanticKind" = 'accounting_event'
        AND document_type = :paymentObservationEarlytradeType
        AND company_code IS NOT NULL
        AND source_account_code IS NOT NULL
        AND clearing_document IS NOT NULL
        AND description_reference IS NOT NULL
    ),
    payment_observation_settlement_anchors AS (
      SELECT
        source_group_key,
        MAX("sourceGroupScope") AS source_group_scope,
        company_code, source_account_code, clearing_document,
        COUNT(*)::int AS settlement_row_count,
        ARRAY_AGG("id" ORDER BY "rowNo", "id") AS settlement_stage_row_ids,
        jsonb_agg(source_provenance ORDER BY "rowNo", "id")
          AS settlement_source_provenance,
        CASE WHEN COUNT(*) = 1 THEN MAX(payment_date) ELSE NULL END
          AS payment_date,
        CASE WHEN COUNT(*) = 1 THEN MAX("paymentAmount") ELSE NULL END
          AS settlement_payment_amount
      FROM payment_observation_source_rows
      WHERE "semanticKind" = 'accounting_event'
        AND document_type = :paymentObservationSettlementType
        AND NOT excluded
        AND company_code IS NOT NULL
        AND source_account_code IS NOT NULL
        AND clearing_document IS NOT NULL
      GROUP BY
        source_group_key, company_code, source_account_code, clearing_document
    ),
    payment_observation_accounting_observations AS (
      SELECT
        'payment-observation:' || invoice."id" AS "observationId",
        'accounting_event'::text AS "observationSourceType",
        invoice."customerId", invoice."ptrsId", invoice."profileId",
        invoice."datasetId" AS "sourceDatasetId",
        invoice."canonicalRevisionId", invoice."canonicalSourceRowId",
        invoice."sourceRawRowId", invoice."sourceRowNo",
        invoice."adapterType", invoice."adapterVersion", invoice."semanticKind",
        invoice."sourceGroupScope",
        invoice.source_group_key AS "sourceGroupKey",
        invoice.joined_reference_lineage AS "joinedReferenceLineage",
        jsonb_build_array(invoice.source_provenance)
          || settlement.settlement_source_provenance AS "sourceProvenance",
        invoice."rowNo",
        invoice."id" AS "primarySourceStageRowId",
        invoice."id" AS "sourceInvoiceStageRowId",
        settlement.settlement_stage_row_ids AS "settlementStageRowIds",
        ARRAY[invoice."id"] || settlement.settlement_stage_row_ids
          AS "sourceStageRowIds",
        invoice.company_code AS "sourceCompanyCode",
        invoice.source_account_code AS "sourceAccountCode",
        invoice.clearing_document AS "clearingDocument",
        invoice."paymentAmount",
        settlement.payment_date AS "paymentDate",
        jsonb_set(
          COALESCE(invoice."data", '{}'::jsonb),
          '{payment_date}',
          COALESCE(to_jsonb(settlement.payment_date), 'null'::jsonb),
          true
        ) AS "data",
        jsonb_set(
          COALESCE(invoice."meta", '{}'::jsonb),
          '{paymentObservation}',
          jsonb_build_object(
            'observationSourceType', 'accounting_event',
            'sourceGroupKey', invoice.source_group_key,
            'sourceInvoiceStageRowId', invoice."id",
            'settlementStageRowIds', settlement.settlement_stage_row_ids,
            'companyCode', invoice.company_code,
            'sourceAccountCode', invoice.source_account_code,
            'clearingDocument', invoice.clearing_document
          ),
          true
        ) AS "meta"
      FROM payment_observation_source_rows invoice
      JOIN payment_observation_settlement_anchors settlement
        ON settlement.source_group_key = invoice.source_group_key
       AND settlement.company_code = invoice.company_code
       AND settlement.source_account_code = invoice.source_account_code
       AND settlement.clearing_document = invoice.clearing_document
      LEFT JOIN payment_observation_earlytrade_keys earlytrade
        ON earlytrade.source_group_key = invoice.source_group_key
       AND earlytrade.company_code = invoice.company_code
       AND earlytrade.source_account_code = invoice.source_account_code
       AND earlytrade.clearing_document = invoice.clearing_document
       AND earlytrade.description_reference = invoice.description_reference
      WHERE invoice."semanticKind" = 'accounting_event'
        AND invoice.document_type = :paymentObservationInvoiceType
        AND NOT invoice.excluded
        AND settlement.settlement_row_count = 1
        AND earlytrade.company_code IS NULL
    ),
    payment_observation_direct_observations AS (
      SELECT
        'payment-observation:' || direct."id" AS "observationId",
        'direct_payment'::text AS "observationSourceType",
        direct."customerId", direct."ptrsId", direct."profileId",
        direct."datasetId" AS "sourceDatasetId",
        direct."canonicalRevisionId", direct."canonicalSourceRowId",
        direct."sourceRawRowId", direct."sourceRowNo",
        direct."adapterType", direct."adapterVersion", direct."semanticKind",
        direct."sourceGroupScope",
        direct.source_group_key AS "sourceGroupKey",
        direct.joined_reference_lineage AS "joinedReferenceLineage",
        jsonb_build_array(direct.source_provenance) AS "sourceProvenance",
        direct."rowNo",
        direct."id" AS "primarySourceStageRowId",
        NULL::varchar AS "sourceInvoiceStageRowId",
        ARRAY[]::varchar[] AS "settlementStageRowIds",
        ARRAY[direct."id"] AS "sourceStageRowIds",
        direct.company_code AS "sourceCompanyCode",
        direct.source_account_code AS "sourceAccountCode",
        direct.clearing_document AS "clearingDocument",
        direct."paymentAmount",
        direct.payment_date AS "paymentDate",
        direct."data" AS "data",
        jsonb_set(
          COALESCE(direct."meta", '{}'::jsonb),
          '{paymentObservation}',
          jsonb_build_object(
            'observationSourceType', 'direct_payment',
            'paymentAmountSemantic', 'actual_settlement_amount',
            'primarySourceStageRowId', direct."id",
            'sourceGroupKey', direct.source_group_key
          ),
          true
        ) AS "meta"
      FROM payment_observation_source_rows direct
      WHERE direct."semanticKind" = 'direct_payment'
        AND NOT direct.excluded
    ),
    payment_observations AS (
      SELECT * FROM payment_observation_accounting_observations
      UNION ALL
      SELECT * FROM payment_observation_direct_observations
    ),
    payment_observation_accounting_settlement_groups AS (
      SELECT DISTINCT
        'accounting_event'::text AS "observationSourceType",
        settlement.source_group_key AS "sourceGroupKey",
        settlement.source_group_scope AS "sourceGroupScope",
        settlement.company_code AS "sourceCompanyCode",
        settlement.source_account_code AS "sourceAccountCode",
        settlement.clearing_document AS "clearingDocument",
        settlement.source_group_key || ':' || settlement.company_code || ':'
          || settlement.source_account_code || ':' || settlement.clearing_document
          AS "settlementIdentity",
        settlement.settlement_payment_amount AS "settlementPaymentAmount"
      FROM payment_observation_settlement_anchors settlement
      JOIN payment_observation_accounting_observations observation
        ON observation."sourceGroupKey" = settlement.source_group_key
       AND observation."sourceCompanyCode" = settlement.company_code
       AND observation."sourceAccountCode" = settlement.source_account_code
       AND observation."clearingDocument" = settlement.clearing_document
    ),
    payment_observation_settlement_groups AS (
      SELECT * FROM payment_observation_accounting_settlement_groups
      UNION ALL
      SELECT
        'direct_payment'::text AS "observationSourceType",
        direct."sourceGroupKey", direct."sourceGroupScope",
        direct."sourceCompanyCode", direct."sourceAccountCode",
        direct."clearingDocument",
        direct."observationId" AS "settlementIdentity",
        direct."paymentAmount" AS "settlementPaymentAmount"
      FROM payment_observation_direct_observations direct
    )
  `;
}

function getPaymentObservationReplacements({ customerId, ptrsId }) {
  return { customerId, ptrsId, ...PAYMENT_OBSERVATION_REPLACEMENTS };
}

async function listPaymentObservations({ customerId, ptrsId, transaction }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
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

async function getPaymentObservationSummary({
  customerId,
  ptrsId,
  transaction,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  const sql = `
    WITH ${buildPaymentObservationsCte()},
    earlytrade_omitted_invoices AS (
      SELECT DISTINCT invoice."id"
      FROM payment_observation_source_rows invoice
      JOIN payment_observation_earlytrade_keys earlytrade
        ON earlytrade.source_group_key = invoice.source_group_key
       AND earlytrade.company_code = invoice.company_code
       AND earlytrade.source_account_code = invoice.source_account_code
       AND earlytrade.clearing_document = invoice.clearing_document
       AND earlytrade.description_reference = invoice.description_reference
      WHERE invoice."semanticKind" = 'accounting_event'
        AND invoice.document_type = :paymentObservationInvoiceType
    )
    SELECT
      (SELECT COUNT(*)::int FROM payment_observation_source_rows) AS "sourceStageRows",
      (SELECT COUNT(*)::int FROM payment_observation_source_rows WHERE excluded) AS "excludedStageRows",
      (SELECT COUNT(*)::int FROM payment_observation_source_rows WHERE NOT excluded) AS "survivingStageRows",
      (SELECT COUNT(*)::int FROM payment_observations) AS "derivedPaymentObservations",
      (SELECT COUNT(*)::int FROM payment_observations WHERE lower("data"->>'is_small_business') = 'true') AS "sbiPositiveObservations",
      (SELECT COUNT(*)::int FROM earlytrade_omitted_invoices) AS "earlytradeMatches",
      (
        SELECT COUNT(*)::int FROM payment_observations
        WHERE lower("data"->>'is_small_business') = 'true'
          AND NULLIF(BTRIM(COALESCE("data"->>'payment_date', '')), '') IS NULL
      ) AS "sbiObservationsMissingPaymentDate",
      (
        SELECT COUNT(*)::int FROM payment_observations
        WHERE lower("data"->>'is_small_business') = 'true'
          AND COALESCE(cardinality("sourceStageRowIds"), 0) = 0
      ) AS "sbiObservationsMissingSourceTrace"
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
  buildPaymentObservationsCte,
  getPaymentObservationReplacements,
  getPaymentObservationSummary,
  listPaymentObservations,
};

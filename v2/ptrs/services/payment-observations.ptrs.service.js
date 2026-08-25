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
        s."id",
        s."customerId",
        s."ptrsId",
        s."profileId",
        s."datasetId",
        s."rowNo",
        s."paymentAmount",
        s."data",
        s."meta",
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
        company_code,
        source_account_code,
        clearing_document,
        description_reference
      FROM payment_observation_source_rows
      WHERE document_type = :paymentObservationEarlytradeType
        AND company_code IS NOT NULL
        AND source_account_code IS NOT NULL
        AND clearing_document IS NOT NULL
        AND description_reference IS NOT NULL
    ),
    payment_observation_settlement_anchors AS (
      SELECT
        company_code,
        source_account_code,
        clearing_document,
        COUNT(*)::int AS settlement_row_count,
        ARRAY_AGG("id" ORDER BY "rowNo") AS settlement_stage_row_ids,
        CASE
          WHEN COUNT(*) = 1 THEN MAX(payment_date)
          ELSE NULL
        END AS payment_date,
        CASE
          WHEN COUNT(*) = 1 THEN MAX("paymentAmount")
          ELSE NULL
        END AS settlement_payment_amount
      FROM payment_observation_source_rows
      WHERE document_type = :paymentObservationSettlementType
        AND NOT excluded
        AND company_code IS NOT NULL
        AND source_account_code IS NOT NULL
        AND clearing_document IS NOT NULL
      GROUP BY company_code, source_account_code, clearing_document
    ),
    payment_observations AS (
      SELECT
        'payment-observation:' || invoice."id" AS "observationId",
        invoice."customerId",
        invoice."ptrsId",
        invoice."profileId",
        invoice."datasetId" AS "sourceDatasetId",
        invoice."rowNo",
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
        ON settlement.company_code = invoice.company_code
       AND settlement.source_account_code = invoice.source_account_code
       AND settlement.clearing_document = invoice.clearing_document
      LEFT JOIN payment_observation_earlytrade_keys earlytrade
        ON earlytrade.company_code = invoice.company_code
       AND earlytrade.source_account_code = invoice.source_account_code
       AND earlytrade.clearing_document = invoice.clearing_document
       AND earlytrade.description_reference = invoice.description_reference
      WHERE invoice.document_type = :paymentObservationInvoiceType
        AND NOT invoice.excluded
        AND settlement.settlement_row_count = 1
        AND earlytrade.company_code IS NULL
    ),
    payment_observation_settlement_groups AS (
      SELECT DISTINCT
        settlement.company_code AS "sourceCompanyCode",
        settlement.source_account_code AS "sourceAccountCode",
        settlement.clearing_document AS "clearingDocument",
        settlement.settlement_payment_amount AS "settlementPaymentAmount"
      FROM payment_observation_settlement_anchors settlement
      JOIN payment_observations observation
        ON observation."sourceCompanyCode" = settlement.company_code
       AND observation."sourceAccountCode" = settlement.source_account_code
       AND observation."clearingDocument" = settlement.clearing_document
    )
  `;
}

function getPaymentObservationReplacements({ customerId, ptrsId }) {
  return {
    customerId,
    ptrsId,
    ...PAYMENT_OBSERVATION_REPLACEMENTS,
  };
}

async function listPaymentObservations({ customerId, ptrsId, transaction }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const sql = `
    WITH ${buildPaymentObservationsCte()}
    SELECT *
    FROM payment_observations
    ORDER BY "rowNo" ASC
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
        ON earlytrade.company_code = invoice.company_code
       AND earlytrade.source_account_code = invoice.source_account_code
       AND earlytrade.clearing_document = invoice.clearing_document
       AND earlytrade.description_reference = invoice.description_reference
      WHERE invoice.document_type = :paymentObservationInvoiceType
    )
    SELECT
      (SELECT COUNT(*)::int FROM payment_observation_source_rows) AS "sourceStageRows",
      (SELECT COUNT(*)::int FROM payment_observation_source_rows WHERE excluded) AS "excludedStageRows",
      (SELECT COUNT(*)::int FROM payment_observation_source_rows WHERE NOT excluded) AS "survivingStageRows",
      (SELECT COUNT(*)::int FROM payment_observations) AS "derivedPaymentObservations",
      (
        SELECT COUNT(*)::int
        FROM payment_observations
        WHERE lower("data"->>'is_small_business') = 'true'
      ) AS "sbiPositiveObservations",
      (SELECT COUNT(*)::int FROM earlytrade_omitted_invoices) AS "earlytradeMatches",
      (
        SELECT COUNT(*)::int
        FROM payment_observations
        WHERE lower("data"->>'is_small_business') = 'true'
          AND NULLIF(BTRIM(COALESCE("data"->>'payment_date', '')), '') IS NULL
      ) AS "sbiObservationsMissingPaymentDate",
      (
        SELECT COUNT(*)::int
        FROM payment_observations
        WHERE lower("data"->>'is_small_business') = 'true'
          AND (
            "sourceInvoiceStageRowId" IS NULL
            OR COALESCE(cardinality("settlementStageRowIds"), 0) = 0
          )
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

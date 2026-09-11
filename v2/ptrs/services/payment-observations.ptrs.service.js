const db = require("@/db/database");
const {
  buildPersistedPaymentNormalisationCte,
  requireCurrentPaymentNormalisationResult,
  VEOLIA_PAYMENT_TIME_REFERENCE_POLICY,
} = require("./payment-normalisation.ptrs.service");

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
    ${buildPersistedPaymentNormalisationCte()},
    payment_observation_accounting_allocations AS MATERIALIZED (
      SELECT
        allocation.*,
        invoice."sourceGroupScope" AS source_group_scope,
        payment."rowNo" AS payment_row_no,
        CASE
          WHEN allocation.payment_time_reference_policy IS NOT NULL
            THEN invoice."meta"->'canonical'->'lineage'
              ->'canonicalSources'->'invoice_receipt_date'
          ELSE NULL
        END AS payment_time_canonical_source
      FROM payment_normalisation_payment_allocations allocation
      JOIN "tbl_ptrs_stage_row" invoice
        ON invoice."id" = allocation.invoice_stage_row_id
       AND invoice."customerId" = :customerId
       AND invoice."ptrsId" = :ptrsId
       AND invoice."deletedAt" IS NULL
      JOIN "tbl_ptrs_stage_row" payment
        ON payment."id" = allocation.payment_stage_row_id
       AND payment."customerId" = :customerId
       AND payment."ptrsId" = :ptrsId
       AND payment."deletedAt" IS NULL
      WHERE allocation.normalisation_result_id = :normalisationResultId
        AND NOT (
          COALESCE((invoice."data"->>'exclude_from_metrics')::boolean, false)
          OR COALESCE((invoice."meta"->'rules'->>'exclude')::boolean, false)
        )
        AND NOT (
          COALESCE((payment."data"->>'exclude_from_metrics')::boolean, false)
          OR COALESCE((payment."meta"->'rules'->>'exclude')::boolean, false)
        )
    ),
    payment_observation_direct_keys AS MATERIALIZED (
      SELECT
        direct.*,
        persisted."normalisationGroupKey" AS normalisation_group_key,
        persisted."companyCode" AS company_code,
        persisted."sourceAccountCode" AS source_account_code,
        persisted."clearingDocument" AS clearing_document
      FROM "tbl_ptrs_payment_normalisation_row" persisted
      JOIN payment_normalisation_result result
        ON result."id" = persisted."normalisationResultId"
      JOIN "tbl_ptrs_stage_row" direct
        ON direct."id" = persisted."stageRowId"
       AND direct."customerId" = :customerId
       AND direct."ptrsId" = :ptrsId
       AND direct."deletedAt" IS NULL
      WHERE persisted."normalisationResultId" = :normalisationResultId
        AND persisted."normalisationRole" = 'DIRECT_PAYMENT'
        AND direct."semanticKind" = 'direct_payment'
        AND NOT (
          COALESCE((direct."data"->>'exclude_from_metrics')::boolean, false)
          OR COALESCE((direct."meta"->'rules'->>'exclude')::boolean, false)
        )
    ),
    payment_observation_accounting_observations AS (
      SELECT
        'payment-observation:' || invoice."id" || ':' || payment."id"
          AS "observationId",
        'accounting_event'::text AS "observationSourceType",
        invoice."customerId", invoice."ptrsId", invoice."profileId",
        invoice."datasetId" AS "sourceDatasetId",
        invoice."canonicalRevisionId", invoice."canonicalSourceRowId",
        invoice."sourceRawRowId", invoice."sourceRowNo",
        invoice."adapterType", invoice."adapterVersion",
        invoice."semanticKind", invoice."sourceGroupScope",
        allocation.normalisation_group_key AS "sourceGroupKey",
        COALESCE(
          invoice."meta"->'canonical'->'lineage'->'joinedReferences',
          '{}'::jsonb
        ) AS "joinedReferenceLineage",
        jsonb_build_array(
          jsonb_build_object(
            'stageRowId', invoice."id",
            'canonicalRevisionId', invoice."canonicalRevisionId",
            'canonicalSourceRowId', invoice."canonicalSourceRowId",
            'datasetId', invoice."datasetId",
            'sourceRawRowId', invoice."sourceRawRowId",
            'sourceRowNo', invoice."sourceRowNo",
            'adapterType', invoice."adapterType",
            'adapterVersion', invoice."adapterVersion",
            'semanticKind', invoice."semanticKind",
            'sourceGroupScope', invoice."sourceGroupScope"
          ),
          jsonb_build_object(
            'stageRowId', payment."id",
            'canonicalRevisionId', payment."canonicalRevisionId",
            'canonicalSourceRowId', payment."canonicalSourceRowId",
            'datasetId', payment."datasetId",
            'sourceRawRowId', payment."sourceRawRowId",
            'sourceRowNo', payment."sourceRowNo",
            'adapterType', payment."adapterType",
            'adapterVersion', payment."adapterVersion",
            'semanticKind', payment."semanticKind",
            'sourceGroupScope', payment."sourceGroupScope"
          )
        ) AS "sourceProvenance",
        payment."rowNo",
        payment."id" AS "primarySourceStageRowId",
        invoice."id" AS "sourceInvoiceStageRowId",
        ARRAY[payment."id"] AS "settlementStageRowIds",
        ARRAY[invoice."id", payment."id"] AS "sourceStageRowIds",
        allocation.company_code AS "sourceCompanyCode",
        allocation.source_account_code AS "sourceAccountCode",
        allocation.clearing_document AS "clearingDocument",
        allocation.allocated_amount AS "paymentAmount",
        allocation.settlement_payment_date::text AS "paymentDate",
        jsonb_set(
          jsonb_set(
            jsonb_set(
              jsonb_set(
                jsonb_set(
                  jsonb_set(
                    jsonb_set(
                      jsonb_set(
                        COALESCE(invoice."data", '{}'::jsonb),
                        '{payment_amount}', to_jsonb(allocation.allocated_amount), true
                      ),
                      '{payment_date}', COALESCE(to_jsonb(allocation.settlement_payment_date), 'null'::jsonb), true
                    ),
                    '{partial_payment}', to_jsonb(allocation.obligation_after_payment > 0.005), true
                  ),
                  '{original_obligation_amount}', to_jsonb(allocation.original_obligation_amount), true
                ),
                '{adjusted_obligation_amount}', to_jsonb(allocation.adjusted_obligation_amount), true
              ),
              '{obligation_before_payment}', to_jsonb(allocation.obligation_before_payment), true
            ),
            '{obligation_after_payment}', to_jsonb(allocation.obligation_after_payment), true
          ),
          '{payment_time_days}', COALESCE(to_jsonb(allocation.payment_time_days), 'null'::jsonb), true
        ) || jsonb_build_object(
          'payment_time_reference_kind', allocation.payment_time_reference_kind,
          'payment_time_reference_date', allocation.payment_time_reference_date,
          'payment_time_reference_policy', allocation.payment_time_reference_policy,
          'payment_time_reference_reason', allocation.payment_time_reference_reason,
          'normalisation_reason_code', CASE
            WHEN allocation.obligation_after_payment > 0.005 THEN 'PARTIAL_PAYMENT'
            ELSE allocation.mapping_exception_code
          END,
          'normalisation_exception_code', allocation.mapping_exception_code
        ) AS "data",
        jsonb_set(
          COALESCE(invoice."meta", '{}'::jsonb),
          '{paymentObservation}',
          jsonb_build_object(
            'observationSourceType', 'accounting_event',
            'sourceGroupKey', allocation.normalisation_group_key,
            'sourceInvoiceStageRowId', invoice."id",
            'settlementStageRowIds', ARRAY[payment."id"],
            'paymentAmountSemantic', 'actual_settlement_allocation',
            'originalObligationAmount', allocation.original_obligation_amount,
            'adjustedObligationAmount', allocation.adjusted_obligation_amount,
            'obligationBeforePayment', allocation.obligation_before_payment,
            'obligationAfterPayment', allocation.obligation_after_payment,
            'partialPayment', allocation.obligation_after_payment > 0.005,
            'finalSettlement', allocation.obligation_after_payment <= 0.005,
            'classificationBasis', CASE
              WHEN UPPER(BTRIM(COALESCE(payment."documentType", ''))) = 'KZ'
                THEN 'outstanding_obligation_after_clearing_settlement'
              ELSE 'outstanding_obligation_after_zp'
            END,
            'contractualInstalmentIndicatorAvailable', false,
            'paymentTimeReference', jsonb_strip_nulls(jsonb_build_object(
              'policy', allocation.payment_time_reference_policy,
              'reason', allocation.payment_time_reference_reason,
              'referenceKind', allocation.payment_time_reference_kind,
              'referenceDate', allocation.payment_time_reference_date,
              'canonicalConcept', CASE
                WHEN allocation.payment_time_reference_policy IS NOT NULL
                  THEN '${VEOLIA_PAYMENT_TIME_REFERENCE_POLICY.canonicalConcept}'
                ELSE NULL
              END,
              'canonicalSource', allocation.payment_time_canonical_source
            )),
            'reasonCode', CASE
              WHEN allocation.obligation_after_payment > 0.005 THEN 'PARTIAL_PAYMENT'
              ELSE allocation.mapping_exception_code
            END,
            'exceptionCode', allocation.mapping_exception_code
          ),
          true
        ) AS "meta"
      FROM payment_observation_accounting_allocations allocation
      JOIN "tbl_ptrs_stage_row" invoice
        ON invoice."id" = allocation.invoice_stage_row_id
       AND invoice."customerId" = :customerId
       AND invoice."ptrsId" = :ptrsId
       AND invoice."deletedAt" IS NULL
      JOIN "tbl_ptrs_stage_row" payment
        ON payment."id" = allocation.payment_stage_row_id
       AND payment."customerId" = :customerId
       AND payment."ptrsId" = :ptrsId
       AND payment."deletedAt" IS NULL
    ),
    payment_observation_direct_observations AS (
      SELECT
        'payment-observation:' || direct."id" AS "observationId",
        'direct_payment'::text AS "observationSourceType",
        direct."customerId", direct."ptrsId", direct."profileId",
        direct."datasetId" AS "sourceDatasetId",
        direct."canonicalRevisionId", direct."canonicalSourceRowId",
        direct."sourceRawRowId", direct."sourceRowNo",
        direct."adapterType", direct."adapterVersion",
        direct."semanticKind", direct."sourceGroupScope",
        direct.normalisation_group_key AS "sourceGroupKey",
        COALESCE(direct."meta"->'canonical'->'lineage'->'joinedReferences', '{}'::jsonb)
          AS "joinedReferenceLineage",
        jsonb_build_array(jsonb_build_object(
          'stageRowId', direct."id",
          'canonicalRevisionId', direct."canonicalRevisionId",
          'canonicalSourceRowId', direct."canonicalSourceRowId",
          'datasetId', direct."datasetId",
          'sourceRawRowId', direct."sourceRawRowId",
          'sourceRowNo', direct."sourceRowNo",
          'adapterType', direct."adapterType",
          'adapterVersion', direct."adapterVersion",
          'semanticKind', direct."semanticKind",
          'sourceGroupScope', direct."sourceGroupScope"
        )) AS "sourceProvenance",
        direct."rowNo", direct."id" AS "primarySourceStageRowId",
        NULL::varchar AS "sourceInvoiceStageRowId",
        ARRAY[]::varchar[] AS "settlementStageRowIds",
        ARRAY[direct."id"] AS "sourceStageRowIds",
        direct.company_code AS "sourceCompanyCode",
        direct.source_account_code AS "sourceAccountCode",
        direct.clearing_document AS "clearingDocument",
        direct."paymentAmount", direct."paymentDate"::text AS "paymentDate",
        jsonb_set(COALESCE(direct."data", '{}'::jsonb), '{partial_payment}', 'false'::jsonb, true) AS "data",
        jsonb_set(COALESCE(direct."meta", '{}'::jsonb), '{paymentObservation}',
          jsonb_build_object(
            'observationSourceType', 'direct_payment',
            'paymentAmountSemantic', 'actual_settlement_amount',
            'primarySourceStageRowId', direct."id",
            'sourceGroupKey', direct.normalisation_group_key,
            'partialPayment', false
          ), true) AS "meta"
      FROM payment_observation_direct_keys direct
    ),
    payment_observations AS (
      SELECT * FROM payment_observation_accounting_observations
      UNION ALL
      SELECT * FROM payment_observation_direct_observations
    ),
    payment_observation_accounting_settlement_groups AS (
      SELECT
        'accounting_event'::text AS "observationSourceType",
        allocation.normalisation_group_key AS "sourceGroupKey",
        MAX(allocation.source_group_scope) AS "sourceGroupScope",
        allocation.company_code AS "sourceCompanyCode",
        allocation.source_account_code AS "sourceAccountCode",
        allocation.clearing_document AS "clearingDocument",
        'payment:' || allocation.payment_stage_row_id AS "settlementIdentity",
        SUM(allocation.allocated_amount)::numeric AS "settlementPaymentAmount"
      FROM payment_observation_accounting_allocations allocation
      GROUP BY allocation.normalisation_group_key, allocation.company_code,
        allocation.source_account_code, allocation.clearing_document,
        allocation.payment_stage_row_id
    ),
    payment_observation_settlement_groups AS (
      SELECT * FROM payment_observation_accounting_settlement_groups
      UNION ALL
      SELECT
        'direct_payment'::text, direct.normalisation_group_key,
        direct."sourceGroupScope", direct.company_code,
        direct.source_account_code, direct.clearing_document,
        'payment-observation:' || direct."id", direct."paymentAmount"
      FROM payment_observation_direct_keys direct
    )
  `;
}

function buildPaymentObservationSummaryCte() {
  return `
    ${buildPaymentObservationsCte()},
    payment_observation_summary_counts AS (
      SELECT
        (SELECT COUNT(*)::int FROM payment_normalisation_source_rows)
          AS "sourceStageRows",
        (SELECT COUNT(*)::int FROM payment_normalisation_source_rows WHERE excluded)
          AS "excludedStageRows",
        (SELECT COUNT(*)::int FROM payment_normalisation_source_rows WHERE NOT excluded)
          AS "survivingStageRows",
        (SELECT COUNT(*)::int FROM payment_observations)
          AS "derivedPaymentObservations",
        (SELECT COALESCE(SUM(ABS("paymentAmount")), 0)::numeric
          FROM payment_observations) AS "tcpPaymentValue",
        (SELECT COUNT(*)::int FROM payment_observations
          WHERE lower(data->>'is_small_business') = 'true')
          AS "sbiPositiveObservations",
        (SELECT COALESCE(SUM(ABS("paymentAmount")), 0)::numeric
          FROM payment_observations
          WHERE lower(data->>'is_small_business') = 'true')
          AS "sbiPositivePaymentValue",
        (SELECT COUNT(*)::int FROM payment_observations
          WHERE lower(COALESCE(data->>'is_small_business', ''))
            NOT IN ('true', 'false')) AS "sbiUnclassifiedObservations",
        (SELECT COALESCE((summary->>'earlyTradeMatchCount')::int, 0)
          FROM payment_normalisation_result) AS "earlytradeMatches",
        (SELECT COUNT(*)::int FROM payment_observations
          WHERE lower(data->>'is_small_business') = 'true'
            AND NULLIF(data->>'payment_date', '') IS NULL)
          AS "sbiObservationsMissingPaymentDate",
        0::int AS "sbiObservationsMissingSourceTrace",
        (SELECT COUNT(*)::int FROM payment_observations
          WHERE lower(data->>'partial_payment') = 'true') AS "partialPayments",
        (SELECT COALESCE(SUM(ABS("paymentAmount")), 0)::numeric
          FROM payment_observations
          WHERE lower(data->>'partial_payment') = 'true') AS "partialPaymentValue",
        (SELECT COUNT(*)::int FROM payment_observations
          WHERE lower(data->>'is_small_business') = 'true'
            AND lower(COALESCE(data->>'partial_payment', 'false')) <> 'true')
          AS "sbiNonPartialObservations",
        (SELECT COUNT(*)::int FROM payment_observations
          WHERE lower(data->>'is_small_business') = 'true'
            AND lower(COALESCE(data->>'partial_payment', 'false')) <> 'true'
            AND NULLIF(data->>'payment_time_days', '') IS NOT NULL)
          AS "paymentTimePopulationCount",
        (SELECT COALESCE(SUM(ABS("paymentAmount")), 0)::numeric
          FROM payment_observations
          WHERE lower(data->>'is_small_business') = 'true'
            AND lower(COALESCE(data->>'partial_payment', 'false')) <> 'true'
            AND NULLIF(data->>'payment_time_days', '') IS NOT NULL)
          AS "paymentTimePopulationValue",
        (SELECT COUNT(*)::int FROM payment_normalisation_exceptions)
          AS "normalisationExceptions"
    )
  `;
}

function getPaymentObservationReplacements({
  customerId,
  ptrsId,
  normalisationResultId,
}) {
  return {
    customerId,
    ptrsId,
    normalisationResultId,
    ...PAYMENT_OBSERVATION_REPLACEMENTS,
  };
}

async function resolveNormalisationResultId({
  customerId,
  ptrsId,
  normalisationResultId = null,
  transaction,
}) {
  const current = await requireCurrentPaymentNormalisationResult({
    customerId,
    ptrsId,
    transaction,
  });
  if (normalisationResultId && normalisationResultId !== current.result.id) {
    const error = new Error(
      "The requested payment normalisation result is not current",
    );
    error.code = "PTRS_NORMALISATION_RESULT_STALE";
    error.statusCode = 409;
    throw error;
  }
  return current.result.id;
}

async function setPaymentObservationWorkMem({ transaction }) {
  if (!transaction) return;
  await db.sequelize.query(
    `SET LOCAL work_mem = '${PAYMENT_OBSERVATION_WORK_MEM}'`,
    { transaction },
  );
}

async function listPaymentObservations({
  customerId,
  ptrsId,
  normalisationResultId = null,
  transaction,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  normalisationResultId = await resolveNormalisationResultId({
    customerId,
    ptrsId,
    normalisationResultId,
    transaction,
  });
  await setPaymentObservationWorkMem({ transaction });
  return db.sequelize.query(
    `WITH ${buildPaymentObservationsCte()}
     SELECT * FROM payment_observations
     ORDER BY "rowNo" ASC, "observationId" ASC`,
    {
      transaction,
      replacements: getPaymentObservationReplacements({
        customerId,
        ptrsId,
        normalisationResultId,
      }),
      type: db.sequelize.QueryTypes.SELECT,
    },
  );
}

async function listPaymentObservationLinks({
  customerId,
  ptrsId,
  normalisationResultId = null,
  transaction,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  normalisationResultId = await resolveNormalisationResultId({
    customerId,
    ptrsId,
    normalisationResultId,
    transaction,
  });
  await setPaymentObservationWorkMem({ transaction });
  return db.sequelize.query(
    `WITH ${buildPaymentObservationsCte()}
     SELECT "observationId", "observationSourceType", "primarySourceStageRowId",
       "sourceInvoiceStageRowId", "settlementStageRowIds", "sourceCompanyCode",
       "sourceAccountCode", "clearingDocument", "rowNo"
     FROM payment_observations
     ORDER BY "rowNo" ASC, "observationId" ASC`,
    {
      transaction,
      replacements: getPaymentObservationReplacements({
        customerId,
        ptrsId,
        normalisationResultId,
      }),
      type: db.sequelize.QueryTypes.SELECT,
    },
  );
}

async function getPaymentObservationSummary({
  customerId,
  ptrsId,
  normalisationResultId = null,
  transaction,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  normalisationResultId = await resolveNormalisationResultId({
    customerId,
    ptrsId,
    normalisationResultId,
    transaction,
  });
  await setPaymentObservationWorkMem({ transaction });
  const rows = await db.sequelize.query(
    `WITH ${buildPaymentObservationSummaryCte()}
     SELECT * FROM payment_observation_summary_counts`,
    {
      transaction,
      replacements: getPaymentObservationReplacements({
        customerId,
        ptrsId,
        normalisationResultId,
      }),
      type: db.sequelize.QueryTypes.SELECT,
    },
  );
  return (
    rows?.[0] || {
      sourceStageRows: 0,
      excludedStageRows: 0,
      survivingStageRows: 0,
      derivedPaymentObservations: 0,
      sbiPositiveObservations: 0,
      earlytradeMatches: 0,
      sbiObservationsMissingPaymentDate: 0,
      sbiObservationsMissingSourceTrace: 0,
      partialPayments: 0,
      partialPaymentValue: 0,
      tcpPaymentValue: 0,
      sbiPositivePaymentValue: 0,
      sbiUnclassifiedObservations: 0,
      sbiNonPartialObservations: 0,
      paymentTimePopulationCount: 0,
      paymentTimePopulationValue: 0,
      normalisationExceptions: 0,
    }
  );
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
  resolveNormalisationResultId,
  setPaymentObservationWorkMem,
};

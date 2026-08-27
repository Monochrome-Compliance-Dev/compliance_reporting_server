const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  appendTransformationHistory,
} = require("./stage.transformation-history");
const {
  buildPaymentObservationsCte,
  getPaymentObservationReplacements,
  listPaymentObservations,
} = require("./payment-observations.ptrs.service");

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function displayRow(row) {
  return row?.rowNo == null ? row?.id : row.rowNo;
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
    const [stageRows, observations, earlytradeMatches] = await Promise.all([
      db.PtrsStageRow.findAll({
        where: { customerId, ptrsId, deletedAt: null },
        order: [["rowNo", "ASC"]],
        transaction,
      }),
      listPaymentObservations({ customerId, ptrsId, transaction }),
      db.sequelize.query(
        `
          WITH ${buildPaymentObservationsCte()}
          SELECT
            invoice."id" AS "invoiceId",
            invoice."rowNo" AS "invoiceRowNo",
            earlytrade."id" AS "earlytradeId",
            earlytrade."rowNo" AS "earlytradeRowNo",
            invoice.company_code AS "companyCode",
            invoice.source_account_code AS "sourceAccountCode",
            invoice.clearing_document AS "clearingDocument",
            invoice.description_reference AS "descriptionReference"
          FROM payment_observation_source_rows invoice
          JOIN payment_observation_source_rows earlytrade
            ON earlytrade.document_type = :paymentObservationEarlytradeType
           AND earlytrade."semanticKind" = 'accounting_event'
           AND earlytrade.source_group_key = invoice.source_group_key
           AND earlytrade.company_code = invoice.company_code
           AND earlytrade.source_account_code = invoice.source_account_code
           AND earlytrade.clearing_document = invoice.clearing_document
           AND earlytrade.description_reference = invoice.description_reference
          WHERE invoice."semanticKind" = 'accounting_event'
            AND invoice.document_type = :paymentObservationInvoiceType
          ORDER BY invoice."rowNo", earlytrade."rowNo"
        `,
        {
          transaction,
          replacements: getPaymentObservationReplacements({
            customerId,
            ptrsId,
          }),
          type: db.sequelize.QueryTypes.SELECT,
        },
      ),
    ]);

    const byId = new Map(stageRows.map((row) => [row.id, row]));
    const changed = new Set();
    const append = (row, event) => {
      if (!row) return;
      const nextMeta = appendTransformationHistory(row.meta, event);
      if (nextMeta === row.meta) return;
      row.meta = nextMeta;
      changed.add(row.id);
    };

    for (const row of stageRows) {
      const exclusionComments = asArray(row?.meta?.exclusions?.comments);
      const exclusionReasons = asArray(row?.meta?.exclusions?.reasons);
      for (const comment of exclusionComments) {
        append(row, {
          key: `exclusion:${Buffer.from(String(comment)).toString("base64url")}`,
          kind: "exclusion",
          comment: String(comment),
          sourceStageRowIds: [row.id],
          targetStageRowIds: [row.id],
          details: { reasons: exclusionReasons },
        });
      }

      const data = row.data || {};
      if (
        data.contract_po_payment_terms_effective_source === "TERM_CHANGES" &&
        data.contract_po_payment_terms_effective
      ) {
        const changedAt =
          data.contract_po_payment_terms_effective_changed_at || null;
        append(row, {
          key: `payment-term-change:${changedAt || "unknown"}:${data.contract_po_payment_terms_effective}`,
          kind: "payment_term_override",
          comment: `Payment term overridden to ${data.contract_po_payment_terms_effective} by supplier term change effective on ${changedAt || "an unspecified date"}`,
          sourceStageRowIds: [row.id],
          targetStageRowIds: [row.id],
          details: {
            effectiveTerm: data.contract_po_payment_terms_effective,
            effectiveDate: changedAt,
            source: "TERM_CHANGES",
          },
        });
      }

      if (data.payment_time_days != null && data.payment_time_reference_kind) {
        append(row, {
          key: `payment-time:${data.payment_time_reference_kind}:${data.payment_time_reference_date || "unknown"}:${data.payment_time_days}`,
          kind: "payment_time",
          comment: `Payment-time reference chosen from ${String(data.payment_time_reference_kind).replaceAll("_", " ")} (${data.payment_time_reference_date || "date unavailable"}); derived payment time ${data.payment_time_days} day(s)`,
          sourceStageRowIds: [row.id],
          targetStageRowIds: [row.id],
          details: {
            referenceKind: data.payment_time_reference_kind,
            referenceDate: data.payment_time_reference_date || null,
            paymentTimeDays: data.payment_time_days,
          },
        });
      }
    }

    for (const observation of observations) {
      const primary = byId.get(observation.primarySourceStageRowId);
      if (observation.observationSourceType === "direct_payment") {
        append(primary, {
          key: `direct-payment-observation:${observation.observationId}`,
          kind: "payment_observation_direct",
          comment: `Stage row ${displayRow(primary)} produced a direct payment observation without SAP event reconstruction`,
          sourceStageRowIds: [primary?.id].filter(Boolean),
          targetStageRowIds: [primary?.id].filter(Boolean),
          details: {
            observationId: observation.observationId,
            sourceDatasetId: observation.sourceDatasetId,
            canonicalRevisionId: observation.canonicalRevisionId,
          },
        });
        continue;
      }
      const invoice =
        byId.get(observation.sourceInvoiceStageRowId) || primary;
      for (const settlementId of observation.settlementStageRowIds || []) {
        const settlement = byId.get(settlementId);
        const key = `payment-observation-anchor:${invoice?.id}:${settlementId}`;
        const comment = `ZP row ${displayRow(settlement)} used as payment anchor for RE row ${displayRow(invoice)}`;
        const event = {
          key,
          kind: "payment_observation_anchor",
          comment,
          sourceStageRowIds: [settlementId],
          targetStageRowIds: [invoice?.id].filter(Boolean),
          details: {
            companyCode: observation.sourceCompanyCode,
            sourceAccountCode: observation.sourceAccountCode,
            clearingDocument: observation.clearingDocument,
            observationId: observation.observationId,
          },
        };
        append(invoice, event);
        append(settlement, event);
      }
    }

    for (const match of earlytradeMatches) {
      const invoice = byId.get(match.invoiceId);
      const earlytrade = byId.get(match.earlytradeId);
      const key = `earlytrade-observation-omission:${match.invoiceId}:${match.earlytradeId}`;
      const details = {
        companyCode: match.companyCode,
        sourceAccountCode: match.sourceAccountCode,
        clearingDocument: match.clearingDocument,
        descriptionReference: match.descriptionReference,
      };
      append(earlytrade, {
        key,
        kind: "earlytrade_match",
        comment: `ET row ${displayRow(earlytrade)} matched to RE row ${displayRow(invoice)} using Company Code + Account + Clearing Document + Reference`,
        sourceStageRowIds: [match.earlytradeId],
        targetStageRowIds: [match.invoiceId],
        details,
      });
      append(invoice, {
        key,
        kind: "payment_observation_omission",
        comment: `RE row ${displayRow(invoice)} omitted from derived payment observations because matched ET row ${displayRow(earlytrade)} treatment applies`,
        sourceStageRowIds: [match.earlytradeId],
        targetStageRowIds: [match.invoiceId],
        details,
      });
    }

    for (const id of changed) {
      await byId.get(id).save({ transaction, fields: ["meta", "updatedAt"] });
    }

    if (ownsTransaction) await transaction.commit();
    return {
      rowsUpdated: changed.size,
      paymentObservationLinks: observations.length,
      earlytradeMatches: earlytradeMatches.length,
    };
  } catch (error) {
    if (ownsTransaction && !transaction.finished) await transaction.rollback();
    throw error;
  }
}

module.exports = { recordStageTransformationHistory };

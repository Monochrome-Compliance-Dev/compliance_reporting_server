const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { buildStableInputHash } = require("./ptrs.service");

const DOCUMENT_TYPES = Object.freeze({
  INVOICE: "RE",
  INVOICE_KR: "KR",
  VENDOR_CREDIT_MEMO: "KG",
  PAYMENT: "ZP",
  CLEARING_PAYMENT: "KZ",
  EARLY_TRADE_DISCOUNT: "ET",
});

const ADJUSTMENT_REASONS = Object.freeze({
  CREDIT: Object.freeze({
    full: "CREDIT_FULL_OFFSET",
    partial: "CREDIT_PARTIAL_OFFSET",
  }),
  REFUND: Object.freeze({
    full: "REFUND_FULL_OFFSET",
    partial: "REFUND_PARTIAL_OFFSET",
  }),
  ET: Object.freeze({
    full: "ET_FULL_OFFSET",
    partial: "ET_PARTIAL_OFFSET",
  }),
  KG: Object.freeze({
    full: "KG_FULL_OFFSET",
    partial: "KG_PARTIAL_OFFSET",
  }),
});

const NORMALISATION_EPSILON = 0.005;
const BALANCED_CLEARING_RECONCILIATION =
  "BALANCED_CLEARING_RECONCILIATION";
const CLEARING_RECONCILIABLE_EXCEPTION_CODES = Object.freeze([
  "AMBIGUOUS_ADJUSTMENT_COMBINATION",
  "UNMATCHED_ADJUSTMENT",
  "UNMATCHED_KG_REVERSAL",
  "UNMATCHED_OBLIGATION_OFFSET",
  "UNMATCHED_PAYMENT",
  "UNRECOGNISED_DOCUMENT_TYPE",
]);
const PAYMENT_NORMALISATION_VERSION = "veolia-payment-normalisation-v6";
const NORMALISATION_RESULT_MISSING = "PTRS_NORMALISATION_RESULT_MISSING";
const VEOLIA_PAYMENT_TIME_REFERENCE_POLICY = Object.freeze({
  id: "veolia_ariba_invoice_receipt_v1",
  reason: "veolia_ordinary_invoice_ariba_receipt_date",
  canonicalConcept: "invoiceReceiptDate",
  sourceRole: "invoices",
  sourceColumn: "Invoice Date Created - Date",
});

function buildPaymentNormalisationInputSignature({
  ptrsId,
  profileId,
  stageExecutionRunId,
  stageInputHash,
  normalisationInputRevision,
}) {
  return buildStableInputHash({
    ptrsId,
    profileId,
    stageExecutionRunId,
    stageInputHash,
    normalisationInputRevision: String(normalisationInputRevision ?? "0"),
  });
}

async function readPaymentNormalisationInputState({
  customerId,
  ptrsId,
  profileId = null,
  transaction,
}) {
  const report = await db.Ptrs.findOne({
    where: { id: ptrsId, customerId },
    attributes: ["id", "profileId", "normalisationInputRevision"],
    raw: true,
    transaction,
  });
  const stageExecution = await db.PtrsExecutionRun.findOne({
    where: { customerId, ptrsId, step: "stage", status: "success" },
    attributes: ["id", "profileId", "inputHash", "finishedAt"],
    order: [
      ["startedAt", "DESC"],
      ["id", "DESC"],
    ],
    raw: true,
    transaction,
  });
  if (!report) {
    const error = new Error("Ptrs not found");
    error.statusCode = 404;
    throw error;
  }
  const resolvedProfileId = profileId || report.profileId;
  if (!resolvedProfileId || resolvedProfileId !== report.profileId) {
    throw new Error("PTRS profile does not match the staged report profile");
  }
  if (!stageExecution?.id || !stageExecution.inputHash) {
    const error = new Error(
      "A successful Stage execution is required before payment normalisation",
    );
    error.statusCode = 409;
    throw error;
  }
  const state = {
    ptrsId,
    profileId: resolvedProfileId,
    stageExecutionRunId: stageExecution.id,
    stageInputHash: stageExecution.inputHash,
    stageCompletedAt: stageExecution.finishedAt || null,
    normalisationInputRevision: String(
      report.normalisationInputRevision ?? "0",
    ),
  };
  return {
    ...state,
    inputSignature: buildPaymentNormalisationInputSignature(state),
  };
}

async function acquirePaymentNormalisationLock({ transaction, identity }) {
  await db.sequelize.query(
    `SELECT pg_advisory_xact_lock(hashtextextended(:lockKey, 0))`,
    {
      transaction,
      replacements: {
        lockKey: `ptrs:payment-normalisation:${identity}`,
      },
      type: db.sequelize.QueryTypes.SELECT,
    },
  );
}

async function requireCurrentPaymentNormalisationResult({
  customerId,
  ptrsId,
  profileId = null,
  calculationVersion = PAYMENT_NORMALISATION_VERSION,
  transaction,
}) {
  if (!transaction) {
    throw new Error("transaction is required to resolve payment normalisation");
  }
  const inputState = await readPaymentNormalisationInputState({
    customerId,
    ptrsId,
    profileId,
    transaction,
  });
  const result = await db.PtrsPaymentNormalisationResult.findOne({
    where: {
      customerId,
      ptrsId,
      inputSignature: inputState.inputSignature,
      calculationVersion,
      status: "succeeded",
    },
    raw: true,
    transaction,
  });
  if (!result) {
    const error = new Error(
      "The authoritative payment normalisation result is missing or stale",
    );
    error.code = NORMALISATION_RESULT_MISSING;
    error.statusCode = 409;
    throw error;
  }
  return { result, inputState };
}

function buildPersistedPaymentNormalisationCte() {
  return `
    payment_normalisation_result AS MATERIALIZED (
      SELECT result.*
      FROM "tbl_ptrs_payment_normalisation_result" result
      WHERE result."id" = :normalisationResultId
        AND result."customerId" = :customerId
        AND result."ptrsId" = :ptrsId
        AND result."status" = 'succeeded'
    ),
    payment_normalisation_source_rows AS MATERIALIZED (
      SELECT
        persisted."stageRowId" AS "id",
        stage."datasetId",
        stage."semanticKind",
        stage."sourceGroupScope",
        persisted."rowNo",
        stage."sourceAccountCode",
        stage."description",
        stage."documentType",
        stage."clearingDocument",
        stage."paymentAmount",
        stage."paymentDate",
        stage."invoiceIssueDate",
        stage."invoiceReceiptDate",
        persisted."documentType" AS document_type,
        persisted."companyCode" AS company_code,
        persisted."sourceAccountCode" AS source_account_code,
        persisted."clearingDocument" AS clearing_document,
        NULLIF(BTRIM(stage."description"), '') AS description_reference,
        persisted."normalisationGroupKey" AS normalisation_group_key,
        persisted."normalisationAmount" AS normalisation_amount,
        persisted."normalisationRole" AS normalisation_role,
        COALESCE((stage."data"->>'exclude_from_metrics')::boolean, false)
          OR COALESCE((stage."meta"->'rules'->>'exclude')::boolean, false)
          AS excluded,
        persisted."originalObligationAmount" AS original_obligation_amount,
        persisted."adjustedObligationAmount" AS adjusted_obligation_amount,
        persisted."adjustmentAllocatedAmount" AS adjustment_allocated_amount,
        persisted."paymentAllocatedAmount" AS payment_allocated_amount,
        persisted."outstandingAmount" AS outstanding_amount,
        persisted."unmatchedAmount" AS unmatched_amount,
        persisted."reversalOffsetAmount" AS reversal_offset_amount,
        persisted."exceptionCode" AS exception_code
      FROM "tbl_ptrs_payment_normalisation_row" persisted
      JOIN payment_normalisation_result result
        ON result."id" = persisted."normalisationResultId"
      JOIN "tbl_ptrs_stage_row" stage
        ON stage."id" = persisted."stageRowId"
       AND stage."customerId" = :customerId
       AND stage."ptrsId" = :ptrsId
       AND stage."deletedAt" IS NULL
      WHERE persisted."normalisationResultId" = :normalisationResultId
    ),
    payment_normalisation_obligations AS MATERIALIZED (
      SELECT * FROM payment_normalisation_source_rows
      WHERE normalisation_role = 'INVOICE'
    ),
    payment_normalisation_adjustments AS MATERIALIZED (
      SELECT * FROM payment_normalisation_source_rows
      WHERE normalisation_role IN ('CREDIT', 'REFUND', 'ET', 'KG')
    ),
    payment_normalisation_adjusted_obligations AS MATERIALIZED (
      SELECT * FROM payment_normalisation_obligations
    ),
    payment_normalisation_payments AS MATERIALIZED (
      SELECT * FROM payment_normalisation_source_rows
      WHERE normalisation_role = 'PAYMENT'
    ),
    payment_normalisation_payment_allocations AS MATERIALIZED (
      SELECT
        allocation."normalisationResultId" AS normalisation_result_id,
        allocation."invoiceStageRowId" AS invoice_stage_row_id,
        allocation."paymentStageRowId" AS payment_stage_row_id,
        allocation."paymentSequence" AS payment_sequence,
        allocation."normalisationGroupKey" AS normalisation_group_key,
        allocation."companyCode" AS company_code,
        allocation."sourceAccountCode" AS source_account_code,
        allocation."clearingDocument" AS clearing_document,
        allocation."allocatedAmount" AS allocated_amount,
        allocation."adjustedObligationAmount" AS adjusted_obligation_amount,
        allocation."originalObligationAmount" AS original_obligation_amount,
        allocation."adjustmentAllocatedAmount" AS adjustment_allocated_amount,
        allocation."settlementPaymentDate" AS settlement_payment_date,
        allocation."sourcePaymentAmount" AS source_payment_amount,
        allocation."obligationBeforePayment" AS obligation_before_payment,
        allocation."obligationAfterPayment" AS obligation_after_payment,
        allocation."paymentTimeDays" AS payment_time_days,
        allocation."paymentTimeReferenceKind" AS payment_time_reference_kind,
        allocation."paymentTimeReferenceDate" AS payment_time_reference_date,
        allocation."paymentTimeReferencePolicy" AS payment_time_reference_policy,
        allocation."paymentTimeReferenceReason" AS payment_time_reference_reason,
        allocation."mappingExceptionCode" AS mapping_exception_code
      FROM "tbl_ptrs_payment_normalisation_allocation" allocation
      JOIN payment_normalisation_result result
        ON result."id" = allocation."normalisationResultId"
      WHERE allocation."normalisationResultId" = :normalisationResultId
    ),
    payment_normalisation_exceptions AS MATERIALIZED (
      SELECT
        exception."sourceStageRowId" AS source_stage_row_id,
        exception."reasonCode" AS reason_code,
        exception."amount" AS amount
      FROM "tbl_ptrs_payment_normalisation_exception" exception
      JOIN payment_normalisation_result result
        ON result."id" = exception."normalisationResultId"
      WHERE exception."normalisationResultId" = :normalisationResultId
    )
  `;
}

function buildPaymentTimeAllocationProjectionSql({
  invoiceAlias = "invoice",
  allocationAlias = "allocation",
} = {}) {
  const rctiSql = `lower(COALESCE(${invoiceAlias}."data"->>'rcti', ''))
    IN ('yes', 'y', 'true', '1')`;
  const aribaApplicableSql = `${invoiceAlias}."adapterType" = 'sap_accounting_event'
    AND ${invoiceAlias}."meta"->'canonical'->'lineage'->'canonicalSources'->'invoice_receipt_date'->>'sourceRole'
      = '${VEOLIA_PAYMENT_TIME_REFERENCE_POLICY.sourceRole}'
    AND ${invoiceAlias}."meta"->'canonical'->'lineage'->'canonicalSources'->'invoice_receipt_date'->>'sourceColumn'
      = '${VEOLIA_PAYMENT_TIME_REFERENCE_POLICY.sourceColumn}'
    AND ${invoiceAlias}."invoiceReceiptDate" IS NOT NULL
    AND NOT (${rctiSql})`;
  const paymentTimeDaysSql = `CASE
    WHEN ${allocationAlias}.settlement_payment_date IS NULL
      OR (${invoiceAlias}."invoiceIssueDate" IS NULL
        AND ${invoiceAlias}."invoiceReceiptDate" IS NULL) THEN NULL
    WHEN ${aribaApplicableSql} THEN
      CASE WHEN ${allocationAlias}.settlement_payment_date - ${invoiceAlias}."invoiceReceiptDate" <= 0
        THEN 0 ELSE ${allocationAlias}.settlement_payment_date - ${invoiceAlias}."invoiceReceiptDate" + 1 END
    WHEN ${invoiceAlias}."invoiceIssueDate" IS NOT NULL
      AND ${invoiceAlias}."invoiceReceiptDate" IS NOT NULL THEN LEAST(
      CASE WHEN ${allocationAlias}.settlement_payment_date - ${invoiceAlias}."invoiceIssueDate" <= 0
        THEN 0 ELSE ${allocationAlias}.settlement_payment_date - ${invoiceAlias}."invoiceIssueDate" + 1 END,
      CASE WHEN ${allocationAlias}.settlement_payment_date - ${invoiceAlias}."invoiceReceiptDate" <= 0
        THEN 0 ELSE ${allocationAlias}.settlement_payment_date - ${invoiceAlias}."invoiceReceiptDate" + 1 END
    )
    WHEN ${invoiceAlias}."invoiceIssueDate" IS NOT NULL THEN
      CASE WHEN ${allocationAlias}.settlement_payment_date - ${invoiceAlias}."invoiceIssueDate" <= 0
        THEN 0 ELSE ${allocationAlias}.settlement_payment_date - ${invoiceAlias}."invoiceIssueDate" + 1 END
    ELSE
      CASE WHEN ${allocationAlias}.settlement_payment_date - ${invoiceAlias}."invoiceReceiptDate" <= 0
        THEN 0 ELSE ${allocationAlias}.settlement_payment_date - ${invoiceAlias}."invoiceReceiptDate" + 1 END
  END`;
  const referenceKindSql = `CASE
    WHEN ${allocationAlias}.settlement_payment_date IS NULL
      OR (${invoiceAlias}."invoiceIssueDate" IS NULL
        AND ${invoiceAlias}."invoiceReceiptDate" IS NULL) THEN NULL
    WHEN ${aribaApplicableSql} THEN 'invoice_receipt'
    WHEN ${invoiceAlias}."invoiceIssueDate" IS NOT NULL
      AND ${invoiceAlias}."invoiceReceiptDate" IS NOT NULL
      AND (${allocationAlias}.settlement_payment_date - ${invoiceAlias}."invoiceIssueDate")
      <= (${allocationAlias}.settlement_payment_date - ${invoiceAlias}."invoiceReceiptDate")
      THEN 'invoice_issue'
    WHEN ${invoiceAlias}."invoiceReceiptDate" IS NOT NULL THEN 'invoice_receipt'
    ELSE 'invoice_issue'
  END`;
  return {
    paymentTimeDaysSql,
    referenceKindSql,
    referenceDateSql: `CASE ${referenceKindSql}
      WHEN 'invoice_issue' THEN ${invoiceAlias}."invoiceIssueDate"
      WHEN 'invoice_receipt' THEN ${invoiceAlias}."invoiceReceiptDate"
      ELSE NULL
    END`,
    referencePolicySql: `CASE WHEN ${aribaApplicableSql}
      THEN '${VEOLIA_PAYMENT_TIME_REFERENCE_POLICY.id}' ELSE NULL END`,
    referenceReasonSql: `CASE WHEN ${aribaApplicableSql}
      THEN '${VEOLIA_PAYMENT_TIME_REFERENCE_POLICY.reason}' ELSE NULL END`,
  };
}

function firstValue(row, keys) {
  for (const key of keys) {
    const value = row?.[key];
    if (value != null && String(value).trim() !== "") return value;
  }
  return null;
}

function normaliseText(value) {
  return value == null ? "" : String(value).trim();
}

function normaliseAmount(value) {
  const parsed = Number(String(value ?? "").replace(/[$,\s]/g, ""));
  return Number.isFinite(parsed) ? Math.abs(parsed) : 0;
}

function signedAmount(value) {
  const parsed = Number(String(value ?? "").replace(/[$,\s]/g, ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

function sourceGroupKey(row) {
  const scope = normaliseText(
    firstValue(row, ["sourceGroupScope", "source_group_scope"]),
  );
  const datasetId = normaliseText(firstValue(row, ["datasetId", "dataset_id"]));
  const companyCode = normaliseText(
    firstValue(row, ["companyCode", "company_code"]),
  );
  const accountCode = normaliseText(
    firstValue(row, ["sourceAccountCode", "source_account_code"]),
  );
  const clearingDocument = normaliseText(
    firstValue(row, ["clearingDocument", "clearing_document"]),
  );
  if (!companyCode || !accountCode || !clearingDocument) return null;
  return [
    scope || `dataset:${datasetId}`,
    companyCode,
    accountCode,
    clearingDocument,
  ]
    .join("|")
    .toLowerCase();
}

function clearingDocumentPrefix(row) {
  return normaliseText(
    firstValue(row, ["clearingDocument", "clearing_document"]),
  ).slice(0, 1);
}

function classifyDocument(row, groupDocumentTypes = null) {
  const documentType = normaliseText(
    firstValue(row, ["documentType", "document_type"]),
  ).toUpperCase();
  const clearingDocument = normaliseText(
    firstValue(row, ["clearingDocument", "clearing_document"]),
  );
  const clearingPrefix = clearingDocumentPrefix(row);
  if (firstValue(row, ["semanticKind", "semantic_kind"]) === "direct_payment") {
    return "DIRECT_PAYMENT";
  }
  if (documentType === DOCUMENT_TYPES.INVOICE) return "INVOICE";
  if (documentType === DOCUMENT_TYPES.PAYMENT) return "PAYMENT";
  if (documentType === DOCUMENT_TYPES.EARLY_TRADE_DISCOUNT) return "ET";
  if (
    documentType === DOCUMENT_TYPES.INVOICE_KR &&
    ["4", "5"].includes(clearingPrefix)
  ) {
    return "INVOICE";
  }
  if (
    documentType === DOCUMENT_TYPES.VENDOR_CREDIT_MEMO &&
    clearingPrefix === "5"
  ) {
    return "KG";
  }
  if (
    documentType === DOCUMENT_TYPES.CLEARING_PAYMENT &&
    clearingPrefix === "4" &&
    (groupDocumentTypes?.has(DOCUMENT_TYPES.INVOICE) ||
      groupDocumentTypes?.has(DOCUMENT_TYPES.INVOICE_KR))
  ) {
    return "PAYMENT";
  }
  if (clearingDocument.startsWith("200")) return "CREDIT";
  if (clearingDocument.startsWith("300")) return "REFUND";
  return "UNRECOGNISED";
}

function compareRows(left, right, dateKeys) {
  const leftDate = normaliseText(firstValue(left, dateKeys));
  const rightDate = normaliseText(firstValue(right, dateKeys));
  const dateCompare = leftDate.localeCompare(rightDate);
  if (dateCompare !== 0) return dateCompare;
  const rowCompare =
    Number(left?.rowNo || left?.row_no || 0) -
    Number(right?.rowNo || right?.row_no || 0);
  if (rowCompare !== 0) return rowCompare;
  return normaliseText(left?.id || left?.stageRowId).localeCompare(
    normaliseText(right?.id || right?.stageRowId),
  );
}

function paymentTimeResult({
  paymentDate,
  invoiceIssueDate,
  invoiceReceiptDate,
  rcti,
}) {
  const parseDateOnly = (value) => {
    const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(value || ""));
    if (!match) return NaN;
    const instant = Date.UTC(
      Number(match[1]),
      Number(match[2]) - 1,
      Number(match[3]),
    );
    const parsed = new Date(instant);
    return parsed.getUTCFullYear() === Number(match[1]) &&
      parsed.getUTCMonth() === Number(match[2]) - 1 &&
      parsed.getUTCDate() === Number(match[3])
      ? instant
      : NaN;
  };
  const payment = parseDateOnly(paymentDate);
  const issue = parseDateOnly(invoiceIssueDate);
  const receipt = parseDateOnly(invoiceReceiptDate);
  if (!Number.isFinite(payment)) {
    return { days: null, referenceDate: null, referenceKind: null };
  }
  const dayMs = 24 * 60 * 60 * 1000;
  const inclusiveDays = (start) => {
    const difference = Math.round((payment - start) / dayMs);
    return difference <= 0 ? 0 : difference + 1;
  };
  const candidates = [
    Number.isFinite(issue)
      ? {
          days: inclusiveDays(issue),
          referenceDate: invoiceIssueDate,
          referenceKind: "invoice_issue",
        }
      : null,
    Number.isFinite(receipt)
      ? {
          days: inclusiveDays(receipt),
          referenceDate: invoiceReceiptDate,
          referenceKind: "invoice_receipt",
        }
      : null,
  ].filter(Boolean);
  if (candidates.length === 0) {
    return { days: null, referenceDate: null, referenceKind: null };
  }
  const isRcti = ["yes", "y", "true", "1"].includes(
    normaliseText(rcti).toLowerCase(),
  );
  const receiptCandidate = candidates.find(
    (candidate) => candidate.referenceKind === "invoice_receipt",
  );
  if (!isRcti && receiptCandidate) {
    return {
      ...receiptCandidate,
      referencePolicy: VEOLIA_PAYMENT_TIME_REFERENCE_POLICY.id,
      referenceReason: VEOLIA_PAYMENT_TIME_REFERENCE_POLICY.reason,
    };
  }
  return candidates.reduce((selected, candidate) =>
    candidate.days < selected.days ? candidate : selected,
  );
}

function normalisePaymentRows(inputRows) {
  const rows = (Array.isArray(inputRows) ? inputRows : []).map((row) => ({
    ...row,
    id: normaliseText(row?.id || row?.stageRowId),
    groupKey: sourceGroupKey(row),
    clearingPrefix: clearingDocumentPrefix(row),
    sourceDocumentType: normaliseText(
      firstValue(row, ["documentType", "document_type"]),
    ).toUpperCase(),
    signedAmount: signedAmount(
      firstValue(row, ["paymentAmount", "payment_amount", "amount"]),
    ),
    amount: normaliseAmount(
      firstValue(row, ["paymentAmount", "payment_amount", "amount"]),
    ),
    descriptionReference: normaliseText(
      firstValue(row, [
        "descriptionReference",
        "description_reference",
        "description",
      ]),
    ),
  }));

  const documentTypesByGroup = new Map();
  const clearingContextByGroup = new Map();
  for (const row of rows) {
    if (!row.groupKey) continue;
    const documentTypes = documentTypesByGroup.get(row.groupKey) || new Set();
    documentTypes.add(row.sourceDocumentType);
    documentTypesByGroup.set(row.groupKey, documentTypes);
    const context = clearingContextByGroup.get(row.groupKey) || {
      clearingPrefix: row.clearingPrefix,
      signedBalance: 0,
    };
    context.signedBalance += row.signedAmount;
    clearingContextByGroup.set(row.groupKey, context);
  }
  for (const row of rows) {
    row.role = classifyDocument(
      row,
      row.groupKey ? documentTypesByGroup.get(row.groupKey) : null,
    );
    row.usesSignedObligationDirection =
      row.role === "INVOICE" &&
      row.clearingPrefix === "4" &&
      [DOCUMENT_TYPES.INVOICE, DOCUMENT_TYPES.INVOICE_KR].includes(
        row.sourceDocumentType,
      );
  }

  const obligations = rows
    .filter((row) => row.role === "INVOICE")
    .map((row) => ({
      ...row,
      originalAmount: row.amount,
      adjustedAmount:
        row.usesSignedObligationDirection &&
        row.signedAmount > NORMALISATION_EPSILON
          ? 0
          : row.amount,
      outstandingAmount:
        row.usesSignedObligationDirection &&
        row.signedAmount > NORMALISATION_EPSILON
          ? 0
          : row.amount,
      directionalOffsetAllocatedAmount: 0,
      directionalOffsetUnmatchedAmount: 0,
      directionalOffsetAllocations: [],
      adjustmentAllocations: [],
      paymentAllocations: [],
    }));
  const adjustments = rows
    .filter((row) => ADJUSTMENT_REASONS[row.role])
    .map((row) => ({
      ...row,
      effectiveAmount: row.amount,
      allocatedAmount: 0,
      reversalOffsetAmount: 0,
      unmatchedAmount: row.amount,
      allocations: [],
    }));
  const payments = rows
    .filter((row) => row.role === "PAYMENT")
    .map((row) => ({
      ...row,
      allocatedAmount: 0,
      unmatchedAmount: row.amount,
      allocations: [],
    }));
  const exceptions = rows
    .filter((row) => row.role === "UNRECOGNISED")
    .map((row) => ({
      code: "UNRECOGNISED_DOCUMENT_TYPE",
      sourceStageRowId: row.id,
      documentType: normaliseText(
        firstValue(row, ["documentType", "document_type"]),
      ),
      amount: row.amount,
    }));
  exceptions.push(
    ...rows
      .filter((row) => row.role !== "UNRECOGNISED" && !row.groupKey)
      .map((row) => ({
        code: "MAPPING_EXCEPTION",
        sourceStageRowId: row.id,
        field: "normalisation_group_key",
        amount: row.amount,
      })),
  );

  const kgAdjustmentsByGroup = new Map();
  for (const adjustment of adjustments) {
    if (adjustment.role !== "KG" || !adjustment.groupKey) continue;
    const list = kgAdjustmentsByGroup.get(adjustment.groupKey) || [];
    list.push(adjustment);
    kgAdjustmentsByGroup.set(adjustment.groupKey, list);
  }
  for (const [groupKey, kgAdjustments] of kgAdjustmentsByGroup.entries()) {
    kgAdjustments.sort((left, right) =>
      compareRows(left, right, ["paymentDate", "payment_date"]),
    );
    const netCreditAmount = kgAdjustments.reduce(
      (total, adjustment) => total + adjustment.signedAmount,
      0,
    );
    let remainingNetCredit = Math.max(0, netCreditAmount);
    for (const adjustment of kgAdjustments) {
      adjustment.effectiveAmount =
        adjustment.signedAmount > NORMALISATION_EPSILON
          ? Math.min(adjustment.amount, remainingNetCredit)
          : 0;
      adjustment.reversalOffsetAmount =
        adjustment.signedAmount > NORMALISATION_EPSILON
          ? Math.max(0, adjustment.amount - adjustment.effectiveAmount)
          : 0;
      if (adjustment.reversalOffsetAmount > NORMALISATION_EPSILON) {
        adjustment.reconciliationCode = "KG_REVERSAL_OFFSET";
      }
      adjustment.unmatchedAmount = adjustment.effectiveAmount;
      remainingNetCredit -= adjustment.effectiveAmount;
    }
    if (netCreditAmount < -NORMALISATION_EPSILON) {
      const source = kgAdjustments.find(
        (adjustment) => adjustment.signedAmount < -NORMALISATION_EPSILON,
      );
      exceptions.push({
        code: "UNMATCHED_KG_REVERSAL",
        sourceStageRowId: source?.id || null,
        adjustmentKind: "KG",
        unmatchedAmount: Math.abs(netCreditAmount),
        groupKey,
      });
    }
  }

  const obligationsByGroup = new Map();
  for (const obligation of obligations) {
    if (!obligation.groupKey) continue;
    const list = obligationsByGroup.get(obligation.groupKey) || [];
    list.push(obligation);
    obligationsByGroup.set(obligation.groupKey, list);
  }
  for (const list of obligationsByGroup.values()) {
    list.sort((left, right) =>
      compareRows(left, right, ["invoiceIssueDate", "invoice_issue_date"]),
    );
  }

  for (const obligationsInGroup of obligationsByGroup.values()) {
    const positiveObligations = obligationsInGroup.filter(
      (obligation) =>
        !obligation.usesSignedObligationDirection ||
        obligation.signedAmount < -NORMALISATION_EPSILON,
    );
    const directionalOffsets = obligationsInGroup.filter(
      (obligation) =>
        obligation.usesSignedObligationDirection &&
        obligation.signedAmount > NORMALISATION_EPSILON,
    );
    for (const offset of directionalOffsets) {
      let remaining = offset.amount;
      for (const obligation of positiveObligations) {
        if (remaining <= NORMALISATION_EPSILON) break;
        if (obligation.adjustedAmount <= NORMALISATION_EPSILON) continue;
        const before = obligation.adjustedAmount;
        const allocated = Math.min(before, remaining);
        const after = Math.max(0, before - allocated);
        const allocation = {
          offsetStageRowId: offset.id,
          invoiceStageRowId: obligation.id,
          amount: allocated,
          obligationBefore: before,
          obligationAfter: after,
        };
        obligation.adjustedAmount = after;
        obligation.outstandingAmount = after;
        obligation.directionalOffsetAllocatedAmount += allocated;
        obligation.directionalOffsetAllocations.push(allocation);
        offset.directionalOffsetAllocatedAmount += allocated;
        offset.directionalOffsetAllocations.push(allocation);
        remaining -= allocated;
      }
      offset.directionalOffsetUnmatchedAmount = Math.max(0, remaining);
      if (offset.directionalOffsetUnmatchedAmount > NORMALISATION_EPSILON) {
        exceptions.push({
          code: "UNMATCHED_OBLIGATION_OFFSET",
          sourceStageRowId: offset.id,
          originalAmount: offset.amount,
          allocatedAmount: offset.directionalOffsetAllocatedAmount,
          unmatchedAmount: offset.directionalOffsetUnmatchedAmount,
        });
      }
    }
  }

  const adjustmentKindsByGroup = new Map();
  for (const adjustment of adjustments) {
    const kinds = adjustmentKindsByGroup.get(adjustment.groupKey) || new Set();
    kinds.add(adjustment.role === "ET" ? "ET" : "FINANCIAL");
    adjustmentKindsByGroup.set(adjustment.groupKey, kinds);
  }
  const ambiguousAdjustmentGroups = new Set(
    Array.from(adjustmentKindsByGroup.entries())
      .filter(([, kinds]) => kinds.size > 1)
      .map(([groupKey]) => groupKey),
  );
  const financialAdjustmentsByGroup = new Map();
  for (const adjustment of adjustments) {
    if (!adjustment.groupKey || adjustment.role === "ET") continue;
    const list = financialAdjustmentsByGroup.get(adjustment.groupKey) || [];
    list.push(adjustment);
    financialAdjustmentsByGroup.set(adjustment.groupKey, list);
  }
  const netZeroAdjustmentReversalGroups = new Set(
    Array.from(financialAdjustmentsByGroup.entries())
      .filter(([groupKey, group]) => {
        const signedTotal = group.reduce(
          (total, adjustment) => total + adjustment.signedAmount,
          0,
        );
        return (
          (obligationsByGroup.get(groupKey) || []).length === 0 &&
          group.some(
            (adjustment) => adjustment.signedAmount > NORMALISATION_EPSILON,
          ) &&
          group.some(
            (adjustment) => adjustment.signedAmount < -NORMALISATION_EPSILON,
          ) &&
          Math.abs(signedTotal) <= NORMALISATION_EPSILON
        );
      })
      .map(([groupKey]) => groupKey),
  );

  adjustments.sort((left, right) =>
    compareRows(left, right, [
      "paymentDate",
      "payment_date",
      "invoiceIssueDate",
      "invoice_issue_date",
    ]),
  );
  for (const adjustment of adjustments) {
    if (netZeroAdjustmentReversalGroups.has(adjustment.groupKey)) {
      adjustment.reversalOffsetAmount = adjustment.amount;
      adjustment.unmatchedAmount = 0;
      adjustment.reconciliationCode = "NET_ZERO_ADJUSTMENT_REVERSAL";
      continue;
    }
    let remaining = adjustment.effectiveAmount;
    const candidates = ambiguousAdjustmentGroups.has(adjustment.groupKey)
      ? []
      : (obligationsByGroup.get(adjustment.groupKey) || []).filter(
          (obligation) =>
            adjustment.role !== "ET" ||
            (adjustment.descriptionReference &&
              obligation.descriptionReference ===
                adjustment.descriptionReference),
        );
    for (const obligation of candidates) {
      if (remaining <= NORMALISATION_EPSILON) break;
      if (obligation.adjustedAmount <= NORMALISATION_EPSILON) continue;
      const before = obligation.adjustedAmount;
      const allocated = Math.min(before, remaining);
      const after = Math.max(0, before - allocated);
      const reason =
        after <= NORMALISATION_EPSILON
          ? ADJUSTMENT_REASONS[adjustment.role].full
          : ADJUSTMENT_REASONS[adjustment.role].partial;
      const allocation = {
        adjustmentStageRowId: adjustment.id,
        invoiceStageRowId: obligation.id,
        adjustmentKind: adjustment.role,
        reasonCode: reason,
        amount: allocated,
        obligationBefore: before,
        obligationAfter: after,
      };
      obligation.adjustedAmount = after;
      obligation.outstandingAmount = after;
      obligation.adjustmentAllocations.push(allocation);
      adjustment.allocations.push(allocation);
      adjustment.allocatedAmount += allocated;
      remaining -= allocated;
    }
    adjustment.unmatchedAmount = Math.max(0, remaining);
    if (adjustment.unmatchedAmount > NORMALISATION_EPSILON) {
      exceptions.push({
        code: ambiguousAdjustmentGroups.has(adjustment.groupKey)
          ? "AMBIGUOUS_ADJUSTMENT_COMBINATION"
          : "UNMATCHED_ADJUSTMENT",
        sourceStageRowId: adjustment.id,
        adjustmentKind: adjustment.role,
        originalAmount: adjustment.amount,
        effectiveAmount: adjustment.effectiveAmount,
        allocatedAmount: adjustment.allocatedAmount,
        unmatchedAmount: adjustment.unmatchedAmount,
        invoiceStageRowIds: adjustment.allocations.map(
          (allocation) => allocation.invoiceStageRowId,
        ),
      });
    }
  }

  payments.sort((left, right) =>
    compareRows(left, right, ["paymentDate", "payment_date"]),
  );
  const observations = [];
  for (const payment of payments) {
    let remaining = payment.amount;
    const candidates = obligationsByGroup.get(payment.groupKey) || [];
    for (const obligation of candidates) {
      if (remaining <= NORMALISATION_EPSILON) break;
      if (obligation.outstandingAmount <= NORMALISATION_EPSILON) continue;
      const before = obligation.outstandingAmount;
      const allocated = Math.min(before, remaining);
      const after = Math.max(0, before - allocated);
      const partial = after > NORMALISATION_EPSILON;
      const paymentDate = normaliseText(
        firstValue(payment, ["paymentDate", "payment_date"]),
      );
      const invoiceIssueDate = normaliseText(
        firstValue(obligation, ["invoiceIssueDate", "invoice_issue_date"]),
      );
      const invoiceReceiptDate = normaliseText(
        firstValue(obligation, ["invoiceReceiptDate", "invoice_receipt_date"]),
      );
      const paymentTime = paymentTimeResult({
        paymentDate,
        invoiceIssueDate,
        invoiceReceiptDate,
        rcti: firstValue(obligation, ["rcti", "RCTI"]),
      });
      const allocation = {
        invoiceStageRowId: obligation.id,
        paymentStageRowId: payment.id,
        amount: allocated,
        obligationBefore: before,
        obligationAfter: after,
        partialPayment: partial,
        finalSettlement: !partial,
      };
      obligation.outstandingAmount = after;
      obligation.paymentAllocations.push(allocation);
      payment.allocations.push(allocation);
      payment.allocatedAmount += allocated;
      remaining -= allocated;
      const observation = {
        ...allocation,
        reasonCode: partial ? "PARTIAL_PAYMENT" : null,
        paymentDate,
        invoiceIssueDate,
        invoiceReceiptDate,
        paymentTimeDays: paymentTime.days,
        paymentTimeReferenceDate: paymentTime.referenceDate,
        paymentTimeReferenceKind: paymentTime.referenceKind,
        paymentTimeReferencePolicy: paymentTime.referencePolicy || null,
        paymentTimeReferenceReason: paymentTime.referenceReason || null,
        originalObligationAmount: obligation.originalAmount,
        adjustedObligationAmount: obligation.adjustedAmount,
        classificationBasis:
          payment.sourceDocumentType === DOCUMENT_TYPES.CLEARING_PAYMENT
            ? "outstanding_obligation_after_clearing_settlement"
            : "outstanding_obligation_after_zp",
        contractualInstalmentIndicatorAvailable: false,
      };
      if (paymentTime.days == null) {
        observation.exceptionCode = "MAPPING_EXCEPTION";
      }
      observations.push(observation);
    }
    payment.unmatchedAmount = Math.max(0, remaining);
    const clearingContext = clearingContextByGroup.get(payment.groupKey);
    if (
      payment.unmatchedAmount > NORMALISATION_EPSILON &&
      clearingContext?.clearingPrefix === "5" &&
      Math.abs(clearingContext.signedBalance) <= NORMALISATION_EPSILON
    ) {
      payment.clearingBalanceOffsetAmount = payment.unmatchedAmount;
      payment.clearingGroupSignedBalance = clearingContext.signedBalance;
      payment.unmatchedAmount = 0;
    }
    if (payment.unmatchedAmount > NORMALISATION_EPSILON) {
      exceptions.push({
        code: "UNMATCHED_PAYMENT",
        sourceStageRowId: payment.id,
        originalAmount: payment.amount,
        allocatedAmount: payment.allocatedAmount,
        unmatchedAmount: payment.unmatchedAmount,
      });
    }
  }

  const sum = (values) => values.reduce((total, value) => total + value, 0);
  const sourceRowsById = new Map(rows.map((row) => [row.id, row]));
  const sourceRowsByGroup = new Map();
  for (const row of rows) {
    if (!row.groupKey) continue;
    const sourceRows = sourceRowsByGroup.get(row.groupKey) || [];
    sourceRows.push(row);
    sourceRowsByGroup.set(row.groupKey, sourceRows);
  }
  const reconciliableCodes = new Set(CLEARING_RECONCILIABLE_EXCEPTION_CODES);
  const reconciliableExceptionsByGroup = new Map();
  for (const exception of exceptions) {
    if (!reconciliableCodes.has(exception.code)) continue;
    const source = sourceRowsById.get(exception.sourceStageRowId);
    if (!source?.groupKey) continue;
    const groupExceptions =
      reconciliableExceptionsByGroup.get(source.groupKey) || [];
    groupExceptions.push(exception);
    reconciliableExceptionsByGroup.set(source.groupKey, groupExceptions);
  }

  const clearingReconciliations = [];
  const balancedReconciledGroups = new Set();
  for (const [groupKey, groupExceptions] of
    reconciliableExceptionsByGroup.entries()) {
    const sourceRows = sourceRowsByGroup.get(groupKey) || [];
    const clearingContext = clearingContextByGroup.get(groupKey);
    const signedClearingGroupTotal = clearingContext?.signedBalance || 0;
    const acceptedAsBalancedClearing =
      Math.abs(signedClearingGroupTotal) <= NORMALISATION_EPSILON;
    const adjustmentsInGroup = adjustments.filter(
      (adjustment) => adjustment.groupKey === groupKey,
    );
    const obligationsInGroup = obligations.filter(
      (obligation) => obligation.groupKey === groupKey,
    );
    const paymentsInGroup = payments.filter(
      (payment) => payment.groupKey === groupKey,
    );
    const residualExceptionAmountBeforeReconciliation = sum(
      groupExceptions.map((exception) =>
        Number(exception.unmatchedAmount ?? exception.amount ?? 0),
      ),
    );
    const reconciliation = {
      normalisationGroupKey: groupKey,
      reconciliationCode: acceptedAsBalancedClearing
        ? BALANCED_CLEARING_RECONCILIATION
        : null,
      acceptedAsBalancedClearing,
      signedClearingGroupTotal,
      remainingSignedResidualBeforeFinalReconciliation:
        signedClearingGroupTotal,
      finalUnexplainedSignedResidual: acceptedAsBalancedClearing
        ? 0
        : signedClearingGroupTotal,
      residualExceptionAmountBeforeReconciliation,
      residualExceptionCodesBeforeReconciliation: Array.from(
        new Set(groupExceptions.map((exception) => exception.code)),
      ).sort(),
      participatingDocumentTypes: Array.from(
        new Set(sourceRows.map((row) => row.sourceDocumentType)),
      ).sort(),
      sourceRows: sourceRows.map((row) => ({
        stageRowId: row.id,
        rowNo: row.rowNo ?? row.row_no ?? null,
        documentType: row.sourceDocumentType,
        normalisationRole: row.role,
        signedAmount: row.signedAmount,
      })),
      semanticEffects: {
        directionalObligationOffsetAmount: sum(
          obligationsInGroup.map(
            (obligation) => obligation.directionalOffsetAllocatedAmount,
          ),
        ),
        adjustmentAllocatedAmount: sum(
          adjustmentsInGroup.map((adjustment) => adjustment.allocatedAmount),
        ),
        adjustmentReversalOffsetAmount: sum(
          adjustmentsInGroup.map(
            (adjustment) => adjustment.reversalOffsetAmount,
          ),
        ),
        paymentAllocatedAmount: sum(
          paymentsInGroup.map((payment) => payment.allocatedAmount),
        ),
        paymentClearingBalanceOffsetAmount: sum(
          paymentsInGroup.map(
            (payment) => payment.clearingBalanceOffsetAmount || 0,
          ),
        ),
      },
    };
    clearingReconciliations.push(reconciliation);
    if (!acceptedAsBalancedClearing) continue;
    balancedReconciledGroups.add(groupKey);
    for (const obligation of obligationsInGroup) {
      if (
        obligation.directionalOffsetUnmatchedAmount > NORMALISATION_EPSILON
      ) {
        obligation.directionalOffsetUnmatchedAmountBeforeClearingReconciliation =
          obligation.directionalOffsetUnmatchedAmount;
        obligation.clearingReconciliationOffsetAmount =
          obligation.directionalOffsetUnmatchedAmount;
        obligation.directionalOffsetUnmatchedAmount = 0;
      }
    }
    for (const adjustment of adjustmentsInGroup) {
      if (adjustment.unmatchedAmount > NORMALISATION_EPSILON) {
        adjustment.unmatchedAmountBeforeClearingReconciliation =
          adjustment.unmatchedAmount;
        adjustment.clearingReconciliationOffsetAmount =
          adjustment.unmatchedAmount;
        adjustment.unmatchedAmount = 0;
        adjustment.reconciliationCode = BALANCED_CLEARING_RECONCILIATION;
      }
    }
    for (const payment of paymentsInGroup) {
      if (payment.unmatchedAmount > NORMALISATION_EPSILON) {
        payment.unmatchedAmountBeforeClearingReconciliation =
          payment.unmatchedAmount;
        payment.clearingReconciliationOffsetAmount = payment.unmatchedAmount;
        payment.unmatchedAmount = 0;
      }
    }
  }
  const finalExceptions = exceptions.filter((exception) => {
    if (!reconciliableCodes.has(exception.code)) return true;
    const source = sourceRowsById.get(exception.sourceStageRowId);
    return !source?.groupKey || !balancedReconciledGroups.has(source.groupKey);
  });

  return {
    obligations,
    adjustments,
    payments,
    observations,
    exceptions: finalExceptions,
    clearingReconciliations,
    reconciliation: {
      startingRowCount: rows.length,
      startingValue: sum(rows.map((row) => row.amount)),
      originalObligationValue: sum(
        obligations.map((row) => row.originalAmount),
      ),
      adjustedObligationValue: sum(
        obligations.map((row) => row.adjustedAmount),
      ),
      directionalObligationOffsetValue: sum(
        obligations
          .filter(
            (row) =>
              row.usesSignedObligationDirection &&
              row.signedAmount > NORMALISATION_EPSILON,
          )
          .map((row) => row.directionalOffsetAllocatedAmount),
      ),
      unmatchedDirectionalObligationOffsetValue: sum(
        obligations.map((row) => row.directionalOffsetUnmatchedAmount),
      ),
      adjustmentValue: sum(adjustments.map((row) => row.amount)),
      adjustmentAllocatedValue: sum(
        adjustments.map((row) => row.allocatedAmount),
      ),
      adjustmentReversalOffsetValue: sum(
        adjustments.map((row) => row.reversalOffsetAmount),
      ),
      unmatchedAdjustmentValue: sum(
        adjustments.map((row) => row.unmatchedAmount),
      ),
      paymentValue: sum(payments.map((row) => row.amount)),
      paymentAllocatedValue: sum(payments.map((row) => row.allocatedAmount)),
      unmatchedPaymentValue: sum(payments.map((row) => row.unmatchedAmount)),
      balancedClearingPaymentOffsetValue: sum(
        payments.map((row) => row.clearingBalanceOffsetAmount || 0),
      ),
      balancedClearingReconciliationCount: clearingReconciliations.filter(
        (row) => row.acceptedAsBalancedClearing,
      ).length,
      unexplainedClearingResidualValue: sum(
        clearingReconciliations.map((row) =>
          Math.abs(row.finalUnexplainedSignedResidual),
        ),
      ),
      paymentObservationCount: observations.length,
      partialPaymentCount: observations.filter((row) => row.partialPayment)
        .length,
      finalPaymentCount: observations.filter((row) => row.finalSettlement)
        .length,
      exceptionCount: finalExceptions.length,
      contractualInstalmentIndicatorAvailable: false,
    },
  };
}

function buildPaymentNormalisationCte() {
  return `
    payment_normalisation_classified_source_rows AS MATERIALIZED (
      SELECT
        s."id",
        s."datasetId",
        s."semanticKind",
        s."sourceGroupScope",
        s."rowNo",
        s."sourceAccountCode",
        s."description",
        s."documentType",
        s."clearingDocument",
        s."paymentAmount",
        s."paymentDate",
        s."invoiceIssueDate",
        s."invoiceReceiptDate",
        UPPER(BTRIM(COALESCE(s."documentType", ''))) AS document_type,
        NULLIF(BTRIM(s."data"->>'company_code'), '') AS company_code,
        NULLIF(BTRIM(s."sourceAccountCode"), '') AS source_account_code,
        NULLIF(BTRIM(s."clearingDocument"), '') AS clearing_document,
        NULLIF(BTRIM(s."description"), '') AS description_reference,
        CASE
          WHEN NULLIF(BTRIM(s."data"->>'company_code'), '') IS NULL
            OR NULLIF(BTRIM(s."sourceAccountCode"), '') IS NULL
            OR NULLIF(BTRIM(s."clearingDocument"), '') IS NULL
            THEN NULL
          ELSE COALESCE(
            NULLIF(BTRIM(s."sourceGroupScope"), ''),
            'dataset:' || s."datasetId"
          ) || '|' || BTRIM(s."data"->>'company_code')
            || '|' || BTRIM(s."sourceAccountCode")
            || '|' || BTRIM(s."clearingDocument")
        END AS normalisation_group_key,
        COALESCE(s."paymentAmount", 0)::numeric AS signed_amount,
        ABS(COALESCE(s."paymentAmount", 0))::numeric AS normalisation_amount,
        COALESCE((s."data"->>'exclude_from_metrics')::boolean, false)
          OR COALESCE((s."meta"->'rules'->>'exclude')::boolean, false) AS excluded
      FROM "tbl_ptrs_stage_row" s
      WHERE s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
    ),
    payment_normalisation_clearing_group_context AS MATERIALIZED (
      SELECT normalisation_group_key,
        BOOL_OR(document_type IN ('RE', 'KR'))
          AS has_recognised_obligation,
        BOOL_AND(clearing_document LIKE '5%') AS is_prefix_5,
        SUM(signed_amount)::numeric AS signed_balance
      FROM payment_normalisation_classified_source_rows
      WHERE normalisation_group_key IS NOT NULL
      GROUP BY normalisation_group_key
    ),
    payment_normalisation_source_rows AS MATERIALIZED (
      SELECT source.*,
        CASE
          WHEN source."semanticKind" = 'direct_payment' THEN 'DIRECT_PAYMENT'
          WHEN source.document_type = 'RE' THEN 'INVOICE'
          WHEN source.document_type = 'ZP' THEN 'PAYMENT'
          WHEN source.document_type = 'ET' THEN 'ET'
          WHEN source.document_type = 'KR'
            AND (source.clearing_document LIKE '4%'
              OR source.clearing_document LIKE '5%')
            THEN 'INVOICE'
          WHEN source.document_type = 'KG'
            AND source.clearing_document LIKE '5%'
            THEN 'KG'
          WHEN source.document_type = 'KZ'
            AND source.clearing_document LIKE '4%'
            AND context.has_recognised_obligation
            THEN 'PAYMENT'
          WHEN source.clearing_document LIKE '200%' THEN 'CREDIT'
          WHEN source.clearing_document LIKE '300%' THEN 'REFUND'
          ELSE 'UNRECOGNISED'
        END AS normalisation_role
      FROM payment_normalisation_classified_source_rows source
      LEFT JOIN payment_normalisation_clearing_group_context context
        ON context.normalisation_group_key = source.normalisation_group_key
    ),
    payment_normalisation_obligations AS MATERIALIZED (
      SELECT
        source.*,
        ROW_NUMBER() OVER (
          PARTITION BY normalisation_group_key
          ORDER BY "invoiceIssueDate" NULLS LAST, "rowNo", "id"
        ) AS obligation_sequence
      FROM payment_normalisation_source_rows source
      WHERE normalisation_role = 'INVOICE'
    ),
    payment_normalisation_positive_obligation_ranges AS MATERIALIZED (
      SELECT obligation.*,
        COALESCE(SUM(normalisation_amount) OVER (
          PARTITION BY normalisation_group_key
          ORDER BY obligation_sequence, "id"
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS obligation_start,
        SUM(normalisation_amount) OVER (
          PARTITION BY normalisation_group_key
          ORDER BY obligation_sequence, "id"
          ROWS UNBOUNDED PRECEDING
        ) AS obligation_end
      FROM payment_normalisation_obligations obligation
      WHERE clearing_document LIKE '4%'
        AND document_type IN ('RE', 'KR')
        AND signed_amount < -0.005
    ),
    payment_normalisation_directional_offset_ranges AS MATERIALIZED (
      SELECT obligation.*,
        COALESCE(SUM(normalisation_amount) OVER (
          PARTITION BY normalisation_group_key
          ORDER BY obligation_sequence, "id"
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS offset_start,
        SUM(normalisation_amount) OVER (
          PARTITION BY normalisation_group_key
          ORDER BY obligation_sequence, "id"
          ROWS UNBOUNDED PRECEDING
        ) AS offset_end
      FROM payment_normalisation_obligations obligation
      WHERE clearing_document LIKE '4%'
        AND document_type IN ('RE', 'KR')
        AND signed_amount > 0.005
    ),
    payment_normalisation_directional_offset_allocations AS MATERIALIZED (
      SELECT
        directional_offset."id" AS offset_stage_row_id,
        obligation."id" AS invoice_stage_row_id,
        GREATEST(0, LEAST(obligation.obligation_end, directional_offset.offset_end)
          - GREATEST(obligation.obligation_start, directional_offset.offset_start))::numeric
          AS allocated_amount
      FROM payment_normalisation_directional_offset_ranges directional_offset
      JOIN payment_normalisation_positive_obligation_ranges obligation
        ON obligation.normalisation_group_key =
            directional_offset.normalisation_group_key
       AND obligation.obligation_end > directional_offset.offset_start
       AND obligation.obligation_start < directional_offset.offset_end
    ),
    payment_normalisation_directional_offset_totals AS MATERIALIZED (
      SELECT invoice_stage_row_id,
        SUM(allocated_amount)::numeric AS allocated_amount
      FROM payment_normalisation_directional_offset_allocations
      WHERE allocated_amount > 0.005
      GROUP BY invoice_stage_row_id
    ),
    payment_normalisation_directional_offset_source_totals AS MATERIALIZED (
      SELECT directional_offset."id" AS offset_stage_row_id,
        directional_offset.normalisation_amount AS original_amount,
        COALESCE(SUM(allocation.allocated_amount), 0)::numeric
          AS allocated_amount,
        GREATEST(0, directional_offset.normalisation_amount
          - COALESCE(SUM(allocation.allocated_amount), 0))::numeric
          AS unmatched_amount
      FROM payment_normalisation_directional_offset_ranges directional_offset
      LEFT JOIN payment_normalisation_directional_offset_allocations allocation
        ON allocation.offset_stage_row_id = directional_offset."id"
       AND allocation.allocated_amount > 0.005
      GROUP BY directional_offset."id",
        directional_offset.normalisation_amount
    ),
    payment_normalisation_directionally_adjusted_obligations AS MATERIALIZED (
      SELECT obligation.*,
        obligation.normalisation_amount AS original_obligation_amount,
        CASE
          WHEN obligation.clearing_document LIKE '4%'
            AND obligation.document_type IN ('RE', 'KR')
            AND obligation.signed_amount > 0.005
            THEN 0
          ELSE GREATEST(0, obligation.normalisation_amount
            - COALESCE(directional_offset.allocated_amount, 0))
        END::numeric AS obligation_amount_after_direction,
        COALESCE(directional_offset.allocated_amount, 0)::numeric
          AS directional_offset_allocated_amount
      FROM payment_normalisation_obligations obligation
      LEFT JOIN payment_normalisation_directional_offset_totals
          directional_offset
        ON directional_offset.invoice_stage_row_id = obligation."id"
    ),
    payment_normalisation_kg_group_totals AS MATERIALIZED (
      SELECT normalisation_group_key,
        SUM(signed_amount)::numeric AS signed_amount,
        GREATEST(0, SUM(signed_amount))::numeric AS effective_credit_amount
      FROM payment_normalisation_source_rows
      WHERE normalisation_role = 'KG'
        AND normalisation_group_key IS NOT NULL
      GROUP BY normalisation_group_key
    ),
    payment_normalisation_unmatched_kg_reversals AS MATERIALIZED (
      SELECT MIN(source."id") AS source_stage_row_id,
        'UNMATCHED_KG_REVERSAL'::text AS reason_code,
        ABS(group_total.signed_amount)::numeric AS amount
      FROM payment_normalisation_kg_group_totals group_total
      JOIN payment_normalisation_source_rows source
        ON source.normalisation_group_key = group_total.normalisation_group_key
       AND source.normalisation_role = 'KG'
       AND source.signed_amount < -0.005
      WHERE group_total.signed_amount < -0.005
      GROUP BY group_total.normalisation_group_key, group_total.signed_amount
    ),
    payment_normalisation_kg_positive_ranges AS MATERIALIZED (
      SELECT source."id",
        source.normalisation_group_key,
        COALESCE(SUM(source.normalisation_amount) OVER (
          PARTITION BY source.normalisation_group_key
          ORDER BY source."paymentDate" NULLS LAST, source."rowNo", source."id"
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS credit_start,
        SUM(source.normalisation_amount) OVER (
          PARTITION BY source.normalisation_group_key
          ORDER BY source."paymentDate" NULLS LAST, source."rowNo", source."id"
          ROWS UNBOUNDED PRECEDING
        ) AS credit_end
      FROM payment_normalisation_source_rows source
      WHERE source.normalisation_role = 'KG'
        AND source.signed_amount > 0.005
    ),
    payment_normalisation_adjustment_sources AS MATERIALIZED (
      SELECT source.*,
        CASE
          WHEN source.normalisation_role = 'KG' THEN GREATEST(
            0,
            LEAST(kg_range.credit_end, kg_group.effective_credit_amount)
              - kg_range.credit_start
          )
          ELSE source.normalisation_amount
        END::numeric AS allocation_amount,
        CASE
          WHEN source.normalisation_role = 'KG'
            AND source.signed_amount > 0.005
            THEN GREATEST(0, source.normalisation_amount - GREATEST(
              0,
              LEAST(kg_range.credit_end, kg_group.effective_credit_amount)
                - kg_range.credit_start
            ))
          ELSE 0
        END::numeric AS kg_reversal_offset_amount
      FROM payment_normalisation_source_rows source
      LEFT JOIN payment_normalisation_kg_group_totals kg_group
        ON kg_group.normalisation_group_key = source.normalisation_group_key
      LEFT JOIN payment_normalisation_kg_positive_ranges kg_range
        ON kg_range."id" = source."id"
    ),
    payment_normalisation_adjustments AS MATERIALIZED (
      SELECT
        source.*,
        CASE
          WHEN normalisation_role = 'ET'
            THEN normalisation_group_key || '|et:' || COALESCE(description_reference, '(missing-reference)')
          ELSE normalisation_group_key || '|financial-adjustment'
        END AS allocation_group_key,
        ROW_NUMBER() OVER (
          PARTITION BY normalisation_group_key,
            CASE WHEN normalisation_role = 'ET' THEN COALESCE(description_reference, '(missing-reference)') ELSE '(financial)' END
          ORDER BY "paymentDate" NULLS LAST, "invoiceIssueDate" NULLS LAST, "rowNo", "id"
        ) AS adjustment_sequence
      FROM payment_normalisation_adjustment_sources source
      WHERE normalisation_role IN ('CREDIT', 'REFUND', 'ET', 'KG')
    ),
    payment_normalisation_net_zero_adjustment_reversal_groups AS MATERIALIZED (
      SELECT normalisation_group_key
      FROM payment_normalisation_adjustments
      WHERE normalisation_group_key IS NOT NULL
      GROUP BY normalisation_group_key
      HAVING BOOL_AND(normalisation_role IN ('CREDIT', 'REFUND'))
        AND BOOL_OR("paymentAmount" > 0.005)
        AND BOOL_OR("paymentAmount" < -0.005)
        AND ABS(SUM(COALESCE("paymentAmount", 0))) <= 0.005
        AND NOT EXISTS (
          SELECT 1
          FROM payment_normalisation_obligations obligation
          WHERE obligation.normalisation_group_key =
            payment_normalisation_adjustments.normalisation_group_key
        )
    ),
    payment_normalisation_ambiguous_adjustment_groups AS MATERIALIZED (
      SELECT normalisation_group_key
      FROM payment_normalisation_adjustments
      GROUP BY normalisation_group_key
      HAVING BOOL_OR(normalisation_role = 'ET')
         AND BOOL_OR(normalisation_role IN ('CREDIT', 'REFUND', 'KG'))
    ),
    payment_normalisation_adjustment_ranges AS MATERIALIZED (
      SELECT
        adjustment.*,
        SUM(allocation_amount) OVER (
          PARTITION BY allocation_group_key
          ORDER BY adjustment_sequence, "id"
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS adjustment_start,
        SUM(allocation_amount) OVER (
          PARTITION BY allocation_group_key
          ORDER BY adjustment_sequence, "id"
          ROWS UNBOUNDED PRECEDING
        ) AS adjustment_end
      FROM payment_normalisation_adjustments adjustment
      WHERE NOT EXISTS (
        SELECT 1 FROM payment_normalisation_ambiguous_adjustment_groups ambiguous
        WHERE ambiguous.normalisation_group_key = adjustment.normalisation_group_key
      )
        AND NOT EXISTS (
          SELECT 1
          FROM payment_normalisation_net_zero_adjustment_reversal_groups reversal
          WHERE reversal.normalisation_group_key = adjustment.normalisation_group_key
        )
        AND allocation_amount > 0.005
    ),
    payment_normalisation_obligation_adjustment_ranges AS (
      SELECT
        adjustment.allocation_group_key,
        obligation."id" AS invoice_stage_row_id,
        obligation.obligation_sequence,
        obligation.obligation_amount_after_direction
          AS adjustment_base_obligation_amount,
        COALESCE(SUM(obligation.obligation_amount_after_direction) OVER (
          PARTITION BY adjustment.allocation_group_key
          ORDER BY obligation.obligation_sequence, obligation."id"
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS obligation_start,
        SUM(obligation.obligation_amount_after_direction) OVER (
          PARTITION BY adjustment.allocation_group_key
          ORDER BY obligation.obligation_sequence, obligation."id"
          ROWS UNBOUNDED PRECEDING
        ) AS obligation_end
      FROM payment_normalisation_directionally_adjusted_obligations obligation
      JOIN (
        SELECT allocation_group_key, normalisation_group_key,
          CASE WHEN BOOL_OR(normalisation_role = 'ET')
            THEN 'ET' ELSE 'FINANCIAL' END AS normalisation_role,
          MAX(description_reference) FILTER (WHERE normalisation_role = 'ET')
            AS description_reference
        FROM payment_normalisation_adjustment_ranges
        GROUP BY allocation_group_key, normalisation_group_key
      ) adjustment
        ON adjustment.normalisation_group_key = obligation.normalisation_group_key
       AND (adjustment.normalisation_role <> 'ET'
            OR adjustment.description_reference = obligation.description_reference)
    ),
    payment_normalisation_adjustment_allocations_raw AS (
      SELECT
        adjustment."id" AS adjustment_stage_row_id,
        obligation.invoice_stage_row_id,
        adjustment.normalisation_role AS adjustment_kind,
        adjustment.adjustment_sequence,
        GREATEST(
          0,
          LEAST(obligation.obligation_end, adjustment.adjustment_end)
            - GREATEST(obligation.obligation_start, COALESCE(adjustment.adjustment_start, 0))
        )::numeric AS allocated_amount,
        obligation.adjustment_base_obligation_amount
      FROM payment_normalisation_adjustment_ranges adjustment
      JOIN payment_normalisation_obligation_adjustment_ranges obligation
        ON obligation.allocation_group_key = adjustment.allocation_group_key
       AND obligation.obligation_end > COALESCE(adjustment.adjustment_start, 0)
       AND obligation.obligation_start < adjustment.adjustment_end
    ),
    payment_normalisation_adjustment_allocations AS MATERIALIZED (
      SELECT
        allocation.*,
        allocation.adjustment_base_obligation_amount
          - COALESCE(SUM(allocation.allocated_amount) OVER (
              PARTITION BY allocation.invoice_stage_row_id
              ORDER BY allocation.adjustment_sequence, allocation.adjustment_stage_row_id
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS obligation_before,
        GREATEST(0, allocation.adjustment_base_obligation_amount
          - SUM(allocation.allocated_amount) OVER (
              PARTITION BY allocation.invoice_stage_row_id
              ORDER BY allocation.adjustment_sequence, allocation.adjustment_stage_row_id
              ROWS UNBOUNDED PRECEDING
            )) AS obligation_after,
        CASE allocation.adjustment_kind
          WHEN 'CREDIT' THEN CASE WHEN allocation.adjustment_base_obligation_amount
            - SUM(allocation.allocated_amount) OVER (PARTITION BY allocation.invoice_stage_row_id ORDER BY allocation.adjustment_sequence, allocation.adjustment_stage_row_id ROWS UNBOUNDED PRECEDING) <= 0.005
            THEN 'CREDIT_FULL_OFFSET' ELSE 'CREDIT_PARTIAL_OFFSET' END
          WHEN 'REFUND' THEN CASE WHEN allocation.adjustment_base_obligation_amount
            - SUM(allocation.allocated_amount) OVER (PARTITION BY allocation.invoice_stage_row_id ORDER BY allocation.adjustment_sequence, allocation.adjustment_stage_row_id ROWS UNBOUNDED PRECEDING) <= 0.005
            THEN 'REFUND_FULL_OFFSET' ELSE 'REFUND_PARTIAL_OFFSET' END
          WHEN 'KG' THEN CASE WHEN allocation.adjustment_base_obligation_amount
            - SUM(allocation.allocated_amount) OVER (PARTITION BY allocation.invoice_stage_row_id ORDER BY allocation.adjustment_sequence, allocation.adjustment_stage_row_id ROWS UNBOUNDED PRECEDING) <= 0.005
            THEN 'KG_FULL_OFFSET' ELSE 'KG_PARTIAL_OFFSET' END
          ELSE CASE WHEN allocation.adjustment_base_obligation_amount
            - SUM(allocation.allocated_amount) OVER (PARTITION BY allocation.invoice_stage_row_id ORDER BY allocation.adjustment_sequence, allocation.adjustment_stage_row_id ROWS UNBOUNDED PRECEDING) <= 0.005
            THEN 'ET_FULL_OFFSET' ELSE 'ET_PARTIAL_OFFSET' END
        END AS reason_code
      FROM payment_normalisation_adjustment_allocations_raw allocation
      WHERE allocation.allocated_amount > 0.005
    ),
    payment_normalisation_adjustment_totals AS (
      SELECT invoice_stage_row_id, SUM(allocated_amount)::numeric AS allocated_amount
      FROM payment_normalisation_adjustment_allocations
      GROUP BY invoice_stage_row_id
    ),
    payment_normalisation_adjustment_source_totals AS MATERIALIZED (
      SELECT adjustment."id" AS adjustment_stage_row_id,
        adjustment.normalisation_amount AS original_amount,
        COALESCE(SUM(allocation.allocated_amount), 0)::numeric AS allocated_amount,
        CASE
          WHEN reversal.normalisation_group_key IS NOT NULL
            THEN adjustment.normalisation_amount
          WHEN adjustment.normalisation_role = 'KG'
            THEN adjustment.kg_reversal_offset_amount
          ELSE 0
        END::numeric
          AS reversal_offset_amount,
        GREATEST(0,
          CASE WHEN adjustment.normalisation_role = 'KG'
            THEN adjustment.allocation_amount
            ELSE adjustment.normalisation_amount
          END
          - COALESCE(SUM(allocation.allocated_amount), 0)
          - CASE WHEN reversal.normalisation_group_key IS NOT NULL
              THEN adjustment.normalisation_amount ELSE 0 END)::numeric
          AS unmatched_amount
      FROM payment_normalisation_adjustments adjustment
      LEFT JOIN payment_normalisation_adjustment_allocations allocation
        ON allocation.adjustment_stage_row_id = adjustment."id"
      LEFT JOIN payment_normalisation_net_zero_adjustment_reversal_groups reversal
        ON reversal.normalisation_group_key = adjustment.normalisation_group_key
      GROUP BY adjustment."id", adjustment.normalisation_amount,
        adjustment.normalisation_role, adjustment.allocation_amount,
        adjustment.kg_reversal_offset_amount,
        reversal.normalisation_group_key
    ),
    payment_normalisation_adjusted_obligations AS MATERIALIZED (
      SELECT obligation.*,
        GREATEST(0, obligation.obligation_amount_after_direction
          - COALESCE(adjustment.allocated_amount, 0))::numeric
          AS adjusted_obligation_amount,
        COALESCE(adjustment.allocated_amount, 0)::numeric AS adjustment_allocated_amount
      FROM payment_normalisation_directionally_adjusted_obligations obligation
      LEFT JOIN payment_normalisation_adjustment_totals adjustment
        ON adjustment.invoice_stage_row_id = obligation."id"
    ),
    payment_normalisation_obligation_payment_ranges AS (
      SELECT obligation.*,
        COALESCE(SUM(adjusted_obligation_amount) OVER (
          PARTITION BY normalisation_group_key ORDER BY obligation_sequence, "id"
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS obligation_start,
        SUM(adjusted_obligation_amount) OVER (
          PARTITION BY normalisation_group_key ORDER BY obligation_sequence, "id"
          ROWS UNBOUNDED PRECEDING
        ) AS obligation_end
      FROM payment_normalisation_adjusted_obligations obligation
      WHERE adjusted_obligation_amount > 0.005
    ),
    payment_normalisation_payments AS MATERIALIZED (
      SELECT source.*,
        ROW_NUMBER() OVER (
          PARTITION BY normalisation_group_key
          ORDER BY "paymentDate" NULLS LAST, "rowNo", "id"
        ) AS payment_sequence
      FROM payment_normalisation_source_rows source
      WHERE normalisation_role = 'PAYMENT'
    ),
    payment_normalisation_payment_ranges AS (
      SELECT payment.*,
        COALESCE(SUM(normalisation_amount) OVER (
          PARTITION BY normalisation_group_key ORDER BY payment_sequence, "id"
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0) AS payment_start,
        SUM(normalisation_amount) OVER (
          PARTITION BY normalisation_group_key ORDER BY payment_sequence, "id"
          ROWS UNBOUNDED PRECEDING
        ) AS payment_end
      FROM payment_normalisation_payments payment
    ),
    payment_normalisation_payment_allocations_raw AS (
      SELECT
        obligation."id" AS invoice_stage_row_id,
        payment."id" AS payment_stage_row_id,
        payment.payment_sequence,
        GREATEST(0, LEAST(obligation.obligation_end, payment.payment_end)
          - GREATEST(obligation.obligation_start, payment.payment_start))::numeric AS allocated_amount,
        obligation.adjusted_obligation_amount,
        obligation.original_obligation_amount,
        obligation.adjustment_allocated_amount,
        payment."paymentDate" AS settlement_payment_date,
        payment.normalisation_amount AS source_payment_amount
      FROM payment_normalisation_obligation_payment_ranges obligation
      JOIN payment_normalisation_payment_ranges payment
        ON payment.normalisation_group_key = obligation.normalisation_group_key
       AND obligation.obligation_end > payment.payment_start
       AND obligation.obligation_start < payment.payment_end
    ),
    payment_normalisation_payment_allocations AS MATERIALIZED (
      SELECT allocation.*,
        allocation.adjusted_obligation_amount
          - COALESCE(SUM(allocation.allocated_amount) OVER (
              PARTITION BY allocation.invoice_stage_row_id
              ORDER BY allocation.payment_sequence, allocation.payment_stage_row_id
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0) AS obligation_before_payment,
        GREATEST(0, allocation.adjusted_obligation_amount
          - SUM(allocation.allocated_amount) OVER (
              PARTITION BY allocation.invoice_stage_row_id
              ORDER BY allocation.payment_sequence, allocation.payment_stage_row_id
              ROWS UNBOUNDED PRECEDING
            )) AS obligation_after_payment
      FROM payment_normalisation_payment_allocations_raw allocation
      WHERE allocation.allocated_amount > 0.005
    ),
    payment_normalisation_payment_source_totals_raw AS MATERIALIZED (
      SELECT payment."id" AS payment_stage_row_id,
        payment.normalisation_amount AS original_amount,
        COALESCE(SUM(allocation.allocated_amount), 0)::numeric AS allocated_amount,
        GREATEST(0, payment.normalisation_amount
          - COALESCE(SUM(allocation.allocated_amount), 0))::numeric
          AS unmatched_amount_before_clearing_balance,
        context.is_prefix_5,
        context.signed_balance AS clearing_group_signed_balance
      FROM payment_normalisation_payments payment
      LEFT JOIN payment_normalisation_payment_allocations allocation
        ON allocation.payment_stage_row_id = payment."id"
      LEFT JOIN payment_normalisation_clearing_group_context context
        ON context.normalisation_group_key = payment.normalisation_group_key
      GROUP BY payment."id", payment.normalisation_amount,
        context.is_prefix_5, context.signed_balance
    ),
    payment_normalisation_payment_source_totals AS MATERIALIZED (
      SELECT totals.*,
        CASE
          WHEN totals.is_prefix_5
            AND ABS(totals.clearing_group_signed_balance) <= 0.005
            THEN totals.unmatched_amount_before_clearing_balance
          ELSE 0
        END::numeric AS clearing_balance_offset_amount,
        CASE
          WHEN totals.is_prefix_5
            AND ABS(totals.clearing_group_signed_balance) <= 0.005
            THEN 0
          ELSE totals.unmatched_amount_before_clearing_balance
        END::numeric AS unmatched_amount
      FROM payment_normalisation_payment_source_totals_raw totals
    ),
    payment_normalisation_unrecognised AS MATERIALIZED (
      SELECT source."id" AS source_stage_row_id,
        'UNRECOGNISED_DOCUMENT_TYPE'::text AS reason_code,
        source.document_type,
        source.normalisation_amount AS amount
      FROM payment_normalisation_source_rows source
      WHERE source.normalisation_role = 'UNRECOGNISED'
    ),
    payment_normalisation_exceptions_before_clearing_reconciliation AS MATERIALIZED (
      SELECT "id" AS source_stage_row_id,
        'MAPPING_EXCEPTION'::text AS reason_code,
        normalisation_amount AS amount
      FROM payment_normalisation_source_rows
      WHERE "semanticKind" = 'accounting_event'
        AND normalisation_role <> 'UNRECOGNISED'
        AND normalisation_group_key IS NULL
      UNION ALL
      SELECT adjustment_stage_row_id AS source_stage_row_id,
        CASE WHEN ambiguous.normalisation_group_key IS NOT NULL
          THEN 'AMBIGUOUS_ADJUSTMENT_COMBINATION'
          ELSE 'UNMATCHED_ADJUSTMENT' END::text AS reason_code,
        totals.unmatched_amount AS amount
      FROM payment_normalisation_adjustment_source_totals totals
      JOIN payment_normalisation_adjustments adjustment
        ON adjustment."id" = totals.adjustment_stage_row_id
      LEFT JOIN payment_normalisation_ambiguous_adjustment_groups ambiguous
        ON ambiguous.normalisation_group_key = adjustment.normalisation_group_key
      WHERE totals.unmatched_amount > 0.005
      UNION ALL
      SELECT source_stage_row_id, reason_code, amount
      FROM payment_normalisation_unmatched_kg_reversals
      UNION ALL
      SELECT offset_stage_row_id, 'UNMATCHED_OBLIGATION_OFFSET', unmatched_amount
      FROM payment_normalisation_directional_offset_source_totals
      WHERE unmatched_amount > 0.005
      UNION ALL
      SELECT payment_stage_row_id, 'UNMATCHED_PAYMENT', unmatched_amount
      FROM payment_normalisation_payment_source_totals
      WHERE unmatched_amount > 0.005
      UNION ALL
      SELECT allocation.invoice_stage_row_id, 'MAPPING_EXCEPTION',
        allocation.allocated_amount
      FROM payment_normalisation_payment_allocations allocation
      JOIN payment_normalisation_source_rows invoice
        ON invoice."id" = allocation.invoice_stage_row_id
      WHERE (invoice."invoiceIssueDate" IS NULL
          AND invoice."invoiceReceiptDate" IS NULL)
        OR allocation.settlement_payment_date IS NULL
      UNION ALL
      SELECT source_stage_row_id, reason_code, amount
      FROM payment_normalisation_unrecognised
    ),
    payment_normalisation_reconcilable_exception_groups AS MATERIALIZED (
      SELECT source.normalisation_group_key,
        COUNT(*)::int AS residual_exception_count,
        COALESCE(SUM(exception.amount), 0)::numeric
          AS residual_exception_amount_before_reconciliation,
        ARRAY_AGG(DISTINCT exception.reason_code ORDER BY exception.reason_code)
          AS residual_exception_codes_before_reconciliation
      FROM payment_normalisation_exceptions_before_clearing_reconciliation
        exception
      JOIN payment_normalisation_source_rows source
        ON source."id" = exception.source_stage_row_id
      WHERE source.normalisation_group_key IS NOT NULL
        AND exception.reason_code IN (
          'AMBIGUOUS_ADJUSTMENT_COMBINATION',
          'UNMATCHED_ADJUSTMENT',
          'UNMATCHED_KG_REVERSAL',
          'UNMATCHED_OBLIGATION_OFFSET',
          'UNMATCHED_PAYMENT',
          'UNRECOGNISED_DOCUMENT_TYPE'
        )
      GROUP BY source.normalisation_group_key
    ),
    payment_normalisation_clearing_group_evidence AS MATERIALIZED (
      SELECT source.normalisation_group_key,
        SUM(source.normalisation_amount)::numeric AS source_absolute_amount,
        ARRAY_AGG(DISTINCT source.document_type ORDER BY source.document_type)
          AS participating_document_types,
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'stageRowId', source."id",
            'rowNo', source."rowNo",
            'documentType', source.document_type,
            'normalisationRole', source.normalisation_role,
            'signedAmount', source.signed_amount
          ) ORDER BY source."rowNo", source."id"
        ) AS source_rows
      FROM payment_normalisation_source_rows source
      JOIN payment_normalisation_reconcilable_exception_groups exception_group
        ON exception_group.normalisation_group_key =
          source.normalisation_group_key
      GROUP BY source.normalisation_group_key
    ),
    payment_normalisation_clearing_reconciliations AS MATERIALIZED (
      SELECT exception_group.normalisation_group_key,
        CASE WHEN ABS(context.signed_balance) <= 0.005
          THEN 'BALANCED_CLEARING_RECONCILIATION'::text ELSE NULL END
          AS reconciliation_code,
        ABS(context.signed_balance) <= 0.005
          AS accepted_as_balanced_clearing,
        context.signed_balance AS signed_clearing_group_total,
        context.signed_balance
          AS remaining_signed_residual_before_final_reconciliation,
        CASE WHEN ABS(context.signed_balance) <= 0.005
          THEN 0 ELSE context.signed_balance END::numeric
          AS final_unexplained_signed_residual,
        group_evidence.source_absolute_amount,
        exception_group.residual_exception_count,
        exception_group.residual_exception_amount_before_reconciliation,
        exception_group.residual_exception_codes_before_reconciliation,
        group_evidence.participating_document_types,
        group_evidence.source_rows,
        COALESCE((
          SELECT SUM(totals.allocated_amount)
          FROM payment_normalisation_directional_offset_source_totals totals
          JOIN payment_normalisation_source_rows source
            ON source."id" = totals.offset_stage_row_id
          WHERE source.normalisation_group_key =
            exception_group.normalisation_group_key
        ), 0)::numeric AS directional_obligation_offset_amount,
        COALESCE((
          SELECT SUM(totals.allocated_amount)
          FROM payment_normalisation_adjustment_source_totals totals
          JOIN payment_normalisation_adjustments adjustment
            ON adjustment."id" = totals.adjustment_stage_row_id
          WHERE adjustment.normalisation_group_key =
            exception_group.normalisation_group_key
        ), 0)::numeric AS adjustment_allocated_amount,
        COALESCE((
          SELECT SUM(totals.reversal_offset_amount)
          FROM payment_normalisation_adjustment_source_totals totals
          JOIN payment_normalisation_adjustments adjustment
            ON adjustment."id" = totals.adjustment_stage_row_id
          WHERE adjustment.normalisation_group_key =
            exception_group.normalisation_group_key
        ), 0)::numeric AS adjustment_reversal_offset_amount,
        COALESCE((
          SELECT SUM(totals.allocated_amount)
          FROM payment_normalisation_payment_source_totals totals
          JOIN payment_normalisation_payments payment
            ON payment."id" = totals.payment_stage_row_id
          WHERE payment.normalisation_group_key =
            exception_group.normalisation_group_key
        ), 0)::numeric AS payment_allocated_amount,
        COALESCE((
          SELECT SUM(totals.clearing_balance_offset_amount)
          FROM payment_normalisation_payment_source_totals totals
          JOIN payment_normalisation_payments payment
            ON payment."id" = totals.payment_stage_row_id
          WHERE payment.normalisation_group_key =
            exception_group.normalisation_group_key
        ), 0)::numeric AS payment_clearing_balance_offset_amount
      FROM payment_normalisation_reconcilable_exception_groups exception_group
      JOIN payment_normalisation_clearing_group_context context
        ON context.normalisation_group_key =
          exception_group.normalisation_group_key
      JOIN payment_normalisation_clearing_group_evidence group_evidence
        ON group_evidence.normalisation_group_key =
          exception_group.normalisation_group_key
    ),
    payment_normalisation_exceptions AS MATERIALIZED (
      SELECT exception.source_stage_row_id,
        exception.reason_code,
        exception.amount
      FROM payment_normalisation_exceptions_before_clearing_reconciliation
        exception
      JOIN payment_normalisation_source_rows source
        ON source."id" = exception.source_stage_row_id
      LEFT JOIN payment_normalisation_clearing_reconciliations reconciliation
        ON reconciliation.normalisation_group_key =
          source.normalisation_group_key
      WHERE NOT (
        COALESCE(reconciliation.accepted_as_balanced_clearing, false)
        AND exception.reason_code IN (
          'AMBIGUOUS_ADJUSTMENT_COMBINATION',
          'UNMATCHED_ADJUSTMENT',
          'UNMATCHED_KG_REVERSAL',
          'UNMATCHED_OBLIGATION_OFFSET',
          'UNMATCHED_PAYMENT',
          'UNRECOGNISED_DOCUMENT_TYPE'
        )
      )
    )
  `;
}

function buildPersistPaymentNormalisationSql() {
  const {
    paymentTimeDaysSql,
    referenceKindSql,
    referenceDateSql,
    referencePolicySql,
    referenceReasonSql,
  } = buildPaymentTimeAllocationProjectionSql();
  return `
      WITH ${buildPaymentNormalisationCte()},
      invoice_adjustment_evidence AS MATERIALIZED (
        SELECT
          allocation.invoice_stage_row_id,
          jsonb_agg(DISTINCT allocation.reason_code) AS reason_codes
        FROM payment_normalisation_adjustment_allocations allocation
        GROUP BY allocation.invoice_stage_row_id
      ),
      invoice_payment_evidence AS MATERIALIZED (
        SELECT
          allocation.invoice_stage_row_id,
          SUM(allocation.allocated_amount)::numeric AS allocated_amount,
          jsonb_agg(
            allocation.payment_stage_row_id
            ORDER BY allocation.payment_sequence
          ) AS payment_stage_row_ids
        FROM payment_normalisation_payment_allocations allocation
        GROUP BY allocation.invoice_stage_row_id
      ),
      invoice_evidence AS (
        SELECT obligation."id" AS stage_row_id,
          jsonb_build_object(
            'role', 'INVOICE_OBLIGATION',
            'originalObligationAmount', obligation.normalisation_amount,
            'signedObligationAmount', obligation.signed_amount,
            'directionalOffsetAllocatedAmount', COALESCE(
              NULLIF(obligation.directional_offset_allocated_amount, 0),
              directional_offset.allocated_amount,
              0
            ),
            'directionalOffsetUnmatchedAmount', COALESCE(
              directional_offset.unmatched_amount,
              0
            ),
            'adjustmentAllocatedAmount', obligation.adjustment_allocated_amount,
            'adjustedObligationAmount', obligation.adjusted_obligation_amount,
            'paymentAllocatedAmount', COALESCE(payment.allocated_amount, 0),
            'outstandingAmount', GREATEST(0, obligation.adjusted_obligation_amount - COALESCE(payment.allocated_amount, 0)),
            'exceptionCode', CASE
              WHEN directional_offset.unmatched_amount > 0.005
                THEN 'UNMATCHED_OBLIGATION_OFFSET'
              WHEN COALESCE(payment.allocated_amount, 0) > 0.005
                AND obligation."invoiceIssueDate" IS NULL
                AND obligation."invoiceReceiptDate" IS NULL
                THEN 'MAPPING_EXCEPTION'
              ELSE NULL
            END,
            'adjustmentReasonCodes', COALESCE(
              adjustment.reason_codes,
              '[]'::jsonb
            ),
            'paymentStageRowIds', COALESCE(
              payment.payment_stage_row_ids,
              '[]'::jsonb
            )
          ) AS evidence
        FROM payment_normalisation_adjusted_obligations obligation
        LEFT JOIN invoice_adjustment_evidence adjustment
          ON adjustment.invoice_stage_row_id = obligation."id"
        LEFT JOIN invoice_payment_evidence payment
          ON payment.invoice_stage_row_id = obligation."id"
        LEFT JOIN payment_normalisation_directional_offset_source_totals
            directional_offset
          ON directional_offset.offset_stage_row_id = obligation."id"
      ),
      adjustment_evidence AS (
        SELECT adjustment."id" AS stage_row_id,
          jsonb_build_object(
            'role', adjustment.normalisation_role || '_ADJUSTMENT',
            'originalAmount', totals.original_amount,
            'signedAmount', adjustment.signed_amount,
            'effectiveAdjustmentAmount', adjustment.allocation_amount,
            'allocatedAmount', totals.allocated_amount,
            'reversalOffsetAmount', totals.reversal_offset_amount,
            'unmatchedAmount', totals.unmatched_amount,
            'reconciliationCode', CASE
              WHEN adjustment.normalisation_role = 'KG'
                AND totals.reversal_offset_amount > 0.005
                THEN 'KG_REVERSAL_OFFSET'
              WHEN totals.reversal_offset_amount > 0.005
                THEN 'NET_ZERO_ADJUSTMENT_REVERSAL'
              ELSE NULL
            END,
            'reasonCodes', COALESCE(jsonb_agg(DISTINCT allocation.reason_code)
              FILTER (WHERE allocation.reason_code IS NOT NULL), '[]'::jsonb),
            'invoiceStageRowIds', COALESCE(jsonb_agg(allocation.invoice_stage_row_id ORDER BY allocation.invoice_stage_row_id)
              FILTER (WHERE allocation.invoice_stage_row_id IS NOT NULL), '[]'::jsonb),
            'exceptionCode', CASE
              WHEN EXISTS (
                SELECT 1
                FROM payment_normalisation_unmatched_kg_reversals kg_reversal
                WHERE kg_reversal.source_stage_row_id = adjustment."id"
              ) THEN 'UNMATCHED_KG_REVERSAL'
              WHEN totals.unmatched_amount <= 0.005 THEN NULL
              WHEN EXISTS (
                SELECT 1
                FROM payment_normalisation_ambiguous_adjustment_groups ambiguous
                WHERE ambiguous.normalisation_group_key = adjustment.normalisation_group_key
              ) THEN 'AMBIGUOUS_ADJUSTMENT_COMBINATION'
              ELSE 'UNMATCHED_ADJUSTMENT'
            END
          ) AS evidence
        FROM payment_normalisation_adjustments adjustment
        JOIN payment_normalisation_adjustment_source_totals totals
          ON totals.adjustment_stage_row_id = adjustment."id"
        LEFT JOIN payment_normalisation_adjustment_allocations allocation
          ON allocation.adjustment_stage_row_id = adjustment."id"
        GROUP BY adjustment."id", adjustment.normalisation_role,
          adjustment.normalisation_group_key, adjustment.signed_amount,
          adjustment.allocation_amount,
          totals.original_amount, totals.allocated_amount,
          totals.reversal_offset_amount, totals.unmatched_amount
      ),
      payment_evidence AS (
        SELECT payment."id" AS stage_row_id,
          jsonb_build_object(
            'role', 'PAYMENT',
            'originalAmount', totals.original_amount,
            'allocatedAmount', totals.allocated_amount,
            'unmatchedAmount', totals.unmatched_amount,
            'unmatchedAmountBeforeClearingBalance', totals.unmatched_amount_before_clearing_balance,
            'clearingBalanceOffsetAmount', totals.clearing_balance_offset_amount,
            'clearingGroupSignedBalance', totals.clearing_group_signed_balance,
            'partialPayment', COALESCE(BOOL_OR(allocation.obligation_after_payment > 0.005), false),
            'finalSettlement', COALESCE(BOOL_OR(allocation.obligation_after_payment <= 0.005), false),
            'classificationBasis', CASE
              WHEN payment.document_type = 'KZ'
                THEN 'outstanding_obligation_after_clearing_settlement'
              ELSE 'outstanding_obligation_after_zp'
            END,
            'contractualInstalmentIndicatorAvailable', false,
            'invoiceStageRowIds', COALESCE(jsonb_agg(allocation.invoice_stage_row_id ORDER BY allocation.invoice_stage_row_id)
              FILTER (WHERE allocation.invoice_stage_row_id IS NOT NULL), '[]'::jsonb),
            'exceptionCode', CASE WHEN totals.unmatched_amount > 0.005 THEN 'UNMATCHED_PAYMENT' ELSE NULL END
          ) AS evidence
        FROM payment_normalisation_payments payment
        JOIN payment_normalisation_payment_source_totals totals
          ON totals.payment_stage_row_id = payment."id"
        LEFT JOIN payment_normalisation_payment_allocations allocation
          ON allocation.payment_stage_row_id = payment."id"
        GROUP BY payment."id", payment.document_type, totals.original_amount,
          totals.allocated_amount, totals.unmatched_amount,
          totals.unmatched_amount_before_clearing_balance,
          totals.clearing_balance_offset_amount,
          totals.clearing_group_signed_balance
      ),
      exception_evidence AS (
        SELECT unrecognised.source_stage_row_id AS stage_row_id,
          jsonb_build_object(
            'role', CASE
              WHEN reconciliation.accepted_as_balanced_clearing
                THEN 'RECONCILED_CLEARING_EVIDENCE'
              ELSE 'EXCEPTION'
            END,
            'exceptionCode', CASE
              WHEN reconciliation.accepted_as_balanced_clearing THEN NULL
              ELSE unrecognised.reason_code
            END,
            'documentType', unrecognised.document_type,
            'amount', unrecognised.amount
          ) AS evidence
        FROM payment_normalisation_unrecognised unrecognised
        JOIN payment_normalisation_source_rows source
          ON source."id" = unrecognised.source_stage_row_id
        LEFT JOIN payment_normalisation_clearing_reconciliations reconciliation
          ON reconciliation.normalisation_group_key =
            source.normalisation_group_key
      ),
      evidence_base AS (
        SELECT * FROM invoice_evidence
        UNION ALL SELECT * FROM adjustment_evidence
        UNION ALL SELECT * FROM payment_evidence
        UNION ALL SELECT * FROM exception_evidence
      ),
      evidence AS (
        SELECT evidence_base.stage_row_id,
          CASE
            WHEN reconciliation.accepted_as_balanced_clearing THEN
              CASE
                WHEN evidence_base.evidence ? 'unmatchedAmount' THEN
                  evidence_base.evidence || jsonb_build_object(
                    'unmatchedAmountBeforeClearingReconciliation',
                      COALESCE((evidence_base.evidence->>'unmatchedAmount')::numeric, 0),
                    'unmatchedAmount', 0,
                    'clearingReconciliationOffsetAmount',
                      COALESCE((evidence_base.evidence->>'unmatchedAmount')::numeric, 0),
                    'exceptionCode', CASE
                      WHEN evidence_base.evidence->>'exceptionCode' IN (
                        'AMBIGUOUS_ADJUSTMENT_COMBINATION',
                        'UNMATCHED_ADJUSTMENT',
                        'UNMATCHED_KG_REVERSAL',
                        'UNMATCHED_OBLIGATION_OFFSET',
                        'UNMATCHED_PAYMENT',
                        'UNRECOGNISED_DOCUMENT_TYPE'
                      ) THEN NULL
                      ELSE evidence_base.evidence->>'exceptionCode'
                    END
                  )
                ELSE evidence_base.evidence || jsonb_build_object(
                  'exceptionCode', CASE
                    WHEN evidence_base.evidence->>'exceptionCode' IN (
                      'AMBIGUOUS_ADJUSTMENT_COMBINATION',
                      'UNMATCHED_ADJUSTMENT',
                      'UNMATCHED_KG_REVERSAL',
                      'UNMATCHED_OBLIGATION_OFFSET',
                      'UNMATCHED_PAYMENT',
                      'UNRECOGNISED_DOCUMENT_TYPE'
                    ) THEN NULL
                    ELSE evidence_base.evidence->>'exceptionCode'
                  END
                )
              END
            ELSE evidence_base.evidence
          END || CASE
            WHEN reconciliation.accepted_as_balanced_clearing
              AND evidence_base.evidence ? 'directionalOffsetUnmatchedAmount'
              THEN jsonb_build_object(
                'directionalOffsetUnmatchedAmountBeforeClearingReconciliation',
                  COALESCE((evidence_base.evidence->>'directionalOffsetUnmatchedAmount')::numeric, 0),
                'directionalOffsetUnmatchedAmount', 0,
                'clearingReconciliationOffsetAmount',
                  COALESCE((evidence_base.evidence->>'directionalOffsetUnmatchedAmount')::numeric, 0)
              )
            ELSE '{}'::jsonb
          END || CASE
            WHEN reconciliation.normalisation_group_key IS NULL THEN '{}'::jsonb
            ELSE jsonb_build_object(
              'clearingReconciliation', jsonb_build_object(
                'reconciliationCode', reconciliation.reconciliation_code,
                'acceptedAsBalancedClearing',
                  reconciliation.accepted_as_balanced_clearing,
                'signedClearingGroupTotal',
                  reconciliation.signed_clearing_group_total,
                'remainingSignedResidualBeforeFinalReconciliation',
                  reconciliation.remaining_signed_residual_before_final_reconciliation,
                'finalUnexplainedSignedResidual',
                  reconciliation.final_unexplained_signed_residual,
                'sourceAbsoluteAmount', reconciliation.source_absolute_amount,
                'residualExceptionCountBeforeReconciliation',
                  reconciliation.residual_exception_count,
                'residualExceptionAmountBeforeReconciliation',
                  reconciliation.residual_exception_amount_before_reconciliation,
                'residualExceptionCodesBeforeReconciliation',
                  reconciliation.residual_exception_codes_before_reconciliation,
                'participatingDocumentTypes',
                  reconciliation.participating_document_types,
                'sourceRows', reconciliation.source_rows,
                'semanticEffects', jsonb_build_object(
                  'directionalObligationOffsetAmount',
                    reconciliation.directional_obligation_offset_amount,
                  'adjustmentAllocatedAmount',
                    reconciliation.adjustment_allocated_amount,
                  'adjustmentReversalOffsetAmount',
                    reconciliation.adjustment_reversal_offset_amount,
                  'paymentAllocatedAmount',
                    reconciliation.payment_allocated_amount,
                  'paymentClearingBalanceOffsetAmount',
                    reconciliation.payment_clearing_balance_offset_amount
                )
              )
            )
          END || jsonb_build_object(
            'normalisationResultId', :normalisationResultId,
            'inputSignature', :inputSignature,
            'calculationVersion', :calculationVersion
          ) AS evidence
        FROM evidence_base
        JOIN payment_normalisation_source_rows source
          ON source."id" = evidence_base.stage_row_id
        LEFT JOIN payment_normalisation_clearing_reconciliations reconciliation
          ON reconciliation.normalisation_group_key =
            source.normalisation_group_key
      ),
      exception_codes AS (
        SELECT source_stage_row_id, MIN(reason_code) AS exception_code
        FROM payment_normalisation_exceptions
        GROUP BY source_stage_row_id
      ),
      normalisation_rows_inserted AS (
        INSERT INTO "tbl_ptrs_payment_normalisation_row" (
          "normalisationResultId", "customerId", "ptrsId", "stageRowId",
          "rowNo", "normalisationGroupKey", "normalisationRole",
          "documentType", "companyCode", "sourceAccountCode",
          "clearingDocument", "normalisationAmount",
          "originalObligationAmount", "adjustedObligationAmount",
          "adjustmentAllocatedAmount", "paymentAllocatedAmount",
          "outstandingAmount", "unmatchedAmount", "reversalOffsetAmount",
          "exceptionCode", "createdAt"
        )
        SELECT
          :normalisationResultId, :customerId, :ptrsId, source."id",
          source."rowNo", source.normalisation_group_key,
          source.normalisation_role, source.document_type,
          source.company_code, source.source_account_code,
          source.clearing_document, source.normalisation_amount,
          obligation.original_obligation_amount,
          obligation.adjusted_obligation_amount,
          obligation.adjustment_allocated_amount,
          payment_evidence.allocated_amount,
          CASE WHEN obligation."id" IS NOT NULL THEN
            GREATEST(0, obligation.adjusted_obligation_amount
              - COALESCE(payment_evidence.allocated_amount, 0))
            ELSE NULL END,
          CASE
            WHEN reconciliation.accepted_as_balanced_clearing THEN 0
            ELSE CASE
              WHEN directional_offset_totals.offset_stage_row_id IS NOT NULL
                THEN directional_offset_totals.unmatched_amount
              WHEN adjustment_totals.adjustment_stage_row_id IS NOT NULL
                THEN adjustment_totals.unmatched_amount
              WHEN payment_totals.payment_stage_row_id IS NOT NULL
                THEN payment_totals.unmatched_amount
              ELSE NULL
            END
          END,
          adjustment_totals.reversal_offset_amount,
          exception_codes.exception_code,
          now()
        FROM payment_normalisation_source_rows source
        LEFT JOIN payment_normalisation_adjusted_obligations obligation
          ON obligation."id" = source."id"
        LEFT JOIN invoice_payment_evidence payment_evidence
          ON payment_evidence.invoice_stage_row_id = source."id"
        LEFT JOIN payment_normalisation_adjustment_source_totals adjustment_totals
          ON adjustment_totals.adjustment_stage_row_id = source."id"
        LEFT JOIN payment_normalisation_directional_offset_source_totals
            directional_offset_totals
          ON directional_offset_totals.offset_stage_row_id = source."id"
        LEFT JOIN payment_normalisation_payment_source_totals payment_totals
          ON payment_totals.payment_stage_row_id = source."id"
        LEFT JOIN exception_codes
          ON exception_codes.source_stage_row_id = source."id"
        LEFT JOIN payment_normalisation_clearing_reconciliations reconciliation
          ON reconciliation.normalisation_group_key =
            source.normalisation_group_key
        RETURNING 1
      ),
      persisted_allocation_source AS MATERIALIZED (
        SELECT allocation.*,
          invoice_source.normalisation_group_key,
          invoice_source.company_code,
          invoice_source.source_account_code,
          invoice_source.clearing_document,
          ${paymentTimeDaysSql} AS payment_time_days,
          ${referenceKindSql} AS payment_time_reference_kind,
          ${referenceDateSql} AS payment_time_reference_date,
          ${referencePolicySql} AS payment_time_reference_policy,
          ${referenceReasonSql} AS payment_time_reference_reason,
          CASE WHEN allocation.settlement_payment_date IS NULL
              OR (invoice."invoiceIssueDate" IS NULL
                AND invoice."invoiceReceiptDate" IS NULL)
            THEN 'MAPPING_EXCEPTION' ELSE NULL END AS mapping_exception_code
        FROM payment_normalisation_payment_allocations allocation
        JOIN payment_normalisation_source_rows invoice_source
          ON invoice_source."id" = allocation.invoice_stage_row_id
        JOIN "tbl_ptrs_stage_row" invoice
          ON invoice."id" = allocation.invoice_stage_row_id
         AND invoice."customerId" = :customerId
         AND invoice."ptrsId" = :ptrsId
         AND invoice."deletedAt" IS NULL
      ),
      normalisation_allocations_inserted AS (
        INSERT INTO "tbl_ptrs_payment_normalisation_allocation" (
          "normalisationResultId", "customerId", "ptrsId",
          "invoiceStageRowId", "paymentStageRowId", "paymentSequence",
          "normalisationGroupKey", "companyCode", "sourceAccountCode",
          "clearingDocument", "allocatedAmount", "originalObligationAmount",
          "adjustedObligationAmount", "adjustmentAllocatedAmount",
          "settlementPaymentDate", "sourcePaymentAmount",
          "obligationBeforePayment", "obligationAfterPayment",
          "partialPayment", "finalSettlement", "paymentTimeDays",
          "paymentTimeReferenceKind", "paymentTimeReferenceDate",
          "paymentTimeReferencePolicy", "paymentTimeReferenceReason",
          "mappingExceptionCode", "createdAt"
        )
        SELECT
          :normalisationResultId, :customerId, :ptrsId,
          invoice_stage_row_id, payment_stage_row_id, payment_sequence,
          normalisation_group_key, company_code, source_account_code,
          clearing_document, allocated_amount, original_obligation_amount,
          adjusted_obligation_amount, adjustment_allocated_amount,
          settlement_payment_date, source_payment_amount,
          obligation_before_payment, obligation_after_payment,
          obligation_after_payment > 0.005,
          obligation_after_payment <= 0.005,
          payment_time_days, payment_time_reference_kind,
          payment_time_reference_date, payment_time_reference_policy,
          payment_time_reference_reason, mapping_exception_code, now()
        FROM persisted_allocation_source
        RETURNING 1
      ),
      normalisation_exceptions_inserted AS (
        INSERT INTO "tbl_ptrs_payment_normalisation_exception" (
          "normalisationResultId", "customerId", "ptrsId",
          "sourceStageRowId", "reasonCode", "amount", "documentType",
          "createdAt"
        )
        SELECT :normalisationResultId, :customerId, :ptrsId,
          exception.source_stage_row_id, exception.reason_code,
          exception.amount, source.document_type, now()
        FROM payment_normalisation_exceptions exception
        JOIN payment_normalisation_source_rows source
          ON source."id" = exception.source_stage_row_id
        RETURNING 1
      ),
      updated AS (
      UPDATE "tbl_ptrs_stage_row" stage_row
      SET "meta" = jsonb_set(COALESCE(stage_row."meta", '{}'::jsonb),
            '{paymentNormalisation}', evidence.evidence, true),
          "data" = CASE
            WHEN evidence.evidence->>'role' = 'PAYMENT' THEN
              jsonb_set(COALESCE(stage_row."data", '{}'::jsonb), '{partial_payment}',
                to_jsonb(COALESCE((evidence.evidence->>'partialPayment')::boolean, false)), true)
            ELSE COALESCE(stage_row."data", '{}'::jsonb)
          END,
          "updatedAt" = now()
      FROM evidence
      WHERE stage_row."id" = evidence.stage_row_id
        AND stage_row."customerId" = :customerId
        AND stage_row."ptrsId" = :ptrsId
        AND stage_row."deletedAt" IS NULL
      RETURNING stage_row."id"
      )
      SELECT
        (SELECT COUNT(*)::int FROM updated) AS "persisted",
        (SELECT COUNT(*)::int FROM normalisation_rows_inserted)
          AS "normalisationRowCount",
        (SELECT COUNT(*)::int FROM normalisation_allocations_inserted)
          AS "persistedAllocationCount",
        (SELECT COUNT(*)::int FROM normalisation_exceptions_inserted)
          AS "persistedExceptionCount",
        (SELECT COUNT(*)::int FROM payment_normalisation_source_rows) AS "startingStageCount",
        (SELECT COALESCE(SUM(normalisation_amount), 0)::numeric FROM payment_normalisation_source_rows) AS "startingAbsoluteValue",
        (SELECT COUNT(*)::int FROM payment_normalisation_obligations) AS "invoiceObligationCount",
        (SELECT COALESCE(SUM(normalisation_amount), 0)::numeric FROM payment_normalisation_obligations) AS "originalObligationValue",
        (SELECT COALESCE(SUM(adjusted_obligation_amount), 0)::numeric FROM payment_normalisation_adjusted_obligations) AS "adjustedObligationValue",
        (SELECT COALESCE(SUM(allocated_amount), 0)::numeric
         FROM payment_normalisation_directional_offset_source_totals)
          AS "directionalObligationOffsetValue",
        (SELECT COALESCE(SUM(unmatched_amount), 0)::numeric
         FROM payment_normalisation_directional_offset_source_totals)
          AS "unmatchedDirectionalObligationOffsetValue",
        (SELECT COUNT(*)::int FROM payment_normalisation_payments) AS "paymentEventCount",
        (SELECT COALESCE(SUM(normalisation_amount), 0)::numeric FROM payment_normalisation_payments) AS "paymentEventValue",
        (SELECT COUNT(*)::int FROM payment_normalisation_payment_allocations) AS "paymentAllocationCount",
        (SELECT COUNT(*)::int FROM payment_normalisation_payment_allocations WHERE obligation_after_payment > 0.005) AS "partialPaymentCount",
        (SELECT COUNT(*)::int FROM payment_normalisation_payment_allocations WHERE obligation_after_payment <= 0.005) AS "finalSettlementCount",
        (SELECT COALESCE(SUM(normalisation_amount), 0)::numeric FROM payment_normalisation_adjustments WHERE normalisation_role = 'CREDIT') AS "creditValue",
        (SELECT COALESCE(SUM(allocated_amount), 0)::numeric FROM payment_normalisation_adjustment_allocations WHERE adjustment_kind = 'CREDIT') AS "creditAllocatedValue",
        (SELECT COALESCE(SUM(normalisation_amount), 0)::numeric FROM payment_normalisation_adjustments WHERE normalisation_role = 'REFUND') AS "refundValue",
        (SELECT COALESCE(SUM(allocated_amount), 0)::numeric FROM payment_normalisation_adjustment_allocations WHERE adjustment_kind = 'REFUND') AS "refundAllocatedValue",
        (SELECT COALESCE(SUM(normalisation_amount), 0)::numeric FROM payment_normalisation_adjustments WHERE normalisation_role = 'ET') AS "earlyTradeDiscountValue",
        (SELECT COALESCE(SUM(allocated_amount), 0)::numeric FROM payment_normalisation_adjustment_allocations WHERE adjustment_kind = 'ET') AS "earlyTradeDiscountAllocatedValue",
        (SELECT COUNT(*)::int FROM payment_normalisation_adjustment_allocations WHERE adjustment_kind = 'ET') AS "earlyTradeMatchCount",
        (SELECT COALESCE(SUM(normalisation_amount), 0)::numeric
         FROM payment_normalisation_adjustments
         WHERE normalisation_role = 'KG') AS "vendorCreditMemoValue",
        (SELECT COALESCE(SUM(allocated_amount), 0)::numeric
         FROM payment_normalisation_adjustment_allocations
         WHERE adjustment_kind = 'KG') AS "vendorCreditMemoAllocatedValue",
        (SELECT COALESCE(SUM(clearing_balance_offset_amount), 0)::numeric
         FROM payment_normalisation_payment_source_totals)
          AS "balancedClearingPaymentOffsetValue",
        (SELECT COUNT(*)::int
         FROM payment_normalisation_clearing_reconciliations
         WHERE accepted_as_balanced_clearing)
          AS "balancedClearingReconciliationCount",
        (SELECT COALESCE(SUM(ABS(final_unexplained_signed_residual)), 0)::numeric
         FROM payment_normalisation_clearing_reconciliations)
          AS "unexplainedClearingResidualValue",
        (SELECT COALESCE(SUM(reversal_offset_amount), 0)::numeric
         FROM payment_normalisation_adjustment_source_totals)
          AS "adjustmentReversalOffsetValue",
        (SELECT COALESCE(SUM(amount), 0)::numeric
         FROM payment_normalisation_exceptions
         WHERE reason_code IN (
           'UNMATCHED_ADJUSTMENT',
           'AMBIGUOUS_ADJUSTMENT_COMBINATION'
         )) AS "unmatchedAdjustmentValue",
        (SELECT COUNT(*)::int
         FROM payment_normalisation_exceptions
         WHERE reason_code IN (
           'UNMATCHED_ADJUSTMENT',
           'AMBIGUOUS_ADJUSTMENT_COMBINATION'
         )) AS "unmatchedAdjustmentExceptionCount",
        (SELECT COUNT(*)::int FROM payment_normalisation_exceptions) AS "exceptionCount"
  `;
}

async function recordPaymentNormalisationFailure({
  customerId,
  ptrsId,
  inputState,
  calculationVersion,
  userId,
  error,
}) {
  if (!inputState?.inputSignature) return;
  const transaction = await beginTransactionWithCustomerContext(customerId);
  try {
    const identity = `${customerId}:${ptrsId}`;
    await acquirePaymentNormalisationLock({ transaction, identity });
    const existing = await db.PtrsPaymentNormalisationResult.findOne({
      where: {
        customerId,
        ptrsId,
        inputSignature: inputState.inputSignature,
        calculationVersion,
      },
      transaction,
    });
    if (existing?.status !== "succeeded") {
      const values = {
        customerId,
        ptrsId,
        profileId: inputState.profileId,
        stageExecutionRunId: inputState.stageExecutionRunId,
        stageInputHash: inputState.stageInputHash,
        normalisationInputRevision: inputState.normalisationInputRevision,
        inputSignature: inputState.inputSignature,
        calculationVersion,
        status: "failed",
        summary: null,
        errorMessage: String(error?.message || error).slice(0, 4000),
        createdBy: userId || null,
        completedAt: new Date(),
      };
      if (existing) await existing.update(values, { transaction });
      else await db.PtrsPaymentNormalisationResult.create(values, { transaction });
    }
    await transaction.commit();
  } catch (failureError) {
    if (!transaction.finished) await transaction.rollback();
  }
}

async function persistPaymentNormalisationEvidence({
  customerId,
  ptrsId,
  profileId = null,
  userId = null,
  calculationVersion = PAYMENT_NORMALISATION_VERSION,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");
  const operationStartedAt = process.hrtime.bigint();
  const transaction = await beginTransactionWithCustomerContext(customerId);
  let inputState;
  try {
    const lookupStartedAt = process.hrtime.bigint();
    inputState = await readPaymentNormalisationInputState({
      customerId,
      ptrsId,
      profileId,
      transaction,
    });
    const identity = `${customerId}:${ptrsId}`;
    await acquirePaymentNormalisationLock({ transaction, identity });
    const lockedInputState = await readPaymentNormalisationInputState({
      customerId,
      ptrsId,
      profileId,
      transaction,
    });
    if (lockedInputState.inputSignature !== inputState.inputSignature) {
      const error = new Error(
        "PTRS payment-normalisation inputs changed while calculation was starting",
      );
      error.code = "PTRS_NORMALISATION_INPUT_CHANGED";
      error.statusCode = 409;
      throw error;
    }
    inputState = lockedInputState;
    let result = await db.PtrsPaymentNormalisationResult.findOne({
      where: {
        customerId,
        ptrsId,
        inputSignature: inputState.inputSignature,
        calculationVersion,
      },
      transaction,
    });
    const lookupMs = Number(process.hrtime.bigint() - lookupStartedAt) / 1e6;
    if (result?.status === "succeeded") {
      await transaction.commit();
      return {
        source: "persisted",
        normalisationResultId: result.id,
        inputSignature: inputState.inputSignature,
        calculationVersion,
        persisted: 0,
        summary: result.summary || {},
        timings: {
          lookupMs,
          calculationAndPersistenceMs: 0,
          totalMs: Number(process.hrtime.bigint() - operationStartedAt) / 1e6,
        },
        limitations: {
          contractualInstalmentIndicatorAvailable: false,
        },
      };
    }
    if (result) {
      await db.PtrsPaymentNormalisationRow.destroy({
        where: { normalisationResultId: result.id },
        transaction,
      });
      await db.PtrsPaymentNormalisationAllocation.destroy({
        where: { normalisationResultId: result.id },
        transaction,
      });
      await db.PtrsPaymentNormalisationException.destroy({
        where: { normalisationResultId: result.id },
        transaction,
      });
      await result.update(
        {
          status: "calculating",
          summary: null,
          errorMessage: null,
          createdBy: userId || result.createdBy || null,
          startedAt: new Date(),
          completedAt: null,
        },
        { transaction },
      );
    } else {
      result = await db.PtrsPaymentNormalisationResult.create(
        {
          customerId,
          ptrsId,
          profileId: inputState.profileId,
          stageExecutionRunId: inputState.stageExecutionRunId,
          stageInputHash: inputState.stageInputHash,
          normalisationInputRevision: inputState.normalisationInputRevision,
          inputSignature: inputState.inputSignature,
          calculationVersion,
          status: "calculating",
          createdBy: userId || null,
        },
        { transaction },
      );
    }
    const calculationStartedAt = process.hrtime.bigint();
    const sql = buildPersistPaymentNormalisationSql();
    const resultRows = await db.sequelize.query(sql, {
      transaction,
      replacements: {
        customerId,
        ptrsId,
        normalisationResultId: result.id,
        inputSignature: inputState.inputSignature,
        calculationVersion,
      },
      type: db.sequelize.QueryTypes.SELECT,
    });
    const materialised = resultRows?.[0] || {};
    const { persisted, ...summary } = materialised;
    const finalInputState = await readPaymentNormalisationInputState({
      customerId,
      ptrsId,
      profileId,
      transaction,
    });
    if (finalInputState.inputSignature !== inputState.inputSignature) {
      const error = new Error(
        "PTRS payment-normalisation inputs changed during calculation",
      );
      error.code = "PTRS_NORMALISATION_INPUT_CHANGED";
      error.statusCode = 409;
      throw error;
    }
    await result.update(
      {
        status: "succeeded",
        summary,
        errorMessage: null,
        completedAt: new Date(),
      },
      { transaction },
    );
    await transaction.commit();
    return {
      source: "calculated",
      normalisationResultId: result.id,
      inputSignature: inputState.inputSignature,
      calculationVersion,
      persisted: Number(persisted || 0),
      summary,
      timings: {
        lookupMs,
        calculationAndPersistenceMs:
          Number(process.hrtime.bigint() - calculationStartedAt) / 1e6,
        totalMs: Number(process.hrtime.bigint() - operationStartedAt) / 1e6,
      },
      limitations: {
        contractualInstalmentIndicatorAvailable: false,
      },
    };
  } catch (error) {
    if (!transaction.finished) await transaction.rollback();
    await recordPaymentNormalisationFailure({
      customerId,
      ptrsId,
      inputState,
      calculationVersion,
      userId,
      error,
    });
    throw error;
  }
}

module.exports = {
  ADJUSTMENT_REASONS,
  DOCUMENT_TYPES,
  PAYMENT_NORMALISATION_VERSION,
  VEOLIA_PAYMENT_TIME_REFERENCE_POLICY,
  buildPaymentNormalisationCte,
  buildPersistedPaymentNormalisationCte,
  buildPersistPaymentNormalisationSql,
  buildPaymentNormalisationInputSignature,
  classifyDocument,
  normalisePaymentRows,
  persistPaymentNormalisationEvidence,
  readPaymentNormalisationInputState,
  requireCurrentPaymentNormalisationResult,
};

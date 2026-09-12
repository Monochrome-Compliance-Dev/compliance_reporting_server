const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { buildStableInputHash } = require("./ptrs.service");
const {
  finishExecutionTiming,
  measureExecutionPhase,
  startExecutionTiming,
} = require("./execution-timing.ptrs.service");

const DOCUMENT_TYPES = Object.freeze({
  INVOICE: "RE",
  INVOICE_KR: "KR",
  VENDOR_CREDIT_MEMO: "KG",
  PAYMENT: "ZP",
  CLEARING_PAYMENT: "KZ",
  EARLY_TRADE_DISCOUNT: "ET",
  ACCOUNTING_ADJUSTMENT: "AB",
  INTERNAL_TRANSFER: "SI",
  PAYABLE_BALANCE: "SA",
  CUSTOMER_CLEARING: "DZ",
  REVERSAL: "ZR",
  SPECIAL_REVERSAL: "$F",
});

const NORMALISATION_EPSILON = 0.005;
const PAYMENT_NORMALISATION_VERSION = "veolia-payment-normalisation-v8";
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
      WHERE normalisation_role IN (
        'ET', 'KG', 'AB', 'SI', 'OBLIGATION_OFFSET',
        'SETTLEMENT_REVERSAL'
      )
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

function approximatelyZero(value) {
  return Math.abs(Number(value) || 0) <= NORMALISATION_EPSILON;
}

function sumRows(rows, selector = (row) => row.signedAmount) {
  return rows.reduce((total, row) => total + Number(selector(row) || 0), 0);
}

function hasBothDirections(rows) {
  return (
    rows.some((row) => row.signedAmount > NORMALISATION_EPSILON) &&
    rows.some((row) => row.signedAmount < -NORMALISATION_EPSILON)
  );
}

function isExactOppositePair(rows) {
  return (
    rows.length === 2 &&
    hasBothDirections(rows) &&
    approximatelyZero(sumRows(rows))
  );
}

function classifyClearingEvent(groupRows) {
  const rows = Array.isArray(groupRows) ? groupRows : [];
  const prefixes = new Set(
    rows.map((row) => row.clearingPrefix).filter(Boolean),
  );
  const prefix = prefixes.size === 1 ? Array.from(prefixes)[0] : null;
  const documentTypes = new Set(rows.map((row) => row.sourceDocumentType));
  const signedBalance = sumRows(rows);
  const reconciled = approximatelyZero(signedBalance);
  const exactPair = isExactOppositePair(rows);
  const allDocumentType = (documentType) =>
    rows.length > 0 &&
    rows.every((row) => row.sourceDocumentType === documentType);
  const result = (kind, pattern) => ({
    kind,
    pattern,
    family: prefix ? `${prefix}*` : "UNKNOWN",
    signedBalance,
    reconciled,
    documentTypes,
  });

  if (!prefix || prefixes.size !== 1) {
    return result("UNKNOWN", "UNSUPPORTED_CLEARING_EVENT");
  }

  if (
    prefix === "2" &&
    rows.every((row) => row.clearingDocument.startsWith("200")) &&
    documentTypes.has("AB") &&
    reconciled &&
    approximatelyZero(
      sumRows(rows.filter((row) => row.sourceDocumentType === "AB")) +
        sumRows(rows.filter((row) => row.sourceDocumentType !== "AB")),
    )
  ) {
    return result("NON_PAYMENT", "SUPPORTED_200_AB_REVERSAL_ADJUSTMENT");
  }

  if (
    prefix === "3" &&
    rows.every((row) => row.clearingDocument.startsWith("300")) &&
    documentTypes.has("DZ") &&
    reconciled &&
    approximatelyZero(
      sumRows(rows.filter((row) => row.sourceDocumentType === "DZ")) +
        sumRows(rows.filter((row) => row.sourceDocumentType !== "DZ")),
    )
  ) {
    return result("NON_PAYMENT", "SUPPORTED_300_DZ_CUSTOMER_CLEARING");
  }

  if (
    exactPair &&
    ["ZP", "KG", "KR", "ET"].some((documentType) =>
      allDocumentType(documentType),
    )
  ) {
    return result(
      "NON_PAYMENT",
      `EXACT_${rows[0].sourceDocumentType}_REVERSAL_PAIR`,
    );
  }

  if (
    exactPair &&
    allDocumentType("ZR") &&
    rows.some((row) => /REVERSE\s+D\.DEBIT/i.test(row.descriptionReference))
  ) {
    return result("NON_PAYMENT", "SUPPORTED_ZR_DIRECT_DEBIT_REVERSAL");
  }

  if (
    prefix === "1" &&
    allDocumentType("SA") &&
    reconciled &&
    hasBothDirections(rows)
  ) {
    return result("NON_PAYMENT", "SUPPORTED_1_SA_BALANCE_TRANSFER");
  }

  if (prefix === "9" && exactPair && allDocumentType("$F")) {
    return result("NON_PAYMENT", "SUPPORTED_9_DOLLAR_F_REVERSAL_PAIR");
  }

  const obligationTypes = new Set(["RE", "KR", "SA"]);
  const hasPayableObligation = rows.some(
    (row) =>
      obligationTypes.has(row.sourceDocumentType) &&
      row.signedAmount < -NORMALISATION_EPSILON,
  );
  const allowedSettlementTypes = new Set([
    "RE",
    "KR",
    "SA",
    "ZP",
    "KZ",
    "ET",
    "KG",
    "AB",
    "SI",
  ]);
  const hasOnlySupportedTypes = rows.every((row) =>
    allowedSettlementTypes.has(row.sourceDocumentType),
  );
  if (
    prefix === "5" &&
    rows.some(
      (row) =>
        row.sourceDocumentType === "ZP" &&
        row.signedAmount > NORMALISATION_EPSILON,
    ) &&
    hasPayableObligation &&
    hasOnlySupportedTypes
  ) {
    return result("SETTLEMENT", "SUPPORTED_5_ZP_SUPPLIER_SETTLEMENT");
  }
  if (
    prefix === "4" &&
    rows.some(
      (row) =>
        row.sourceDocumentType === "KZ" &&
        row.signedAmount > NORMALISATION_EPSILON,
    ) &&
    hasPayableObligation &&
    hasOnlySupportedTypes
  ) {
    return result("SETTLEMENT", "SUPPORTED_4_KZ_SUPPLIER_SETTLEMENT");
  }

  if (
    prefix === "5" &&
    reconciled &&
    documentTypes.has("AB") &&
    documentTypes.has("ZP") &&
    rows.every((row) => ["AB", "ZP"].includes(row.sourceDocumentType))
  ) {
    return result("NON_PAYMENT", "SUPPORTED_AB_ZP_NO_OBLIGATION");
  }

  return result("UNKNOWN", "UNSUPPORTED_CLEARING_EVENT");
}

function classifyDocument(row, clearingEvent = null) {
  if (firstValue(row, ["semanticKind", "semantic_kind"]) === "direct_payment") {
    return "DIRECT_PAYMENT";
  }
  if (!clearingEvent || clearingEvent.kind === "UNKNOWN") return "UNRECOGNISED";
  if (clearingEvent.kind === "NON_PAYMENT") return "NON_PAYMENT";

  const documentType =
    row.sourceDocumentType ||
    normaliseText(
      firstValue(row, ["documentType", "document_type"]),
    ).toUpperCase();
  const signed = Number(
    row.signedAmount ??
      signedAmount(
        firstValue(row, ["paymentAmount", "payment_amount", "amount"]),
      ),
  );
  if (["RE", "KR", "SA"].includes(documentType)) {
    return signed < -NORMALISATION_EPSILON ? "INVOICE" : "OBLIGATION_OFFSET";
  }
  if (
    (clearingEvent.family === "5*" && documentType === "ZP") ||
    (clearingEvent.family === "4*" && documentType === "KZ")
  ) {
    return signed > NORMALISATION_EPSILON ? "PAYMENT" : "SETTLEMENT_REVERSAL";
  }
  if (["ET", "KG", "AB", "SI"].includes(documentType)) return documentType;
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

function normalisePaymentRows(inputRows, options = {}) {
  const rows = (Array.isArray(inputRows) ? inputRows : []).map((row) => ({
    ...row,
    id: normaliseText(row?.id || row?.stageRowId),
    groupKey: sourceGroupKey(row),
    clearingDocument: normaliseText(
      firstValue(row, ["clearingDocument", "clearing_document"]),
    ),
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
    economicReference: normaliseText(
      firstValue(row, [
        "economicReference",
        "economic_reference",
        "descriptionReference",
        "description_reference",
        "description",
        "reference",
        "Reference",
      ]) || row?.data?.Reference,
    ).toLowerCase(),
    descriptionReference: normaliseText(
      firstValue(row, [
        "descriptionReference",
        "description_reference",
        "description",
      ]),
    ),
  }));

  const obligations = [];
  const adjustments = [];
  const payments = [];
  const nonPaymentRows = [];
  const observations = [];
  const exceptions = [];
  const clearingReconciliations = [];
  const rowsByGroup = new Map();

  for (const sourceRow of rows) {
    if (
      firstValue(sourceRow, ["semanticKind", "semantic_kind"]) ===
      "direct_payment"
    ) {
      sourceRow.role = "DIRECT_PAYMENT";
      continue;
    }
    if (!sourceRow.groupKey) {
      sourceRow.role = "UNRECOGNISED";
      exceptions.push({
        code: "MAPPING_EXCEPTION",
        sourceStageRowId: sourceRow.id,
        field: "normalisation_group_key",
        amount: sourceRow.amount,
      });
      continue;
    }
    const group = rowsByGroup.get(sourceRow.groupKey) || [];
    group.push(sourceRow);
    rowsByGroup.set(sourceRow.groupKey, group);
  }

  const rowOrder = (left, right) =>
    compareRows(left, right, [
      "paymentDate",
      "payment_date",
      "invoiceIssueDate",
      "invoice_issue_date",
    ]);

  for (const [groupKey, groupRows] of rowsByGroup.entries()) {
    groupRows.sort(rowOrder);
    const event = classifyClearingEvent(groupRows);
    for (const sourceRow of groupRows) {
      sourceRow.clearingEventKind = event.kind;
      sourceRow.clearingPattern = event.pattern;
      sourceRow.role = classifyDocument(sourceRow, event);
    }

    const buildSourceEvidence = () =>
      groupRows.map((sourceRow) => ({
        stageRowId: sourceRow.id,
        rowNo: sourceRow.rowNo ?? sourceRow.row_no ?? null,
        documentType: sourceRow.sourceDocumentType,
        normalisationRole: sourceRow.role,
        signedAmount: sourceRow.signedAmount,
      }));
    const buildGroupEvidence = (allocationResolved, semanticEffects = {}) => ({
      normalisationGroupKey: groupKey,
      clearingFamily: event.family,
      clearingPattern: event.pattern,
      reconciled: event.reconciled,
      allocationResolved,
      signedClearingGroupTotal: event.signedBalance,
      sourceAbsoluteAmount: sumRows(groupRows, (row) => row.amount),
      participatingDocumentTypes: Array.from(event.documentTypes).sort(),
      semanticEffects,
    });

    if (event.kind === "NON_PAYMENT") {
      for (const sourceRow of groupRows) {
        sourceRow.reversalOffsetAmount = sourceRow.amount;
        sourceRow.reconciliationCode = event.pattern;
        nonPaymentRows.push(sourceRow);
      }
      clearingReconciliations.push(buildGroupEvidence(true));
      continue;
    }

    if (event.kind === "UNKNOWN") {
      exceptions.push({
        code: "UNSUPPORTED_CLEARING_EVENT",
        sourceStageRowId: groupRows[0]?.id || null,
        groupKey,
        clearingFamily: event.family,
        clearingPattern: event.pattern,
        amount: Math.abs(event.signedBalance),
        signedSourceAmounts: buildSourceEvidence(),
      });
      clearingReconciliations.push(buildGroupEvidence(false));
      continue;
    }

    const neutralisedIds = new Set();
    const neutraliseRows = (rowsToNeutralise, reconciliationCode) => {
      for (const neutralisedRow of rowsToNeutralise) {
        neutralisedIds.add(neutralisedRow.id);
        neutralisedRow.role = "NEUTRALISED";
        neutralisedRow.reversalOffsetAmount = neutralisedRow.amount;
        neutralisedRow.reconciliationCode = reconciliationCode;
        nonPaymentRows.push(neutralisedRow);
      }
    };
    const negativeAbRows = groupRows.filter(
      (sourceRow) =>
        sourceRow.sourceDocumentType === "AB" &&
        sourceRow.signedAmount < -NORMALISATION_EPSILON,
    );
    const positiveInvoiceOffsetRows = groupRows.filter(
      (sourceRow) =>
        ["RE", "KR"].includes(sourceRow.sourceDocumentType) &&
        sourceRow.signedAmount > NORMALISATION_EPSILON,
    );
    if (
      negativeAbRows.length > 0 &&
      positiveInvoiceOffsetRows.length > 0 &&
      approximatelyZero(
        sumRows(negativeAbRows) + sumRows(positiveInvoiceOffsetRows),
      )
    ) {
      neutraliseRows(
        [...negativeAbRows, ...positiveInvoiceOffsetRows],
        "AGGREGATE_AB_OBLIGATION_OFFSET_NEUTRALISATION",
      );
    }
    const pairRows = (leftPredicate, rightPredicate, compatible) => {
      const leftRows = groupRows.filter(
        (sourceRow) =>
          !neutralisedIds.has(sourceRow.id) && leftPredicate(sourceRow),
      );
      const rightRows = groupRows.filter(
        (sourceRow) =>
          !neutralisedIds.has(sourceRow.id) && rightPredicate(sourceRow),
      );
      const candidates = leftRows.flatMap((left) =>
        rightRows
          .filter((right) => compatible(left, right))
          .map((right) => ({ left, right })),
      );
      for (const candidate of candidates) {
        if (
          neutralisedIds.has(candidate.left.id) ||
          neutralisedIds.has(candidate.right.id)
        ) {
          continue;
        }
        const leftMatches = candidates.filter(
          (item) => item.left.id === candidate.left.id,
        );
        const rightMatches = candidates.filter(
          (item) => item.right.id === candidate.right.id,
        );
        if (leftMatches.length !== 1 || rightMatches.length !== 1) continue;
        neutraliseRows(
          [candidate.left, candidate.right],
          "DETERMINISTIC_SIGNED_NEUTRALISATION",
        );
      }
    };
    const sameAmountOppositeSign = (left, right) =>
      approximatelyZero(left.signedAmount + right.signedAmount);
    pairRows(
      (sourceRow) =>
        sourceRow.sourceDocumentType === "AB" &&
        sourceRow.signedAmount < -NORMALISATION_EPSILON,
      (sourceRow) =>
        ["RE", "KR"].includes(sourceRow.sourceDocumentType) &&
        sourceRow.signedAmount > NORMALISATION_EPSILON,
      sameAmountOppositeSign,
    );
    for (const documentType of ["ET", "KG", "KR"]) {
      pairRows(
        (sourceRow) =>
          sourceRow.sourceDocumentType === documentType &&
          sourceRow.signedAmount < -NORMALISATION_EPSILON,
        (sourceRow) =>
          sourceRow.sourceDocumentType === documentType &&
          sourceRow.signedAmount > NORMALISATION_EPSILON,
        (left, right) =>
          sameAmountOppositeSign(left, right) &&
          left.economicReference === right.economicReference,
      );
    }

    const groupObligations = groupRows
      .filter(
        (sourceRow) =>
          sourceRow.role === "INVOICE" && !neutralisedIds.has(sourceRow.id),
      )
      .map((sourceRow) => ({
        ...sourceRow,
        originalAmount: sourceRow.amount,
        adjustedAmount: sourceRow.amount,
        outstandingAmount: sourceRow.amount,
        adjustmentAllocations: [],
        paymentAllocations: [],
      }));
    const groupPayments = groupRows
      .filter(
        (sourceRow) =>
          sourceRow.role === "PAYMENT" && !neutralisedIds.has(sourceRow.id),
      )
      .map((sourceRow) => ({
        ...sourceRow,
        allocatedAmount: 0,
        unmatchedAmount: sourceRow.amount,
        allocations: [],
      }));
    const groupAdjustments = groupRows
      .filter(
        (sourceRow) =>
          [
            "ET",
            "KG",
            "AB",
            "SI",
            "OBLIGATION_OFFSET",
            "SETTLEMENT_REVERSAL",
          ].includes(sourceRow.role) && !neutralisedIds.has(sourceRow.id),
      )
      .map((sourceRow) => ({
        ...sourceRow,
        effectiveAmount:
          sourceRow.signedAmount > NORMALISATION_EPSILON ? sourceRow.amount : 0,
        allocatedAmount: 0,
        reversalOffsetAmount: 0,
        unmatchedAmount: sourceRow.amount,
        allocations: [],
      }));
    obligations.push(...groupObligations);
    payments.push(...groupPayments);
    adjustments.push(...groupAdjustments);

    const remainingByObligation = new Map(
      groupObligations.map((obligation) => [
        obligation.id,
        obligation.adjustedAmount,
      ]),
    );
    const adjustmentPlan = [];
    const unresolvedAdjustments = [];
    for (const adjustment of groupAdjustments.sort(rowOrder)) {
      if (
        adjustment.role === "SETTLEMENT_REVERSAL" ||
        adjustment.signedAmount <= NORMALISATION_EPSILON
      ) {
        unresolvedAdjustments.push(adjustment);
        continue;
      }
      const referenceMatches = adjustment.economicReference
        ? groupObligations.filter(
            (obligation) =>
              obligation.economicReference === adjustment.economicReference,
          )
        : [];
      const candidates =
        adjustment.role === "ET"
          ? referenceMatches
          : adjustment.economicReference
            ? referenceMatches
            : groupObligations.length === 1
              ? groupObligations
              : [];
      if (candidates.length === 0) {
        unresolvedAdjustments.push(adjustment);
        continue;
      }
      let remaining = adjustment.amount;
      const plannedForAdjustment = [];
      for (const obligation of candidates.sort((left, right) =>
        compareRows(left, right, ["invoiceIssueDate", "invoice_issue_date"]),
      )) {
        if (remaining <= NORMALISATION_EPSILON) break;
        const before = remainingByObligation.get(obligation.id) || 0;
        if (before <= NORMALISATION_EPSILON) continue;
        const allocated = Math.min(before, remaining);
        const after = Math.max(0, before - allocated);
        remainingByObligation.set(obligation.id, after);
        plannedForAdjustment.push({
          adjustment,
          obligation,
          amount: allocated,
          before,
          after,
        });
        remaining -= allocated;
      }
      if (remaining > NORMALISATION_EPSILON) {
        for (const planned of plannedForAdjustment) {
          remainingByObligation.set(planned.obligation.id, planned.before);
        }
        unresolvedAdjustments.push(adjustment);
        continue;
      }
      adjustmentPlan.push(...plannedForAdjustment);
    }

    let allocationResolved = unresolvedAdjustments.length === 0;
    if (!allocationResolved) {
      const totalAdjustmentValue = sumRows(groupAdjustments);
      const unresolvedAmount = approximatelyZero(totalAdjustmentValue)
        ? sumRows(unresolvedAdjustments, (adjustment) => adjustment.amount)
        : Math.abs(totalAdjustmentValue);
      exceptions.push({
        code: "UNRESOLVED_ADJUSTMENT_ALLOCATION",
        sourceStageRowId:
          unresolvedAdjustments[0]?.id || groupRows[0]?.id || null,
        groupKey,
        clearingFamily: event.family,
        clearingPattern: event.pattern,
        affectedObligationStageRowIds: groupObligations.map((row) => row.id),
        adjustmentStageRowIds: groupAdjustments.map((row) => row.id),
        settlementStageRowIds: groupPayments.map((row) => row.id),
        signedSourceAmounts: buildSourceEvidence(),
        totalObligationValue: sumRows(
          groupObligations,
          (row) => row.originalAmount,
        ),
        totalAdjustmentValue,
        totalSettlementValue: sumRows(groupPayments, (row) => row.signedAmount),
        unresolvedAmount,
        amount: unresolvedAmount,
        reason:
          "Source evidence does not deterministically attribute every adjustment to one invoice obligation.",
      });
    } else {
      for (const planned of adjustmentPlan) {
        const reasonFamily =
          planned.adjustment.role === "OBLIGATION_OFFSET"
            ? "AB"
            : planned.adjustment.role;
        const reason =
          planned.after <= NORMALISATION_EPSILON
            ? reasonFamily + "_FULL_OFFSET"
            : reasonFamily + "_PARTIAL_OFFSET";
        const allocation = {
          adjustmentStageRowId: planned.adjustment.id,
          invoiceStageRowId: planned.obligation.id,
          adjustmentKind: planned.adjustment.role,
          reasonCode: reason,
          amount: planned.amount,
          obligationBefore: planned.before,
          obligationAfter: planned.after,
        };
        planned.obligation.adjustedAmount = planned.after;
        planned.obligation.outstandingAmount = planned.after;
        planned.obligation.adjustmentAllocations.push(allocation);
        planned.adjustment.allocations.push(allocation);
        planned.adjustment.allocatedAmount += planned.amount;
        planned.adjustment.unmatchedAmount = Math.max(
          0,
          planned.adjustment.amount - planned.adjustment.allocatedAmount,
        );
      }
    }

    const blockedByFaultInjection =
      options.preventAllocationForGroupKeys?.includes(groupKey) ||
      options.shouldPreventAllocation?.({
        groupKey,
        event,
        rows: groupRows,
      }) === true;
    if (blockedByFaultInjection) {
      allocationResolved = false;
      exceptions.push({
        code: "SEMANTIC_ALLOCATION_FAILED",
        sourceStageRowId:
          groupPayments[0]?.id || groupObligations[0]?.id || null,
        groupKey,
        amount: sumRows(groupPayments, (payment) => payment.amount),
      });
    }

    if (allocationResolved) {
      groupPayments.sort(rowOrder);
      groupObligations.sort((left, right) =>
        compareRows(left, right, ["invoiceIssueDate", "invoice_issue_date"]),
      );
      for (const payment of groupPayments) {
        let remaining = payment.amount;
        for (const obligation of groupObligations) {
          if (remaining <= NORMALISATION_EPSILON) break;
          if (obligation.outstandingAmount <= NORMALISATION_EPSILON) continue;
          const before = obligation.outstandingAmount;
          const allocated = Math.min(before, remaining);
          const after = Math.max(0, before - allocated);
          const paymentDate = normaliseText(
            firstValue(payment, ["paymentDate", "payment_date"]),
          );
          const invoiceIssueDate = normaliseText(
            firstValue(obligation, ["invoiceIssueDate", "invoice_issue_date"]),
          );
          const invoiceReceiptDate = normaliseText(
            firstValue(obligation, [
              "invoiceReceiptDate",
              "invoice_receipt_date",
            ]),
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
            partialPayment: after > NORMALISATION_EPSILON,
            finalSettlement: after <= NORMALISATION_EPSILON,
          };
          obligation.outstandingAmount = after;
          obligation.paymentAllocations.push(allocation);
          payment.allocations.push(allocation);
          payment.allocatedAmount += allocated;
          remaining -= allocated;
          const observation = {
            ...allocation,
            reasonCode: allocation.partialPayment ? "PARTIAL_PAYMENT" : null,
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
        if (payment.unmatchedAmount > NORMALISATION_EPSILON) {
          allocationResolved = false;
          exceptions.push({
            code: "UNMATCHED_PAYMENT",
            sourceStageRowId: payment.id,
            originalAmount: payment.amount,
            allocatedAmount: payment.allocatedAmount,
            unmatchedAmount: payment.unmatchedAmount,
            amount: payment.unmatchedAmount,
          });
        }
      }
    }

    clearingReconciliations.push(
      buildGroupEvidence(allocationResolved, {
        adjustmentAllocatedAmount: sumRows(
          groupAdjustments,
          (row) => row.allocatedAmount,
        ),
        adjustmentReversalOffsetAmount: sumRows(
          groupRows,
          (row) => row.reversalOffsetAmount || 0,
        ),
        paymentAllocatedAmount: sumRows(
          groupPayments,
          (row) => row.allocatedAmount,
        ),
      }),
    );
  }

  const sum = (values) => values.reduce((total, value) => total + value, 0);
  return {
    obligations,
    adjustments,
    payments,
    nonPaymentRows,
    observations,
    exceptions,
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
      adjustmentValue: sum(adjustments.map((row) => row.amount)),
      adjustmentAllocatedValue: sum(
        adjustments.map((row) => row.allocatedAmount),
      ),
      adjustmentReversalOffsetValue: sum(
        rows.map((row) => row.reversalOffsetAmount || 0),
      ),
      unmatchedAdjustmentValue: sum(
        adjustments.map((row) => row.unmatchedAmount),
      ),
      paymentValue: sum(payments.map((row) => row.amount)),
      paymentAllocatedValue: sum(payments.map((row) => row.allocatedAmount)),
      unmatchedPaymentValue: sum(payments.map((row) => row.unmatchedAmount)),
      accountingReconciledGroupCount: clearingReconciliations.filter(
        (row) => row.reconciled,
      ).length,
      allocationResolvedGroupCount: clearingReconciliations.filter(
        (row) => row.allocationResolved,
      ).length,
      unexplainedClearingResidualValue: sum(
        clearingReconciliations.map((row) =>
          row.reconciled ? 0 : Math.abs(row.signedClearingGroupTotal),
        ),
      ),
      paymentObservationCount: observations.length,
      partialPaymentCount: observations.filter((row) => row.partialPayment)
        .length,
      finalPaymentCount: observations.filter((row) => row.finalSettlement)
        .length,
      exceptionCount: exceptions.length,
      contractualInstalmentIndicatorAvailable: false,
    },
  };
}
function buildPaymentNormalisationCte() {
  return `
    payment_normalisation_classified_source_rows AS MATERIALIZED (
      SELECT
        s."id", s."semanticKind", s."rowNo", s."paymentDate",
        s."invoiceIssueDate", s."invoiceReceiptDate",
        UPPER(BTRIM(COALESCE(s."documentType", ''))) AS document_type,
        NULLIF(BTRIM(s."data"->>'company_code'), '') AS company_code,
        NULLIF(BTRIM(s."sourceAccountCode"), '') AS source_account_code,
        NULLIF(BTRIM(s."clearingDocument"), '') AS clearing_document,
        LOWER(NULLIF(BTRIM(s."description"), ''))
          AS economic_reference,
        NULLIF(BTRIM(s."description"), '') AS description_reference,
        CASE
          WHEN NULLIF(BTRIM(s."data"->>'company_code'), '') IS NULL
            OR NULLIF(BTRIM(s."sourceAccountCode"), '') IS NULL
            OR NULLIF(BTRIM(s."clearingDocument"), '') IS NULL
            THEN NULL
          ELSE LOWER(
            COALESCE(
              NULLIF(BTRIM(s."sourceGroupScope"), ''),
              'dataset:' || s."datasetId"
            ) || '|' || BTRIM(s."data"->>'company_code')
              || '|' || BTRIM(s."sourceAccountCode")
              || '|' || BTRIM(s."clearingDocument")
          )
        END AS normalisation_group_key,
        COALESCE(s."paymentAmount", 0)::numeric AS signed_amount,
        ABS(COALESCE(s."paymentAmount", 0))::numeric
          AS normalisation_amount
      FROM "tbl_ptrs_stage_row" s
      WHERE s."customerId" = :customerId
        AND s."ptrsId" = :ptrsId
        AND s."deletedAt" IS NULL
    ),
    payment_normalisation_group_facts AS MATERIALIZED (
      SELECT
        normalisation_group_key,
        MIN(clearing_document) AS clearing_document,
        LEFT(MIN(clearing_document), 1) AS clearing_family,
        SUM(signed_amount)::numeric AS signed_balance,
        SUM(normalisation_amount)::numeric AS source_absolute_amount,
        ABS(SUM(signed_amount)) <= 0.005 AS reconciled,
        COUNT(*)::int AS row_count,
        ARRAY_AGG(DISTINCT document_type ORDER BY document_type)
          AS document_types,
        BOOL_OR(signed_amount > 0.005) AS has_positive,
        BOOL_OR(signed_amount < -0.005) AS has_negative,
        BOOL_OR(document_type = 'AB') AS has_ab,
        BOOL_OR(document_type = 'DZ') AS has_dz,
        BOOL_OR(document_type = 'ZP' AND signed_amount > 0.005)
          AS has_positive_zp,
        BOOL_OR(document_type = 'KZ' AND signed_amount > 0.005)
          AS has_positive_kz,
        BOOL_OR(
          document_type IN ('RE', 'KR', 'SA')
          AND signed_amount < -0.005
        ) AS has_payable_obligation,
        BOOL_AND(document_type = 'SA') AS all_sa,
        BOOL_AND(document_type = '$F') AS all_dollar_f,
        BOOL_AND(document_type IN (
          'RE', 'KR', 'SA', 'ZP', 'KZ', 'ET', 'KG', 'AB', 'SI'
        )) AS all_supported_settlement_types,
        BOOL_AND(document_type IN ('AB', 'ZP')) AS all_ab_zp,
        BOOL_OR(
          document_type = 'ZR'
          AND description_reference ~* 'REVERSE[[:space:]]+D[.]DEBIT'
        ) AS has_supported_zr_description
      FROM payment_normalisation_classified_source_rows
      WHERE normalisation_group_key IS NOT NULL
        AND "semanticKind" = 'accounting_event'
      GROUP BY normalisation_group_key
    ),
    payment_normalisation_clearing_events AS MATERIALIZED (
      SELECT facts.*,
        CASE
          WHEN clearing_document LIKE '200%'
            AND has_ab AND reconciled
            THEN 'SUPPORTED_200_AB_REVERSAL_ADJUSTMENT'
          WHEN clearing_document LIKE '300%'
            AND has_dz AND reconciled
            THEN 'SUPPORTED_300_DZ_CUSTOMER_CLEARING'
          WHEN row_count = 2
            AND has_positive AND has_negative AND reconciled
            AND CARDINALITY(document_types) = 1
            AND document_types[1] IN ('ZP', 'KG', 'KR', 'ET')
            THEN 'EXACT_' || document_types[1] || '_REVERSAL_PAIR'
          WHEN row_count = 2
            AND has_positive AND has_negative AND reconciled
            AND document_types = ARRAY['ZR']::text[]
            AND has_supported_zr_description
            THEN 'SUPPORTED_ZR_DIRECT_DEBIT_REVERSAL'
          WHEN clearing_family = '1'
            AND all_sa AND has_positive AND has_negative AND reconciled
            THEN 'SUPPORTED_1_SA_BALANCE_TRANSFER'
          WHEN clearing_family = '9'
            AND row_count = 2 AND all_dollar_f
            AND has_positive AND has_negative AND reconciled
            THEN 'SUPPORTED_9_DOLLAR_F_REVERSAL_PAIR'
          WHEN clearing_family = '5'
            AND all_ab_zp AND has_ab AND has_positive_zp
            AND NOT has_payable_obligation AND reconciled
            THEN 'SUPPORTED_AB_ZP_NO_OBLIGATION'
          WHEN clearing_family = '5'
            AND has_positive_zp AND has_payable_obligation
            AND all_supported_settlement_types
            THEN 'SUPPORTED_5_ZP_SUPPLIER_SETTLEMENT'
          WHEN clearing_family = '4'
            AND has_positive_kz AND has_payable_obligation
            AND all_supported_settlement_types
            THEN 'SUPPORTED_4_KZ_SUPPLIER_SETTLEMENT'
          ELSE 'UNSUPPORTED_CLEARING_EVENT'
        END AS clearing_pattern
      FROM payment_normalisation_group_facts facts
    ),
    payment_normalisation_neutralisation_candidates AS (
      SELECT
        negative."id" AS negative_stage_row_id,
        positive."id" AS positive_stage_row_id,
        COUNT(*) OVER (PARTITION BY negative."id") AS negative_match_count,
        COUNT(*) OVER (PARTITION BY positive."id") AS positive_match_count
      FROM payment_normalisation_classified_source_rows negative
      JOIN payment_normalisation_clearing_events event
        ON event.normalisation_group_key = negative.normalisation_group_key
      JOIN payment_normalisation_classified_source_rows positive
        ON positive.normalisation_group_key =
          negative.normalisation_group_key
       AND positive.signed_amount > 0.005
       AND ABS(negative.signed_amount + positive.signed_amount) <= 0.005
       AND (
         (
           negative.document_type = 'AB'
           AND positive.document_type IN ('RE', 'KR')
         )
         OR (
           negative.document_type IN ('ET', 'KG', 'KR')
           AND positive.document_type = negative.document_type
           AND COALESCE(positive.economic_reference, '') =
             COALESCE(negative.economic_reference, '')
         )
       )
      WHERE event.clearing_pattern IN (
          'SUPPORTED_5_ZP_SUPPLIER_SETTLEMENT',
          'SUPPORTED_4_KZ_SUPPLIER_SETTLEMENT'
        )
        AND negative.signed_amount < -0.005
    ),
    payment_normalisation_aggregate_ab_offset_groups AS MATERIALIZED (
      SELECT source.normalisation_group_key
      FROM payment_normalisation_classified_source_rows source
      JOIN payment_normalisation_clearing_events event
        ON event.normalisation_group_key = source.normalisation_group_key
      WHERE event.clearing_pattern IN (
          'SUPPORTED_5_ZP_SUPPLIER_SETTLEMENT',
          'SUPPORTED_4_KZ_SUPPLIER_SETTLEMENT'
        )
        AND (
          (document_type = 'AB' AND signed_amount < -0.005)
          OR (
            document_type IN ('RE', 'KR') AND signed_amount > 0.005
          )
        )
      GROUP BY source.normalisation_group_key
      HAVING BOOL_OR(document_type = 'AB' AND signed_amount < -0.005)
        AND BOOL_OR(
          document_type IN ('RE', 'KR') AND signed_amount > 0.005
        )
        AND ABS(SUM(signed_amount)) <= 0.005
    ),
    payment_normalisation_neutralised_rows AS MATERIALIZED (
      SELECT negative_stage_row_id AS stage_row_id
      FROM payment_normalisation_neutralisation_candidates
      WHERE negative_match_count = 1 AND positive_match_count = 1
      UNION
      SELECT positive_stage_row_id
      FROM payment_normalisation_neutralisation_candidates
      WHERE negative_match_count = 1 AND positive_match_count = 1
      UNION
      SELECT source."id"
      FROM payment_normalisation_classified_source_rows source
      JOIN payment_normalisation_aggregate_ab_offset_groups aggregate_offset
        ON aggregate_offset.normalisation_group_key =
          source.normalisation_group_key
      WHERE (
          source.document_type = 'AB' AND source.signed_amount < -0.005
        ) OR (
          source.document_type IN ('RE', 'KR')
          AND source.signed_amount > 0.005
        )
    ),
    payment_normalisation_source_rows AS MATERIALIZED (
      SELECT source.*, event.clearing_family, event.clearing_pattern,
        event.signed_balance AS clearing_group_signed_balance,
        event.reconciled,
        CASE
          WHEN source."semanticKind" = 'direct_payment' THEN 'DIRECT_PAYMENT'
          WHEN source.normalisation_group_key IS NULL THEN 'UNKNOWN'
          WHEN event.clearing_pattern = 'UNSUPPORTED_CLEARING_EVENT'
            THEN 'UNKNOWN'
          WHEN event.clearing_pattern IN (
            'SUPPORTED_5_ZP_SUPPLIER_SETTLEMENT',
            'SUPPORTED_4_KZ_SUPPLIER_SETTLEMENT'
          ) THEN 'SETTLEMENT'
          ELSE 'NON_PAYMENT'
        END AS clearing_event_kind,
        CASE
          WHEN neutralised.stage_row_id IS NOT NULL THEN 'NEUTRALISED'
          WHEN source."semanticKind" = 'direct_payment' THEN 'DIRECT_PAYMENT'
          WHEN source.normalisation_group_key IS NULL THEN 'UNRECOGNISED'
          WHEN event.clearing_pattern = 'UNSUPPORTED_CLEARING_EVENT'
            THEN 'UNRECOGNISED'
          WHEN event.clearing_pattern NOT IN (
            'SUPPORTED_5_ZP_SUPPLIER_SETTLEMENT',
            'SUPPORTED_4_KZ_SUPPLIER_SETTLEMENT'
          ) THEN 'NON_PAYMENT'
          WHEN source.document_type IN ('RE', 'KR', 'SA')
            AND source.signed_amount < -0.005 THEN 'INVOICE'
          WHEN event.clearing_family = '5'
            AND source.document_type = 'ZP'
            AND source.signed_amount > 0.005 THEN 'PAYMENT'
          WHEN event.clearing_family = '4'
            AND source.document_type = 'KZ'
            AND source.signed_amount > 0.005 THEN 'PAYMENT'
          WHEN source.document_type IN ('RE', 'KR', 'SA')
            AND source.signed_amount >= -0.005 THEN 'OBLIGATION_OFFSET'
          WHEN (
            event.clearing_family = '5' AND source.document_type = 'ZP'
          ) OR (
            event.clearing_family = '4' AND source.document_type = 'KZ'
          ) THEN 'SETTLEMENT_REVERSAL'
          WHEN source.document_type IN ('ET', 'KG', 'AB', 'SI')
            THEN source.document_type
          ELSE 'UNRECOGNISED'
        END AS normalisation_role
      FROM payment_normalisation_classified_source_rows source
      LEFT JOIN payment_normalisation_clearing_events event
        ON event.normalisation_group_key = source.normalisation_group_key
      LEFT JOIN payment_normalisation_neutralised_rows neutralised
        ON neutralised.stage_row_id = source."id"
    ),
    payment_normalisation_obligations AS MATERIALIZED (
      SELECT source.*,
        ROW_NUMBER() OVER (
          PARTITION BY normalisation_group_key
          ORDER BY "invoiceIssueDate" NULLS LAST, "rowNo", "id"
        ) AS obligation_sequence,
        normalisation_amount AS original_obligation_amount
      FROM payment_normalisation_source_rows source
      WHERE normalisation_role = 'INVOICE'
    ),
    payment_normalisation_obligation_counts AS MATERIALIZED (
      SELECT normalisation_group_key, COUNT(*)::int AS obligation_count
      FROM payment_normalisation_obligations
      GROUP BY normalisation_group_key
    ),
    payment_normalisation_adjustments AS MATERIALIZED (
      SELECT source.*,
        ROW_NUMBER() OVER (
          PARTITION BY normalisation_group_key
          ORDER BY "paymentDate" NULLS LAST,
            "invoiceIssueDate" NULLS LAST, "rowNo", "id"
        ) AS adjustment_sequence
      FROM payment_normalisation_source_rows source
      WHERE normalisation_role IN (
        'ET', 'KG', 'AB', 'SI', 'OBLIGATION_OFFSET',
        'SETTLEMENT_REVERSAL'
      )
    ),
    payment_normalisation_adjustment_target_sets AS MATERIALIZED (
      SELECT adjustment.*,
        CASE
          WHEN adjustment.economic_reference IS NOT NULL
            THEN adjustment.normalisation_group_key || '|reference:'
              || adjustment.economic_reference
          ELSE adjustment.normalisation_group_key || '|single-obligation'
        END AS allocation_group_key
      FROM payment_normalisation_adjustments adjustment
      JOIN payment_normalisation_obligation_counts counts
        ON counts.normalisation_group_key =
          adjustment.normalisation_group_key
      WHERE adjustment.signed_amount > 0.005
        AND adjustment.normalisation_role <> 'SETTLEMENT_REVERSAL'
        AND (
          (
            adjustment.economic_reference IS NOT NULL AND EXISTS (
              SELECT 1
              FROM payment_normalisation_obligations obligation
              WHERE obligation.normalisation_group_key =
                adjustment.normalisation_group_key
                AND obligation.economic_reference =
                  adjustment.economic_reference
            )
          )
          OR (
            adjustment.normalisation_role <> 'ET'
            AND adjustment.economic_reference IS NULL
            AND counts.obligation_count = 1
          )
        )
    ),
    payment_normalisation_adjustment_ranges AS (
      SELECT target."id" AS adjustment_stage_row_id,
        target.normalisation_group_key,
        target.economic_reference,
        target.allocation_group_key,
        target.adjustment_sequence,
        target.normalisation_amount,
        COALESCE(SUM(normalisation_amount) OVER (
          PARTITION BY allocation_group_key
          ORDER BY adjustment_sequence, "id"
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0)::numeric AS adjustment_start,
        SUM(normalisation_amount) OVER (
          PARTITION BY allocation_group_key
          ORDER BY adjustment_sequence, "id"
          ROWS UNBOUNDED PRECEDING
        )::numeric AS adjustment_end
      FROM payment_normalisation_adjustment_target_sets target
    ),
    payment_normalisation_obligation_adjustment_ranges AS (
      SELECT target.allocation_group_key,
        obligation."id" AS invoice_stage_row_id,
        obligation.normalisation_amount AS adjustment_base_obligation_amount,
        COALESCE(SUM(obligation.normalisation_amount) OVER (
          PARTITION BY target.allocation_group_key
          ORDER BY obligation.obligation_sequence, obligation."id"
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0)::numeric AS obligation_start,
        SUM(obligation.normalisation_amount) OVER (
          PARTITION BY target.allocation_group_key
          ORDER BY obligation.obligation_sequence, obligation."id"
          ROWS UNBOUNDED PRECEDING
        )::numeric AS obligation_end
      FROM payment_normalisation_obligations obligation
      JOIN (
        SELECT DISTINCT allocation_group_key, normalisation_group_key,
          economic_reference
        FROM payment_normalisation_adjustment_target_sets
      ) target
        ON target.normalisation_group_key =
          obligation.normalisation_group_key
       AND (
         target.economic_reference IS NULL
         OR target.economic_reference = obligation.economic_reference
       )
    ),
    payment_normalisation_adjustment_allocations_raw AS (
      SELECT
        adjustment.adjustment_stage_row_id,
        obligation.invoice_stage_row_id,
        adjustment.adjustment_sequence,
        GREATEST(
          0,
          LEAST(obligation.obligation_end, adjustment.adjustment_end)
            - GREATEST(
              obligation.obligation_start,
              adjustment.adjustment_start
            )
        )::numeric AS allocated_amount,
        obligation.adjustment_base_obligation_amount
      FROM payment_normalisation_adjustment_ranges adjustment
      JOIN payment_normalisation_obligation_adjustment_ranges obligation
        ON obligation.allocation_group_key = adjustment.allocation_group_key
       AND obligation.obligation_end > adjustment.adjustment_start
       AND obligation.obligation_start < adjustment.adjustment_end
    ),
    payment_normalisation_adjustment_allocations_pre AS MATERIALIZED (
      SELECT allocation.*,
        source.normalisation_role AS adjustment_kind,
        allocation.adjustment_base_obligation_amount
          - COALESCE(SUM(allocation.allocated_amount) OVER (
            PARTITION BY allocation.invoice_stage_row_id
            ORDER BY allocation.adjustment_sequence,
              allocation.adjustment_stage_row_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
          ), 0)::numeric AS obligation_before,
        GREATEST(
          0,
          allocation.adjustment_base_obligation_amount
            - SUM(allocation.allocated_amount) OVER (
              PARTITION BY allocation.invoice_stage_row_id
              ORDER BY allocation.adjustment_sequence,
                allocation.adjustment_stage_row_id
              ROWS UNBOUNDED PRECEDING
            )
        )::numeric AS obligation_after
      FROM payment_normalisation_adjustment_allocations_raw allocation
      JOIN payment_normalisation_adjustments source
        ON source."id" = allocation.adjustment_stage_row_id
      WHERE allocation.allocated_amount > 0.005
    ),
    payment_normalisation_adjustment_source_totals_pre AS MATERIALIZED (
      SELECT adjustment."id" AS adjustment_stage_row_id,
        adjustment.normalisation_group_key,
        adjustment.signed_amount,
        adjustment.normalisation_amount AS original_amount,
        COALESCE(SUM(allocation.allocated_amount), 0)::numeric
          AS allocated_amount,
        GREATEST(
          0,
          adjustment.normalisation_amount
            - COALESCE(SUM(allocation.allocated_amount), 0)
        )::numeric AS unmatched_amount
      FROM payment_normalisation_adjustments adjustment
      LEFT JOIN payment_normalisation_adjustment_allocations_pre allocation
        ON allocation.adjustment_stage_row_id = adjustment."id"
      GROUP BY adjustment."id", adjustment.normalisation_group_key,
        adjustment.signed_amount, adjustment.normalisation_amount
    ),
    payment_normalisation_unresolved_adjustment_groups AS MATERIALIZED (
      SELECT
        adjustment.normalisation_group_key,
        MIN(adjustment.adjustment_stage_row_id) AS source_stage_row_id,
        CASE
          WHEN ABS(SUM(adjustment.signed_amount)) <= 0.005
            THEN SUM(adjustment.original_amount)
          ELSE ABS(SUM(adjustment.signed_amount))
        END::numeric AS unresolved_amount,
        SUM(adjustment.signed_amount)::numeric AS total_adjustment_value
      FROM payment_normalisation_adjustment_source_totals_pre adjustment
      GROUP BY adjustment.normalisation_group_key
      HAVING BOOL_OR(adjustment.unmatched_amount > 0.005)
    ),
    payment_normalisation_adjustment_allocations AS MATERIALIZED (
      SELECT allocation.*,
        CASE
          WHEN allocation.adjustment_kind = 'OBLIGATION_OFFSET'
            THEN CASE WHEN allocation.obligation_after <= 0.005
              THEN 'AB_FULL_OFFSET' ELSE 'AB_PARTIAL_OFFSET' END
          ELSE CASE WHEN allocation.obligation_after <= 0.005
            THEN allocation.adjustment_kind || '_FULL_OFFSET'
            ELSE allocation.adjustment_kind || '_PARTIAL_OFFSET' END
        END AS reason_code
      FROM payment_normalisation_adjustment_allocations_pre allocation
      JOIN payment_normalisation_adjustments adjustment
        ON adjustment."id" = allocation.adjustment_stage_row_id
      LEFT JOIN payment_normalisation_unresolved_adjustment_groups unresolved
        ON unresolved.normalisation_group_key =
          adjustment.normalisation_group_key
      WHERE unresolved.normalisation_group_key IS NULL
        AND allocation.allocated_amount > 0.005
    ),
    payment_normalisation_adjustment_source_totals AS MATERIALIZED (
      SELECT adjustment."id" AS adjustment_stage_row_id,
        adjustment.normalisation_amount AS original_amount,
        COALESCE(SUM(allocation.allocated_amount), 0)::numeric
          AS allocated_amount,
        CASE
          WHEN unresolved.normalisation_group_key IS NOT NULL
            THEN adjustment.normalisation_amount
          ELSE GREATEST(
            0,
            adjustment.normalisation_amount
              - COALESCE(SUM(allocation.allocated_amount), 0)
          )
        END::numeric AS unmatched_amount,
        0::numeric AS reversal_offset_amount
      FROM payment_normalisation_adjustments adjustment
      LEFT JOIN payment_normalisation_adjustment_allocations allocation
        ON allocation.adjustment_stage_row_id = adjustment."id"
      LEFT JOIN payment_normalisation_unresolved_adjustment_groups unresolved
        ON unresolved.normalisation_group_key =
          adjustment.normalisation_group_key
      GROUP BY adjustment."id", adjustment.normalisation_amount,
        unresolved.normalisation_group_key
    ),
    payment_normalisation_adjustment_totals AS (
      SELECT invoice_stage_row_id,
        SUM(allocated_amount)::numeric AS allocated_amount
      FROM payment_normalisation_adjustment_allocations
      GROUP BY invoice_stage_row_id
    ),
    payment_normalisation_adjusted_obligations AS MATERIALIZED (
      SELECT obligation.*,
        GREATEST(
          0,
          obligation.normalisation_amount
            - COALESCE(adjustment.allocated_amount, 0)
        )::numeric AS adjusted_obligation_amount,
        COALESCE(adjustment.allocated_amount, 0)::numeric
          AS adjustment_allocated_amount
      FROM payment_normalisation_obligations obligation
      LEFT JOIN payment_normalisation_adjustment_totals adjustment
        ON adjustment.invoice_stage_row_id = obligation."id"
    ),
    payment_normalisation_obligation_payment_ranges AS (
      SELECT obligation.*,
        COALESCE(SUM(adjusted_obligation_amount) OVER (
          PARTITION BY obligation.normalisation_group_key
          ORDER BY obligation.obligation_sequence, obligation."id"
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0)::numeric AS obligation_start,
        SUM(adjusted_obligation_amount) OVER (
          PARTITION BY obligation.normalisation_group_key
          ORDER BY obligation.obligation_sequence, obligation."id"
          ROWS UNBOUNDED PRECEDING
        )::numeric AS obligation_end
      FROM payment_normalisation_adjusted_obligations obligation
      LEFT JOIN payment_normalisation_unresolved_adjustment_groups unresolved
        ON unresolved.normalisation_group_key =
          obligation.normalisation_group_key
      WHERE adjusted_obligation_amount > 0.005
        AND unresolved.normalisation_group_key IS NULL
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
          PARTITION BY payment.normalisation_group_key
          ORDER BY payment.payment_sequence, payment."id"
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0)::numeric AS payment_start,
        SUM(normalisation_amount) OVER (
          PARTITION BY payment.normalisation_group_key
          ORDER BY payment.payment_sequence, payment."id"
          ROWS UNBOUNDED PRECEDING
        )::numeric AS payment_end
      FROM payment_normalisation_payments payment
      LEFT JOIN payment_normalisation_unresolved_adjustment_groups unresolved
        ON unresolved.normalisation_group_key =
          payment.normalisation_group_key
      WHERE unresolved.normalisation_group_key IS NULL
    ),
    payment_normalisation_payment_allocations_raw AS (
      SELECT
        obligation."id" AS invoice_stage_row_id,
        payment."id" AS payment_stage_row_id,
        payment.payment_sequence,
        GREATEST(
          0,
          LEAST(obligation.obligation_end, payment.payment_end)
            - GREATEST(obligation.obligation_start, payment.payment_start)
        )::numeric AS allocated_amount,
        obligation.adjusted_obligation_amount,
        obligation.original_obligation_amount,
        obligation.adjustment_allocated_amount,
        payment."paymentDate" AS settlement_payment_date,
        payment.normalisation_amount AS source_payment_amount
      FROM payment_normalisation_obligation_payment_ranges obligation
      JOIN payment_normalisation_payment_ranges payment
        ON payment.normalisation_group_key =
          obligation.normalisation_group_key
       AND obligation.obligation_end > payment.payment_start
       AND obligation.obligation_start < payment.payment_end
    ),
    payment_normalisation_payment_allocations AS MATERIALIZED (
      SELECT allocation.*,
        allocation.adjusted_obligation_amount
          - COALESCE(SUM(allocation.allocated_amount) OVER (
            PARTITION BY allocation.invoice_stage_row_id
            ORDER BY allocation.payment_sequence,
              allocation.payment_stage_row_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
          ), 0)::numeric AS obligation_before_payment,
        GREATEST(
          0,
          allocation.adjusted_obligation_amount
            - SUM(allocation.allocated_amount) OVER (
              PARTITION BY allocation.invoice_stage_row_id
              ORDER BY allocation.payment_sequence,
                allocation.payment_stage_row_id
              ROWS UNBOUNDED PRECEDING
            )
        )::numeric AS obligation_after_payment
      FROM payment_normalisation_payment_allocations_raw allocation
      WHERE allocation.allocated_amount > 0.005
    ),
    payment_normalisation_payment_source_totals AS MATERIALIZED (
      SELECT payment."id" AS payment_stage_row_id,
        payment.normalisation_group_key,
        payment.normalisation_amount AS original_amount,
        COALESCE(SUM(allocation.allocated_amount), 0)::numeric
          AS allocated_amount,
        GREATEST(
          0,
          payment.normalisation_amount
            - COALESCE(SUM(allocation.allocated_amount), 0)
        )::numeric AS unmatched_amount
      FROM payment_normalisation_payments payment
      LEFT JOIN payment_normalisation_payment_allocations allocation
        ON allocation.payment_stage_row_id = payment."id"
      GROUP BY payment."id", payment.normalisation_group_key,
        payment.normalisation_amount
    ),
    payment_normalisation_unmatched_payment_groups AS MATERIALIZED (
      SELECT normalisation_group_key
      FROM payment_normalisation_payment_source_totals
      WHERE unmatched_amount > 0.005
      GROUP BY normalisation_group_key
    ),
    payment_normalisation_unresolved_group_source_evidence AS MATERIALIZED (
      SELECT source.normalisation_group_key,
        JSONB_AGG(
          JSONB_BUILD_OBJECT(
            'stageRowId', source."id",
            'rowNo', source."rowNo",
            'documentType', source.document_type,
            'normalisationRole', source.normalisation_role,
            'signedAmount', source.signed_amount
          ) ORDER BY source."rowNo", source."id"
        ) AS source_rows,
        JSONB_AGG(source."id" ORDER BY source."id")
          FILTER (WHERE source.normalisation_role = 'INVOICE')
          AS obligation_stage_row_ids,
        JSONB_AGG(source."id" ORDER BY source."id")
          FILTER (WHERE source.normalisation_role IN (
            'ET', 'KG', 'AB', 'SI', 'OBLIGATION_OFFSET',
            'SETTLEMENT_REVERSAL'
          )) AS adjustment_stage_row_ids,
        JSONB_AGG(source."id" ORDER BY source."id")
          FILTER (WHERE source.normalisation_role = 'PAYMENT')
          AS settlement_stage_row_ids,
        COALESCE(SUM(source.normalisation_amount)
          FILTER (WHERE source.normalisation_role = 'INVOICE'), 0)::numeric
          AS total_obligation_value,
        COALESCE(SUM(source.signed_amount)
          FILTER (WHERE source.normalisation_role = 'PAYMENT'), 0)::numeric
          AS total_settlement_value
      FROM payment_normalisation_source_rows source
      JOIN payment_normalisation_unresolved_adjustment_groups unresolved
        ON unresolved.normalisation_group_key =
          source.normalisation_group_key
      GROUP BY source.normalisation_group_key
    ),
    payment_normalisation_unresolved_adjustment_evidence AS MATERIALIZED (
      SELECT unresolved.normalisation_group_key,
        JSONB_BUILD_OBJECT(
          'normalisationGroupKey', unresolved.normalisation_group_key,
          'clearingFamily', event.clearing_family || '*',
          'clearingPattern', event.clearing_pattern,
          'affectedObligationStageRowIds', COALESCE(
            evidence.obligation_stage_row_ids, '[]'::jsonb
          ),
          'adjustmentStageRowIds', COALESCE(
            evidence.adjustment_stage_row_ids, '[]'::jsonb
          ),
          'settlementStageRowIds', COALESCE(
            evidence.settlement_stage_row_ids, '[]'::jsonb
          ),
          'signedSourceAmounts', evidence.source_rows,
          'totalObligationValue', evidence.total_obligation_value,
          'totalAdjustmentValue', unresolved.total_adjustment_value,
          'totalSettlementValue', evidence.total_settlement_value,
          'unresolvedAmount', unresolved.unresolved_amount,
          'reason',
            'Source evidence does not deterministically attribute every adjustment to one invoice obligation.'
        ) AS evidence
      FROM payment_normalisation_unresolved_adjustment_groups unresolved
      JOIN payment_normalisation_clearing_events event
        ON event.normalisation_group_key =
          unresolved.normalisation_group_key
      JOIN payment_normalisation_unresolved_group_source_evidence evidence
        ON evidence.normalisation_group_key =
          unresolved.normalisation_group_key
    ),
    payment_normalisation_clearing_reconciliations AS MATERIALIZED (
      SELECT event.normalisation_group_key,
        event.clearing_family || '*' AS clearing_family,
        event.clearing_pattern,
        event.reconciled,
        CASE
          WHEN event.clearing_pattern = 'UNSUPPORTED_CLEARING_EVENT'
            THEN false
          WHEN event.clearing_pattern NOT IN (
            'SUPPORTED_5_ZP_SUPPLIER_SETTLEMENT',
            'SUPPORTED_4_KZ_SUPPLIER_SETTLEMENT'
          ) THEN true
          WHEN unresolved.normalisation_group_key IS NOT NULL THEN false
          WHEN unmatched_payment.normalisation_group_key IS NOT NULL
            THEN false
          ELSE true
        END AS allocation_resolved,
        event.signed_balance AS signed_clearing_group_total,
        CASE WHEN event.reconciled
          THEN 0 ELSE event.signed_balance END::numeric
          AS final_unexplained_signed_residual,
        event.source_absolute_amount,
        event.document_types AS participating_document_types,
        unresolved_evidence.evidence AS unresolved_adjustment_evidence
      FROM payment_normalisation_clearing_events event
      LEFT JOIN payment_normalisation_unresolved_adjustment_groups unresolved
        ON unresolved.normalisation_group_key =
          event.normalisation_group_key
      LEFT JOIN payment_normalisation_unmatched_payment_groups
          unmatched_payment
        ON unmatched_payment.normalisation_group_key =
          event.normalisation_group_key
      LEFT JOIN payment_normalisation_unresolved_adjustment_evidence
          unresolved_evidence
        ON unresolved_evidence.normalisation_group_key =
          event.normalisation_group_key
    ),
    payment_normalisation_exceptions AS MATERIALIZED (
      SELECT source."id" AS source_stage_row_id,
        'MAPPING_EXCEPTION'::text AS reason_code,
        source.normalisation_amount AS amount
      FROM payment_normalisation_source_rows source
      WHERE source."semanticKind" = 'accounting_event'
        AND source.normalisation_group_key IS NULL
      UNION ALL
      SELECT MIN(source."id") AS source_stage_row_id,
        'UNSUPPORTED_CLEARING_EVENT'::text AS reason_code,
        ABS(event.signed_balance)::numeric AS amount
      FROM payment_normalisation_clearing_events event
      JOIN payment_normalisation_source_rows source
        ON source.normalisation_group_key =
          event.normalisation_group_key
      WHERE event.clearing_pattern = 'UNSUPPORTED_CLEARING_EVENT'
      GROUP BY event.normalisation_group_key, event.signed_balance
      UNION ALL
      SELECT unresolved.source_stage_row_id,
        'UNRESOLVED_ADJUSTMENT_ALLOCATION'::text AS reason_code,
        unresolved.unresolved_amount AS amount
      FROM payment_normalisation_unresolved_adjustment_groups unresolved
      UNION ALL
      SELECT payment.payment_stage_row_id,
        'UNMATCHED_PAYMENT'::text AS reason_code,
        payment.unmatched_amount AS amount
      FROM payment_normalisation_payment_source_totals payment
      LEFT JOIN payment_normalisation_unresolved_adjustment_groups unresolved
        ON unresolved.normalisation_group_key =
          payment.normalisation_group_key
      WHERE payment.unmatched_amount > 0.005
        AND unresolved.normalisation_group_key IS NULL
      UNION ALL
      SELECT allocation.invoice_stage_row_id,
        'MAPPING_EXCEPTION'::text AS reason_code,
        allocation.allocated_amount AS amount
      FROM payment_normalisation_payment_allocations allocation
      JOIN payment_normalisation_source_rows invoice
        ON invoice."id" = allocation.invoice_stage_row_id
      WHERE (
          invoice."invoiceIssueDate" IS NULL
          AND invoice."invoiceReceiptDate" IS NULL
        )
        OR allocation.settlement_payment_date IS NULL
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
        SELECT allocation.invoice_stage_row_id,
          JSONB_AGG(DISTINCT allocation.reason_code) AS reason_codes
        FROM payment_normalisation_adjustment_allocations allocation
        GROUP BY allocation.invoice_stage_row_id
      ),
      invoice_payment_evidence AS MATERIALIZED (
        SELECT allocation.invoice_stage_row_id,
          SUM(allocation.allocated_amount)::numeric AS allocated_amount,
          JSONB_AGG(
            allocation.payment_stage_row_id
            ORDER BY allocation.payment_sequence
          ) AS payment_stage_row_ids
        FROM payment_normalisation_payment_allocations allocation
        GROUP BY allocation.invoice_stage_row_id
      ),
      payment_source_evidence AS MATERIALIZED (
        SELECT allocation.payment_stage_row_id,
          JSONB_AGG(
            allocation.invoice_stage_row_id
            ORDER BY allocation.invoice_stage_row_id
          ) AS invoice_stage_row_ids,
          BOOL_OR(allocation.obligation_after_payment > 0.005)
            AS partial_payment,
          BOOL_OR(allocation.obligation_after_payment <= 0.005)
            AS final_settlement
        FROM payment_normalisation_payment_allocations allocation
        GROUP BY allocation.payment_stage_row_id
      ),
      exception_codes AS MATERIALIZED (
        SELECT source_stage_row_id, MIN(reason_code) AS exception_code
        FROM payment_normalisation_exceptions
        GROUP BY source_stage_row_id
      ),
      evidence AS (
        SELECT source."id" AS stage_row_id,
          JSONB_BUILD_OBJECT(
            'role', source.normalisation_role,
            'signedAmount', source.signed_amount,
            'originalAmount', source.normalisation_amount,
            'originalObligationAmount',
              obligation.original_obligation_amount,
            'adjustedObligationAmount',
              obligation.adjusted_obligation_amount,
            'adjustmentAllocatedAmount',
              obligation.adjustment_allocated_amount,
            'paymentAllocatedAmount', COALESCE(
              invoice_payment.allocated_amount,
              payment_totals.allocated_amount
            ),
            'outstandingAmount', CASE
              WHEN obligation."id" IS NULL THEN NULL
              ELSE GREATEST(
                0,
                obligation.adjusted_obligation_amount
                  - COALESCE(invoice_payment.allocated_amount, 0)
              )
            END,
            'unmatchedAmount', COALESCE(
              adjustment_totals.unmatched_amount,
              payment_totals.unmatched_amount
            ),
            'reversalOffsetAmount', CASE
              WHEN source.normalisation_role IN (
                'NON_PAYMENT', 'NEUTRALISED'
              ) THEN source.normalisation_amount
              ELSE COALESCE(adjustment_totals.reversal_offset_amount, 0)
            END,
            'exceptionCode', exception_codes.exception_code,
            'adjustmentReasonCodes',
              COALESCE(adjustment_evidence.reason_codes, '[]'::jsonb),
            'paymentStageRowIds',
              COALESCE(invoice_payment.payment_stage_row_ids, '[]'::jsonb),
            'invoiceStageRowIds', CASE
              WHEN source.normalisation_role <> 'PAYMENT' THEN '[]'::jsonb
              ELSE COALESCE(
                payment_evidence.invoice_stage_row_ids,
                '[]'::jsonb
              )
            END,
            'partialPayment', CASE
              WHEN source.normalisation_role <> 'PAYMENT' THEN NULL
              ELSE COALESCE(payment_evidence.partial_payment, false)
            END,
            'finalSettlement', CASE
              WHEN source.normalisation_role <> 'PAYMENT' THEN NULL
              ELSE COALESCE(payment_evidence.final_settlement, false)
            END,
            'classificationBasis', CASE
              WHEN source.normalisation_role <> 'PAYMENT' THEN NULL
              WHEN source.document_type = 'KZ'
                THEN 'outstanding_obligation_after_clearing_settlement'
              ELSE 'outstanding_obligation_after_zp'
            END,
            'contractualInstalmentIndicatorAvailable', false,
            'clearingEvent', CASE
              WHEN reconciliation.normalisation_group_key IS NULL THEN NULL
              ELSE JSONB_BUILD_OBJECT(
                'clearingFamily', reconciliation.clearing_family,
                'clearingPattern', reconciliation.clearing_pattern,
                'reconciled', reconciliation.reconciled,
                'allocationResolved', reconciliation.allocation_resolved,
                'signedClearingGroupTotal',
                  reconciliation.signed_clearing_group_total,
                'finalUnexplainedSignedResidual',
                  reconciliation.final_unexplained_signed_residual,
                'sourceAbsoluteAmount',
                  reconciliation.source_absolute_amount,
                'participatingDocumentTypes',
                  reconciliation.participating_document_types
              )
            END,
            'unresolvedAdjustmentAllocation', CASE
              WHEN exception_codes.exception_code =
                'UNRESOLVED_ADJUSTMENT_ALLOCATION'
                THEN reconciliation.unresolved_adjustment_evidence
              ELSE NULL
            END,
            'normalisationResultId', :normalisationResultId,
            'inputSignature', :inputSignature,
            'calculationVersion', :calculationVersion
          ) AS evidence
        FROM payment_normalisation_source_rows source
        LEFT JOIN payment_normalisation_adjusted_obligations obligation
          ON obligation."id" = source."id"
        LEFT JOIN invoice_adjustment_evidence adjustment_evidence
          ON adjustment_evidence.invoice_stage_row_id = source."id"
        LEFT JOIN invoice_payment_evidence invoice_payment
          ON invoice_payment.invoice_stage_row_id = source."id"
        LEFT JOIN payment_source_evidence payment_evidence
          ON payment_evidence.payment_stage_row_id = source."id"
        LEFT JOIN payment_normalisation_adjustment_source_totals
            adjustment_totals
          ON adjustment_totals.adjustment_stage_row_id = source."id"
        LEFT JOIN payment_normalisation_payment_source_totals payment_totals
          ON payment_totals.payment_stage_row_id = source."id"
        LEFT JOIN exception_codes
          ON exception_codes.source_stage_row_id = source."id"
        LEFT JOIN payment_normalisation_clearing_reconciliations
            reconciliation
          ON reconciliation.normalisation_group_key =
            source.normalisation_group_key
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
          invoice_payment.allocated_amount,
          CASE WHEN obligation."id" IS NOT NULL THEN
            GREATEST(
              0,
              obligation.adjusted_obligation_amount
                - COALESCE(invoice_payment.allocated_amount, 0)
            )
            ELSE NULL END,
          COALESCE(
            adjustment_totals.unmatched_amount,
            payment_totals.unmatched_amount
          ),
          CASE
            WHEN source.normalisation_role IN (
              'NON_PAYMENT', 'NEUTRALISED'
            ) THEN source.normalisation_amount
            ELSE adjustment_totals.reversal_offset_amount
          END,
          exception_codes.exception_code,
          now()
        FROM payment_normalisation_source_rows source
        LEFT JOIN payment_normalisation_adjusted_obligations obligation
          ON obligation."id" = source."id"
        LEFT JOIN invoice_payment_evidence invoice_payment
          ON invoice_payment.invoice_stage_row_id = source."id"
        LEFT JOIN payment_normalisation_adjustment_source_totals
            adjustment_totals
          ON adjustment_totals.adjustment_stage_row_id = source."id"
        LEFT JOIN payment_normalisation_payment_source_totals payment_totals
          ON payment_totals.payment_stage_row_id = source."id"
        LEFT JOIN exception_codes
          ON exception_codes.source_stage_row_id = source."id"
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
              OR (
                invoice."invoiceIssueDate" IS NULL
                AND invoice."invoiceReceiptDate" IS NULL
              )
            THEN 'MAPPING_EXCEPTION' ELSE NULL
          END AS mapping_exception_code
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
        SET "meta" = JSONB_SET(
              COALESCE(stage_row."meta", '{}'::jsonb),
              '{paymentNormalisation}',
              evidence.evidence,
              true
            ),
            "data" = CASE
              WHEN evidence.evidence->>'role' = 'PAYMENT' THEN JSONB_SET(
                COALESCE(stage_row."data", '{}'::jsonb),
                '{partial_payment}',
                TO_JSONB(COALESCE(
                  (evidence.evidence->>'partialPayment')::boolean,
                  false
                )),
                true
              )
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
        (SELECT COUNT(*)::int FROM payment_normalisation_source_rows)
          AS "startingStageCount",
        (SELECT COALESCE(SUM(normalisation_amount), 0)::numeric
         FROM payment_normalisation_source_rows)
          AS "startingAbsoluteValue",
        (SELECT COUNT(*)::int FROM payment_normalisation_obligations)
          AS "invoiceObligationCount",
        (SELECT COALESCE(SUM(normalisation_amount), 0)::numeric
         FROM payment_normalisation_obligations)
          AS "originalObligationValue",
        (SELECT COALESCE(SUM(adjusted_obligation_amount), 0)::numeric
         FROM payment_normalisation_adjusted_obligations)
          AS "adjustedObligationValue",
        0::numeric AS "directionalObligationOffsetValue",
        0::numeric AS "unmatchedDirectionalObligationOffsetValue",
        (SELECT COUNT(*)::int FROM payment_normalisation_payments)
          AS "paymentEventCount",
        (SELECT COALESCE(SUM(normalisation_amount), 0)::numeric
         FROM payment_normalisation_payments)
          AS "paymentEventValue",
        (SELECT COUNT(*)::int
         FROM payment_normalisation_payment_allocations)
          AS "paymentAllocationCount",
        (SELECT COUNT(*)::int
         FROM payment_normalisation_payment_allocations
         WHERE obligation_after_payment > 0.005)
          AS "partialPaymentCount",
        (SELECT COUNT(*)::int
         FROM payment_normalisation_payment_allocations
         WHERE obligation_after_payment <= 0.005)
          AS "finalSettlementCount",
        0::numeric AS "creditValue",
        0::numeric AS "creditAllocatedValue",
        0::numeric AS "refundValue",
        0::numeric AS "refundAllocatedValue",
        (SELECT COALESCE(SUM(normalisation_amount), 0)::numeric
         FROM payment_normalisation_adjustments
         WHERE normalisation_role = 'ET')
          AS "earlyTradeDiscountValue",
        (SELECT COALESCE(SUM(allocated_amount), 0)::numeric
         FROM payment_normalisation_adjustment_allocations
         WHERE adjustment_kind = 'ET')
          AS "earlyTradeDiscountAllocatedValue",
        (SELECT COUNT(*)::int
         FROM payment_normalisation_adjustment_allocations
         WHERE adjustment_kind = 'ET')
          AS "earlyTradeMatchCount",
        (SELECT COALESCE(SUM(normalisation_amount), 0)::numeric
         FROM payment_normalisation_adjustments
         WHERE normalisation_role = 'KG')
          AS "vendorCreditMemoValue",
        (SELECT COALESCE(SUM(allocated_amount), 0)::numeric
         FROM payment_normalisation_adjustment_allocations
         WHERE adjustment_kind = 'KG')
          AS "vendorCreditMemoAllocatedValue",
        (SELECT COUNT(*)::int
         FROM payment_normalisation_clearing_reconciliations
         WHERE reconciled)
          AS "accountingReconciledGroupCount",
        (SELECT COUNT(*)::int
         FROM payment_normalisation_clearing_reconciliations
         WHERE allocation_resolved)
          AS "allocationResolvedGroupCount",
        (SELECT COALESCE(
           SUM(ABS(final_unexplained_signed_residual)),
           0
         )::numeric
         FROM payment_normalisation_clearing_reconciliations)
          AS "unexplainedClearingResidualValue",
        (SELECT COALESCE(SUM(
           CASE
             WHEN normalisation_role IN ('NON_PAYMENT', 'NEUTRALISED')
               THEN normalisation_amount
             ELSE 0
           END
         ), 0)::numeric
         FROM payment_normalisation_source_rows)
          AS "adjustmentReversalOffsetValue",
        (SELECT COALESCE(SUM(amount), 0)::numeric
         FROM payment_normalisation_exceptions
         WHERE reason_code = 'UNRESOLVED_ADJUSTMENT_ALLOCATION')
          AS "unmatchedAdjustmentValue",
        (SELECT COUNT(*)::int
         FROM payment_normalisation_exceptions
         WHERE reason_code = 'UNRESOLVED_ADJUSTMENT_ALLOCATION')
          AS "unmatchedAdjustmentExceptionCount",
        (SELECT COUNT(*)::int FROM payment_normalisation_exceptions)
          AS "exceptionCount"
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
      else
        await db.PtrsPaymentNormalisationResult.create(values, { transaction });
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
  const operationTiming = startExecutionTiming();
  const phaseTimings = {};
  const measure = (name, run) =>
    measureExecutionPhase({ timings: phaseTimings, name, run });
  let transaction;
  let inputState;
  try {
    transaction = await measure("transactionAcquire", () =>
      beginTransactionWithCustomerContext(customerId),
    );
    const lookupStartedAt = process.hrtime.bigint();
    inputState = await measure("initialInputStateRead", () =>
      readPaymentNormalisationInputState({
        customerId,
        ptrsId,
        profileId,
        transaction,
      }),
    );
    const identity = `${customerId}:${ptrsId}`;
    await measure("advisoryLockWait", () =>
      acquirePaymentNormalisationLock({ transaction, identity }),
    );
    const lockedInputState = await measure("lockedInputStateRead", () =>
      readPaymentNormalisationInputState({
        customerId,
        ptrsId,
        profileId,
        transaction,
      }),
    );
    if (lockedInputState.inputSignature !== inputState.inputSignature) {
      const error = new Error(
        "PTRS payment-normalisation inputs changed while calculation was starting",
      );
      error.code = "PTRS_NORMALISATION_INPUT_CHANGED";
      error.statusCode = 409;
      throw error;
    }
    inputState = lockedInputState;
    let result = await measure("resultLookup", () =>
      db.PtrsPaymentNormalisationResult.findOne({
        where: {
          customerId,
          ptrsId,
          inputSignature: inputState.inputSignature,
          calculationVersion,
        },
        transaction,
      }),
    );
    const lookupMs = Number(process.hrtime.bigint() - lookupStartedAt) / 1e6;
    if (result?.status === "succeeded") {
      await measure("commit", () => transaction.commit());
      const totalTiming = finishExecutionTiming(operationTiming);
      return {
        source: "persisted",
        normalisationResultId: result.id,
        inputSignature: inputState.inputSignature,
        calculationVersion,
        persisted: 0,
        summary: result.summary || {},
        timings: {
          startedAt: totalTiming.startedAt,
          finishedAt: totalTiming.finishedAt,
          lookupMs,
          calculationAndPersistenceMs: 0,
          totalMs: totalTiming.elapsedMs,
          phases: phaseTimings,
        },
        limitations: {
          contractualInstalmentIndicatorAvailable: false,
        },
      };
    }
    if (result) {
      await measure("cleanupNormalisationRows", () =>
        db.PtrsPaymentNormalisationRow.destroy({
          where: { normalisationResultId: result.id },
          transaction,
        }),
      );
      await measure("cleanupAllocations", () =>
        db.PtrsPaymentNormalisationAllocation.destroy({
          where: { normalisationResultId: result.id },
          transaction,
        }),
      );
      await measure("cleanupExceptions", () =>
        db.PtrsPaymentNormalisationException.destroy({
          where: { normalisationResultId: result.id },
          transaction,
        }),
      );
      await measure("resultReset", () =>
        result.update(
          {
            status: "calculating",
            summary: null,
            errorMessage: null,
            createdBy: userId || result.createdBy || null,
            startedAt: new Date(),
            completedAt: null,
          },
          { transaction },
        ),
      );
    } else {
      result = await measure("resultCreate", () =>
        db.PtrsPaymentNormalisationResult.create(
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
        ),
      );
    }
    const calculationStartedAt = process.hrtime.bigint();
    const sql = buildPersistPaymentNormalisationSql();
    const resultRows = await measure("materialisationStatement", () =>
      db.sequelize.query(sql, {
        transaction,
        replacements: {
          customerId,
          ptrsId,
          normalisationResultId: result.id,
          inputSignature: inputState.inputSignature,
          calculationVersion,
        },
        type: db.sequelize.QueryTypes.SELECT,
      }),
    );
    const materialised = resultRows?.[0] || {};
    const { persisted, ...summary } = materialised;
    const finalInputState = await measure("finalInputStateRead", () =>
      readPaymentNormalisationInputState({
        customerId,
        ptrsId,
        profileId,
        transaction,
      }),
    );
    if (finalInputState.inputSignature !== inputState.inputSignature) {
      const error = new Error(
        "PTRS payment-normalisation inputs changed during calculation",
      );
      error.code = "PTRS_NORMALISATION_INPUT_CHANGED";
      error.statusCode = 409;
      throw error;
    }
    await measure("resultSummaryUpdate", () =>
      result.update(
        {
          status: "succeeded",
          summary,
          errorMessage: null,
          completedAt: new Date(),
        },
        { transaction },
      ),
    );
    await measure("commit", () => transaction.commit());
    const totalTiming = finishExecutionTiming(operationTiming);
    return {
      source: "calculated",
      normalisationResultId: result.id,
      inputSignature: inputState.inputSignature,
      calculationVersion,
      persisted: Number(persisted || 0),
      summary,
      timings: {
        startedAt: totalTiming.startedAt,
        finishedAt: totalTiming.finishedAt,
        lookupMs,
        calculationAndPersistenceMs:
          Number(process.hrtime.bigint() - calculationStartedAt) / 1e6,
        totalMs: totalTiming.elapsedMs,
        phases: phaseTimings,
      },
      limitations: {
        contractualInstalmentIndicatorAvailable: false,
      },
    };
  } catch (error) {
    if (transaction && !transaction.finished) await transaction.rollback();
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
  DOCUMENT_TYPES,
  PAYMENT_NORMALISATION_VERSION,
  VEOLIA_PAYMENT_TIME_REFERENCE_POLICY,
  buildPaymentNormalisationCte,
  buildPersistedPaymentNormalisationCte,
  buildPersistPaymentNormalisationSql,
  buildPaymentNormalisationInputSignature,
  classifyClearingEvent,
  classifyDocument,
  normalisePaymentRows,
  persistPaymentNormalisationEvidence,
  readPaymentNormalisationInputState,
  requireCurrentPaymentNormalisationResult,
};

jest.mock("@/db/database", () => ({
  Ptrs: { findOne: jest.fn() },
  PtrsExecutionRun: { findOne: jest.fn() },
  PtrsPaymentNormalisationResult: {
    findOne: jest.fn(),
    create: jest.fn(),
  },
  PtrsPaymentNormalisationRow: { destroy: jest.fn() },
  PtrsPaymentNormalisationAllocation: { destroy: jest.fn() },
  PtrsPaymentNormalisationException: { destroy: jest.fn() },
  sequelize: {
    QueryTypes: { SELECT: "SELECT" },
    query: jest.fn(),
  },
}));
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(),
}));

const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const {
  buildPersistedPaymentNormalisationCte,
  buildPaymentNormalisationInputSignature,
  normalisePaymentRows,
  persistPaymentNormalisationEvidence,
  requireCurrentPaymentNormalisationResult,
} = require("./payment-normalisation.ptrs.service");

function row(id, documentType, paymentAmount, overrides = {}) {
  return {
    id,
    rowNo: Number(id.replace(/\D/g, "")) || 1,
    datasetId: "dataset-1",
    sourceGroupScope: "ledger-1",
    companyCode: "1000",
    sourceAccountCode: "supplier-1",
    invoiceReferenceNumber: "reference-1",
    description: "reference-1",
    clearingDocument: "clear-1",
    documentType,
    paymentAmount,
    invoiceIssueDate: "2026-01-01",
    invoiceReceiptDate: "2026-01-02",
    paymentDate: "2026-01-10",
    ...overrides,
  };
}

describe("PTRS payment normalisation", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    db.Ptrs.findOne.mockResolvedValue({
      id: "ptrs-1",
      profileId: "profile-1",
      normalisationInputRevision: "7",
    });
    db.PtrsExecutionRun.findOne.mockResolvedValue({
      id: "stage-run-1",
      profileId: "profile-1",
      inputHash: "a".repeat(64),
      finishedAt: new Date("2026-09-09T00:00:00Z"),
    });
  });

  test("projects result-keyed typed allocation fields for downstream consumers", () => {
    const sql = buildPersistedPaymentNormalisationCte();
    expect(sql).toContain(
      `allocation."normalisationResultId" AS normalisation_result_id`,
    );
    expect(sql).toContain(
      `allocation."normalisationGroupKey" AS normalisation_group_key`,
    );
    expect(sql).toContain(`allocation."companyCode" AS company_code`);
    expect(sql).toContain(
      `WHERE allocation."normalisationResultId" = :normalisationResultId`,
    );
    expect(sql).toContain(
      `WHERE persisted."normalisationResultId" = :normalisationResultId`,
    );
  });

  test("classifies a supported 200* AB event before row document types", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -100, { clearingDocument: "200001" }),
      row("ab-2", "AB", 100, { clearingDocument: "200001" }),
    ]);

    expect(result.obligations).toHaveLength(0);
    expect(result.adjustments).toHaveLength(0);
    expect(result.observations).toHaveLength(0);
    expect(result.nonPaymentRows).toHaveLength(2);
    expect(result.clearingReconciliations[0]).toMatchObject({
      clearingPattern: "SUPPORTED_200_AB_REVERSAL_ADJUSTMENT",
      reconciled: true,
      allocationResolved: true,
    });
  });

  test("classifies a supported 300* DZ event as customer-side activity", () => {
    const result = normalisePaymentRows([
      row("dz-1", "DZ", -75, { clearingDocument: "300001" }),
      row("ab-2", "AB", 75, { clearingDocument: "300001" }),
    ]);

    expect(result.obligations).toHaveLength(0);
    expect(result.payments).toHaveLength(0);
    expect(result.observations).toHaveLength(0);
    expect(result.clearingReconciliations[0].clearingPattern).toBe(
      "SUPPORTED_300_DZ_CUSTOMER_CLEARING",
    );
  });

  test.each(["RE", "KR"])(
    "settles a simple %s obligation with ZP",
    (documentType) => {
      const result = normalisePaymentRows([
        row("invoice-1", documentType, -100, {
          clearingDocument: "500001",
        }),
        row("zp-2", "ZP", 100, { clearingDocument: "500001" }),
      ]);

      expect(result.obligations).toEqual([
        expect.objectContaining({
          id: "invoice-1",
          sourceDocumentType: documentType,
        }),
      ]);
      expect(result.observations).toEqual([
        expect.objectContaining({
          invoiceStageRowId: "invoice-1",
          paymentStageRowId: "zp-2",
          amount: 100,
          finalSettlement: true,
          paymentTimeDays: 9,
        }),
      ]);
      expect(result.exceptions).toHaveLength(0);
    },
  );

  test.each(["RE", "KR"])(
    "settles a simple %s obligation with KZ",
    (documentType) => {
      const result = normalisePaymentRows([
        row("invoice-1", documentType, -125, {
          clearingDocument: "400001",
        }),
        row("kz-2", "KZ", 125, { clearingDocument: "400001" }),
      ]);

      expect(result.observations).toEqual([
        expect.objectContaining({
          invoiceStageRowId: "invoice-1",
          paymentStageRowId: "kz-2",
          amount: 125,
          classificationBasis:
            "outstanding_obligation_after_clearing_settlement",
        }),
      ]);
      expect(result.exceptions).toHaveLength(0);
    },
  );

  test.each([
    ["ZP", "500002"],
    ["KZ", "400002"],
  ])(
    "treats SA as payable in an evidenced %s settlement",
    (anchor, clearingDocument) => {
      const result = normalisePaymentRows([
        row("sa-1", "SA", -90, { clearingDocument }),
        row("payment-2", anchor, 90, { clearingDocument }),
      ]);

      expect(result.obligations).toEqual([
        expect.objectContaining({ id: "sa-1", sourceDocumentType: "SA" }),
      ]);
      expect(result.observations).toEqual([
        expect.objectContaining({
          invoiceStageRowId: "sa-1",
          paymentStageRowId: "payment-2",
          amount: 90,
        }),
      ]);
    },
  );

  test("matches ET to only the invoice with the same economic reference", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -100, {
        clearingDocument: "500003",
        description: "invoice-a",
      }),
      row("re-2", "RE", -100, {
        clearingDocument: "500003",
        description: "invoice-b",
        invoiceIssueDate: "2026-01-02",
      }),
      row("et-3", "ET", 20, {
        clearingDocument: "500003",
        description: "invoice-b",
      }),
      row("zp-4", "ZP", 180, { clearingDocument: "500003" }),
    ]);

    expect(result.obligations).toEqual([
      expect.objectContaining({ id: "re-1", adjustedAmount: 100 }),
      expect.objectContaining({ id: "re-2", adjustedAmount: 80 }),
    ]);
    expect(result.adjustments[0].allocations).toEqual([
      expect.objectContaining({
        invoiceStageRowId: "re-2",
        reasonCode: "ET_PARTIAL_OFFSET",
        amount: 20,
      }),
    ]);
    expect(result.observations.map((item) => item.amount)).toEqual([100, 80]);
  });

  test("matches ET across the obligation rows for one Veolia business Reference", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -20, {
        clearingDocument: "5000031",
        description: "shared-reference",
      }),
      row("re-2", "RE", -80, {
        clearingDocument: "5000031",
        description: "shared-reference",
        invoiceIssueDate: "2026-01-02",
      }),
      row("et-3", "ET", 30, {
        clearingDocument: "5000031",
        description: "shared-reference",
        invoiceReferenceNumber: "different-document-number",
      }),
      row("zp-4", "ZP", 70, { clearingDocument: "5000031" }),
    ]);

    expect(result.adjustments[0].allocations).toEqual([
      expect.objectContaining({ invoiceStageRowId: "re-1", amount: 20 }),
      expect.objectContaining({ invoiceStageRowId: "re-2", amount: 10 }),
    ]);
    expect(result.obligations).toEqual([
      expect.objectContaining({ id: "re-1", adjustedAmount: 0 }),
      expect.objectContaining({ id: "re-2", adjustedAmount: 70 }),
    ]);
    expect(result.observations).toEqual([
      expect.objectContaining({ invoiceStageRowId: "re-2", amount: 70 }),
    ]);
    expect(result.exceptions).toHaveLength(0);
  });

  test("matches KG to only the invoice with deterministic source evidence", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -100, {
        clearingDocument: "500004",
        description: "invoice-a",
      }),
      row("kr-2", "KR", -100, {
        clearingDocument: "500004",
        description: "invoice-b",
      }),
      row("kg-3", "KG", 25, {
        clearingDocument: "500004",
        description: "invoice-a",
      }),
      row("zp-4", "ZP", 175, { clearingDocument: "500004" }),
    ]);

    expect(result.obligations).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: "re-1", adjustedAmount: 75 }),
        expect.objectContaining({ id: "kr-2", adjustedAmount: 100 }),
      ]),
    );
    expect(result.adjustments[0].allocations[0]).toMatchObject({
      invoiceStageRowId: "re-1",
      reasonCode: "KG_PARTIAL_OFFSET",
    });
    expect(result.exceptions).toHaveLength(0);
  });

  test("refuses to spread a group-level KG across multiple obligations", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -100, {
        clearingDocument: "500005",
        description: "invoice-a",
      }),
      row("re-2", "RE", -100, {
        clearingDocument: "500005",
        description: "invoice-b",
      }),
      row("kg-3", "KG", 20, {
        clearingDocument: "500005",
        description: null,
      }),
      row("zp-4", "ZP", 180, { clearingDocument: "500005" }),
    ]);

    expect(result.observations).toHaveLength(0);
    expect(result.adjustments[0].allocatedAmount).toBe(0);
    expect(result.exceptions).toContainEqual(
      expect.objectContaining({
        code: "UNRESOLVED_ADJUSTMENT_ALLOCATION",
        affectedObligationStageRowIds: ["re-1", "re-2"],
        adjustmentStageRowIds: ["kg-3"],
        settlementStageRowIds: ["zp-4"],
        totalObligationValue: 200,
        totalAdjustmentValue: 20,
        totalSettlementValue: 180,
        unresolvedAmount: 20,
      }),
    );
    expect(result.clearingReconciliations[0]).toMatchObject({
      reconciled: true,
      allocationResolved: false,
    });
  });

  test("treats AB and ZP without an invoice obligation as non-payment", () => {
    const result = normalisePaymentRows([
      row("ab-1", "AB", -100, { clearingDocument: "500006" }),
      row("zp-2", "ZP", 100, { clearingDocument: "500006" }),
    ]);

    expect(result.obligations).toHaveLength(0);
    expect(result.payments).toHaveLength(0);
    expect(result.observations).toHaveLength(0);
    expect(result.clearingReconciliations[0].clearingPattern).toBe(
      "SUPPORTED_AB_ZP_NO_OBLIGATION",
    );
  });

  test("neutralises the RP11 AB and positive RE pair before settlement", () => {
    const result = normalisePaymentRows([
      row("ab-1", "AB", -1194, { clearingDocument: "500007" }),
      row("re-2", "RE", -175.56, {
        clearingDocument: "500007",
        description: "genuine-invoice",
      }),
      row("re-3", "RE", 1194, { clearingDocument: "500007" }),
      row("zp-4", "ZP", 175.56, { clearingDocument: "500007" }),
    ]);

    expect(result.nonPaymentRows).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: "ab-1", role: "NEUTRALISED" }),
        expect.objectContaining({ id: "re-3", role: "NEUTRALISED" }),
      ]),
    );
    expect(result.obligations).toEqual([
      expect.objectContaining({ id: "re-2", originalAmount: 175.56 }),
    ]);
    expect(result.observations).toEqual([
      expect.objectContaining({
        invoiceStageRowId: "re-2",
        paymentStageRowId: "zp-4",
        amount: 175.56,
      }),
    ]);
    expect(result.exceptions).toHaveLength(0);
  });

  test("applies a positive RE to its aggregate business-Reference obligation without exact amount pairing", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -80, {
        clearingDocument: "5000071",
        description: "shared-reference",
      }),
      row("re-2", "RE", -70, {
        clearingDocument: "5000071",
        description: "shared-reference",
        invoiceIssueDate: "2026-01-02",
      }),
      row("offset-3", "RE", 120, {
        clearingDocument: "5000071",
        description: "shared-reference",
      }),
      row("zp-4", "ZP", 30, { clearingDocument: "5000071" }),
    ]);

    expect(result.adjustments[0]).toMatchObject({
      id: "offset-3",
      role: "OBLIGATION_OFFSET",
      allocatedAmount: 120,
      unmatchedAmount: 0,
    });
    expect(result.adjustments[0].allocations).toEqual([
      expect.objectContaining({ invoiceStageRowId: "re-1", amount: 80 }),
      expect.objectContaining({ invoiceStageRowId: "re-2", amount: 40 }),
    ]);
    expect(result.observations).toEqual([
      expect.objectContaining({
        invoiceStageRowId: "re-2",
        amount: 30,
        finalSettlement: true,
      }),
    ]);
    expect(result.exceptions).toHaveLength(0);
  });

  test("retains a genuine partial settlement and outstanding obligation", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -100, { clearingDocument: "5000072" }),
      row("zp-2", "ZP", 60, { clearingDocument: "5000072" }),
    ]);

    expect(result.observations).toEqual([
      expect.objectContaining({
        invoiceStageRowId: "re-1",
        paymentStageRowId: "zp-2",
        amount: 60,
        obligationBefore: 100,
        obligationAfter: 40,
        partialPayment: true,
        finalSettlement: false,
        reasonCode: "PARTIAL_PAYMENT",
      }),
    ]);
    expect(result.obligations[0]).toMatchObject({ outstandingAmount: 40 });
    expect(result.reconciliation.partialPaymentCount).toBe(1);
  });

  test("reconciles AUSGRID while refusing arbitrary SI allocation", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -91626.5, {
        clearingDocument: "500008",
        description: "invoice-a",
      }),
      row("re-2", "RE", -299.39, {
        clearingDocument: "500008",
        description: "invoice-b",
      }),
      row("si-3", "SI", -442.64, {
        clearingDocument: "500008",
        description: null,
      }),
      row("si-4", "SI", 4000.22, {
        clearingDocument: "500008",
        description: null,
      }),
      row("zp-5", "ZP", 88368.31, { clearingDocument: "500008" }),
    ]);

    expect(result.observations).toHaveLength(0);
    expect(result.exceptions).toContainEqual(
      expect.objectContaining({
        code: "UNRESOLVED_ADJUSTMENT_ALLOCATION",
        totalObligationValue: 91925.89,
        totalAdjustmentValue: 3557.58,
        totalSettlementValue: 88368.31,
        unresolvedAmount: 3557.58,
      }),
    );
    expect(result.clearingReconciliations[0]).toMatchObject({
      reconciled: true,
      allocationResolved: false,
      signedClearingGroupTotal: 0,
    });
  });

  test("treats an exact ZP-only pair as a reversal", () => {
    const result = normalisePaymentRows([
      row("zp-1", "ZP", -75, { clearingDocument: "500009" }),
      row("zp-2", "ZP", 75, { clearingDocument: "500009" }),
    ]);

    expect(result.observations).toHaveLength(0);
    expect(result.payments).toHaveLength(0);
    expect(result.exceptions).toHaveLength(0);
    expect(result.clearingReconciliations[0].clearingPattern).toBe(
      "EXACT_ZP_REVERSAL_PAIR",
    );
  });

  test.each(["KG", "KR", "ET"])(
    "treats an exact %s pair as a reversal",
    (documentType) => {
      const result = normalisePaymentRows([
        row("negative-1", documentType, -45, {
          clearingDocument: "500010",
        }),
        row("positive-2", documentType, 45, {
          clearingDocument: "500010",
        }),
      ]);

      expect(result.observations).toHaveLength(0);
      expect(result.exceptions).toHaveLength(0);
      expect(result.clearingReconciliations[0].clearingPattern).toBe(
        "EXACT_" + documentType + "_REVERSAL_PAIR",
      );
    },
  );

  test("requires the observed description evidence for a ZR reversal", () => {
    const result = normalisePaymentRows([
      row("zr-1", "ZR", -30, {
        clearingDocument: "500011",
        description: "REVERSE D.DEBIT",
      }),
      row("zr-2", "ZR", 30, {
        clearingDocument: "500011",
        description: "Direct debit",
      }),
    ]);

    expect(result.observations).toHaveLength(0);
    expect(result.exceptions).toHaveLength(0);
    expect(result.clearingReconciliations[0].clearingPattern).toBe(
      "SUPPORTED_ZR_DIRECT_DEBIT_REVERSAL",
    );
  });

  test("supports only the observed 1* SA balance-transfer pattern", () => {
    const result = normalisePaymentRows([
      row("sa-1", "SA", -60, { clearingDocument: "100001" }),
      row("sa-2", "SA", 60, { clearingDocument: "100001" }),
    ]);

    expect(result.observations).toHaveLength(0);
    expect(result.exceptions).toHaveLength(0);
    expect(result.clearingReconciliations[0].clearingPattern).toBe(
      "SUPPORTED_1_SA_BALANCE_TRANSFER",
    );
  });

  test("supports only the observed 9* $F reversal pattern", () => {
    const result = normalisePaymentRows([
      row("f-1", "$F", -25, { clearingDocument: "900001" }),
      row("f-2", "$F", 25, { clearingDocument: "900001" }),
    ]);

    expect(result.observations).toHaveLength(0);
    expect(result.exceptions).toHaveLength(0);
    expect(result.clearingReconciliations[0].clearingPattern).toBe(
      "SUPPORTED_9_DOLLAR_F_REVERSAL_PAIR",
    );
  });

  test("does not suppress a fault-injected semantic failure for a balanced group", () => {
    const result = normalisePaymentRows(
      [
        row("re-1", "RE", -1000, { clearingDocument: "500012" }),
        row("zp-2", "ZP", 1000, { clearingDocument: "500012" }),
      ],
      { shouldPreventAllocation: () => true },
    );

    expect(result.observations).toHaveLength(0);
    expect(result.exceptions).toContainEqual(
      expect.objectContaining({ code: "SEMANTIC_ALLOCATION_FAILED" }),
    );
    expect(result.clearingReconciliations[0]).toMatchObject({
      reconciled: true,
      allocationResolved: false,
      signedClearingGroupTotal: 0,
    });
  });

  test("keeps an unknown net-zero combination exceptional", () => {
    const result = normalisePaymentRows([
      row("unknown-1", "XY", -10, { clearingDocument: "500013" }),
      row("unknown-2", "ZZ", 10, { clearingDocument: "500013" }),
    ]);

    expect(result.observations).toHaveLength(0);
    expect(result.exceptions).toContainEqual(
      expect.objectContaining({ code: "UNSUPPORTED_CLEARING_EVENT" }),
    );
    expect(result.clearingReconciliations[0]).toMatchObject({
      reconciled: true,
      allocationResolved: false,
    });
  });

  test("retains an unmatched settlement exception even when source arithmetic balances", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -100, { clearingDocument: "500014" }),
      row("zp-2", "ZP", 100, { clearingDocument: "500014" }),
    ]);
    result.payments[0].unmatchedAmount = 100;
    result.exceptions.push({
      code: "UNMATCHED_PAYMENT",
      sourceStageRowId: "zp-2",
      amount: 100,
    });

    expect(result.clearingReconciliations[0].reconciled).toBe(true);
    expect(result.exceptions).toContainEqual(
      expect.objectContaining({ code: "UNMATCHED_PAYMENT" }),
    );
  });

  test("uses signed arithmetic for semantic direction and positive output values", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -100, { clearingDocument: "500015" }),
      row("zp-2", "ZP", 100, { clearingDocument: "500015" }),
    ]);

    expect(result.obligations[0].signedAmount).toBe(-100);
    expect(result.payments[0].signedAmount).toBe(100);
    expect(result.observations[0].amount).toBe(100);
  });

  test("does not use invoice due date when both invoice references are missing", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -100, {
        clearingDocument: "500016",
        invoiceIssueDate: null,
        invoiceReceiptDate: null,
        invoiceDueDate: "2026-01-09",
      }),
      row("zp-2", "ZP", 100, { clearingDocument: "500016" }),
    ]);

    expect(result.observations[0]).toMatchObject({
      paymentTimeDays: null,
      paymentTimeReferenceDate: null,
      paymentTimeReferenceKind: null,
      exceptionCode: "MAPPING_EXCEPTION",
    });
  });

  test("rejects missing grouping mappings explicitly", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -10, { companyCode: null }),
    ]);

    expect(result.exceptions).toContainEqual(
      expect.objectContaining({
        code: "MAPPING_EXCEPTION",
        field: "normalisation_group_key",
      }),
    );
  });

  test("keeps production SQL equivalent for business-Reference pools without balance suppression", () => {
    const {
      buildPaymentNormalisationCte,
      buildPersistPaymentNormalisationSql,
    } = require("./payment-normalisation.ptrs.service");
    const cte = buildPaymentNormalisationCte();
    const sql = buildPersistPaymentNormalisationSql();

    expect(cte).toContain("payment_normalisation_clearing_events");
    expect(cte).toContain("SUPPORTED_200_AB_REVERSAL_ADJUSTMENT");
    expect(cte).toContain("SUPPORTED_300_DZ_CUSTOMER_CLEARING");
    expect(cte).toContain("UNRESOLVED_ADJUSTMENT_ALLOCATION");
    expect(cte).toContain("economic_reference");
    expect(cte).toContain(`LOWER(NULLIF(BTRIM(s."description"), ''))`);
    expect(cte).not.toContain(
      `LOWER(NULLIF(BTRIM(s."invoiceReferenceNumber"), ''))`,
    );
    expect(cte).toContain("payment_normalisation_adjustment_target_sets");
    expect(cte).toContain("payment_normalisation_aggregate_ab_offset_groups");
    expect(cte).toContain(
      "target.economic_reference = obligation.economic_reference",
    );
    expect(cte).toContain("PARTITION BY target.allocation_group_key");
    expect(cte).toContain(
      "obligation.obligation_end > adjustment.adjustment_start",
    );
    expect(cte).not.toContain("target_count = 1");
    expect(cte).not.toContain(
      "payment_normalisation_event_rows AS MATERIALIZED",
    );
    expect(cte).not.toContain("payment_normalisation_group_source_evidence");
    expect(cte).toContain("signed_amount");
    expect(cte).toContain("allocation_resolved");
    expect(sql).toContain("'reconciled', reconciliation.reconciled");
    expect(sql).toContain(
      "'allocationResolved', reconciliation.allocation_resolved",
    );
    expect(sql).not.toContain("BALANCED_CLEARING_RECONCILIATION");
    expect(sql).not.toContain("accepted_as_balanced_clearing");
  });

  test("persists and summarises normalisation inside a tenant-scoped transaction", async () => {
    const transaction = {
      finished: false,
      commit: jest.fn(function commit() {
        this.finished = "commit";
      }),
      rollback: jest.fn(),
    };
    beginTransactionWithCustomerContext.mockResolvedValue(transaction);
    const resultRow = {
      id: "norm-1",
      update: jest.fn().mockResolvedValue(undefined),
    };
    db.PtrsPaymentNormalisationResult.findOne.mockResolvedValue(null);
    db.PtrsPaymentNormalisationResult.create.mockResolvedValue(resultRow);
    db.sequelize.query.mockResolvedValueOnce([]).mockResolvedValueOnce([
      {
        persisted: 3,
        startingStageCount: 3,
        paymentAllocationCount: 1,
      },
    ]);

    await expect(
      persistPaymentNormalisationEvidence({
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        profileId: "profile-1",
      }),
    ).resolves.toEqual({
      source: "calculated",
      normalisationResultId: "norm-1",
      inputSignature: expect.stringMatching(/^[a-f0-9]{64}$/),
      calculationVersion: "veolia-payment-normalisation-v8",
      persisted: 3,
      summary: { startingStageCount: 3, paymentAllocationCount: 1 },
      timings: expect.objectContaining({
        startedAt: expect.any(String),
        finishedAt: expect.any(String),
        lookupMs: expect.any(Number),
        calculationAndPersistenceMs: expect.any(Number),
        totalMs: expect.any(Number),
        phases: expect.objectContaining({
          transactionAcquire: expect.objectContaining({
            elapsedMs: expect.any(Number),
          }),
          materialisationStatement: expect.objectContaining({
            elapsedMs: expect.any(Number),
          }),
          commit: expect.objectContaining({ elapsedMs: expect.any(Number) }),
        }),
      }),
      limitations: { contractualInstalmentIndicatorAvailable: false },
    });
    expect(db.sequelize.query).toHaveBeenCalledTimes(2);
    const [sql, options] = db.sequelize.query.mock.calls[1];
    expect(options).toMatchObject({
      transaction,
      replacements: {
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        normalisationResultId: "norm-1",
        inputSignature: expect.stringMatching(/^[a-f0-9]{64}$/),
        calculationVersion: "veolia-payment-normalisation-v8",
      },
      type: "SELECT",
    });
    expect(sql).toContain("invoice_adjustment_evidence AS MATERIALIZED");
    expect(sql).toContain("invoice_payment_evidence AS MATERIALIZED");
    expect(sql).toContain('INSERT INTO "tbl_ptrs_payment_normalisation_row"');
    expect(sql).toContain(
      'INSERT INTO "tbl_ptrs_payment_normalisation_allocation"',
    );
    expect(sql).toContain(
      'INSERT INTO "tbl_ptrs_payment_normalisation_exception"',
    );
    expect(sql).toContain("'clearingEvent', CASE");
    expect(sql).toContain("payment_source_evidence AS MATERIALIZED");
    expect(sql).not.toContain("'sourceRows', reconciliation.source_rows");
    expect(sql).not.toContain("\n      evidence AS MATERIALIZED");
    expect(sql).toContain(
      "'allocationResolved', reconciliation.allocation_resolved",
    );
    expect(sql).toContain("'reconciled', reconciliation.reconciled");
    expect(sql).not.toContain("BALANCED_CLEARING_RECONCILIATION");
    expect(sql).toContain("updated AS (");
    expect(
      sql.match(/payment_normalisation_source_rows AS MATERIALIZED/g),
    ).toHaveLength(1);
    expect(sql).toContain("payment_normalisation_payment_allocations_raw AS (");
    expect(sql).not.toContain(
      "payment_normalisation_payment_allocations_raw AS MATERIALIZED",
    );
    expect(transaction.commit).toHaveBeenCalledTimes(1);
    expect(transaction.rollback).not.toHaveBeenCalled();
  });

  test("reuses an exact successful input and version without recalculation", async () => {
    const transaction = {
      finished: false,
      commit: jest.fn(function commit() {
        this.finished = "commit";
      }),
      rollback: jest.fn(),
    };
    beginTransactionWithCustomerContext.mockResolvedValue(transaction);
    db.sequelize.query.mockResolvedValue([]);
    db.PtrsPaymentNormalisationResult.findOne.mockResolvedValue({
      id: "norm-existing",
      status: "succeeded",
      summary: { paymentAllocationCount: 11 },
    });

    await expect(
      persistPaymentNormalisationEvidence({
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        profileId: "profile-1",
      }),
    ).resolves.toMatchObject({
      source: "persisted",
      normalisationResultId: "norm-existing",
      persisted: 0,
      summary: { paymentAllocationCount: 11 },
      timings: {
        lookupMs: expect.any(Number),
        calculationAndPersistenceMs: 0,
        totalMs: expect.any(Number),
        startedAt: expect.any(String),
        finishedAt: expect.any(String),
        phases: expect.objectContaining({
          resultLookup: expect.objectContaining({
            elapsedMs: expect.any(Number),
          }),
          commit: expect.objectContaining({ elapsedMs: expect.any(Number) }),
        }),
      },
    });
    expect(db.sequelize.query).toHaveBeenCalledTimes(1);
    expect(db.PtrsPaymentNormalisationResult.create).not.toHaveBeenCalled();
  });

  test("does not reuse an incomplete matching result", async () => {
    const transaction = {
      finished: false,
      commit: jest.fn(function commit() {
        this.finished = "commit";
      }),
      rollback: jest.fn(),
    };
    const resultRow = {
      id: "norm-incomplete",
      status: "failed",
      createdBy: null,
      update: jest.fn().mockResolvedValue(undefined),
    };
    beginTransactionWithCustomerContext.mockResolvedValue(transaction);
    db.PtrsPaymentNormalisationResult.findOne.mockResolvedValue(resultRow);
    db.PtrsPaymentNormalisationRow.destroy.mockResolvedValue(0);
    db.PtrsPaymentNormalisationAllocation.destroy.mockResolvedValue(0);
    db.PtrsPaymentNormalisationException.destroy.mockResolvedValue(0);
    db.sequelize.query
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([{ persisted: 1, startingStageCount: 1 }]);

    const result = await persistPaymentNormalisationEvidence({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      profileId: "profile-1",
    });

    expect(result.source).toBe("calculated");
    expect(resultRow.update).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ status: "calculating" }),
      { transaction },
    );
    expect(resultRow.update).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ status: "succeeded" }),
      { transaction },
    );
  });

  test("changes the result identity when financial inputs or version change", async () => {
    const base = {
      ptrsId: "ptrs-1",
      profileId: "profile-1",
      stageExecutionRunId: "stage-run-1",
      stageInputHash: "a".repeat(64),
      normalisationInputRevision: "7",
    };
    expect(buildPaymentNormalisationInputSignature(base)).not.toBe(
      buildPaymentNormalisationInputSignature({
        ...base,
        normalisationInputRevision: "8",
      }),
    );
    expect(
      require("./payment-normalisation.ptrs.service")
        .PAYMENT_NORMALISATION_VERSION,
    ).toBe("veolia-payment-normalisation-v8");
  });

  test("v8 does not reuse an existing successful v7 result", async () => {
    const transaction = {
      finished: false,
      commit: jest.fn(function commit() {
        this.finished = "commit";
      }),
      rollback: jest.fn(),
    };
    const resultRow = {
      id: "norm-new",
      update: jest.fn().mockResolvedValue(undefined),
    };
    beginTransactionWithCustomerContext.mockResolvedValue(transaction);
    db.PtrsPaymentNormalisationResult.findOne.mockImplementation(
      async ({ where }) =>
        where.calculationVersion === "veolia-payment-normalisation-v7"
          ? { id: "norm-old", status: "succeeded", summary: {} }
          : null,
    );
    db.PtrsPaymentNormalisationResult.create.mockResolvedValue(resultRow);
    db.sequelize.query
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([{ persisted: 2, startingStageCount: 2 }]);

    const calculated = await persistPaymentNormalisationEvidence({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      profileId: "profile-1",
    });

    expect(calculated).toMatchObject({
      source: "calculated",
      normalisationResultId: "norm-new",
      calculationVersion: "veolia-payment-normalisation-v8",
    });
    expect(db.PtrsPaymentNormalisationResult.create).toHaveBeenCalledWith(
      expect.objectContaining({
        calculationVersion: "veolia-payment-normalisation-v8",
      }),
      { transaction },
    );
    expect(db.PtrsPaymentNormalisationResult.findOne).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          calculationVersion: "veolia-payment-normalisation-v8",
        }),
      }),
    );
  });

  test("a changed financial-input revision cannot reuse the prior signature", async () => {
    const oldSignature = buildPaymentNormalisationInputSignature({
      ptrsId: "ptrs-1",
      profileId: "profile-1",
      stageExecutionRunId: "stage-run-1",
      stageInputHash: "a".repeat(64),
      normalisationInputRevision: "7",
    });
    db.Ptrs.findOne.mockResolvedValue({
      id: "ptrs-1",
      profileId: "profile-1",
      normalisationInputRevision: "8",
    });
    db.PtrsPaymentNormalisationResult.findOne.mockImplementation(
      async ({ where }) =>
        where.inputSignature === oldSignature
          ? { id: "norm-old", status: "succeeded", summary: {} }
          : null,
    );
    const transaction = {
      finished: false,
      commit: jest.fn(function commit() {
        this.finished = "commit";
      }),
      rollback: jest.fn(),
    };
    const resultRow = {
      id: "norm-new",
      update: jest.fn().mockResolvedValue(undefined),
    };
    beginTransactionWithCustomerContext.mockResolvedValue(transaction);
    db.PtrsPaymentNormalisationResult.create.mockResolvedValue(resultRow);
    db.sequelize.query
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([{ persisted: 2, startingStageCount: 2 }]);

    const calculated = await persistPaymentNormalisationEvidence({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      profileId: "profile-1",
    });

    expect(calculated.source).toBe("calculated");
    expect(calculated.inputSignature).not.toBe(oldSignature);
  });

  test("downstream resolution fails explicitly when the current result is missing", async () => {
    db.PtrsPaymentNormalisationResult.findOne.mockResolvedValue(null);

    await expect(
      requireCurrentPaymentNormalisationResult({
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        profileId: "profile-1",
        transaction: { id: "transaction-1" },
      }),
    ).rejects.toMatchObject({
      code: "PTRS_NORMALISATION_RESULT_MISSING",
      statusCode: 409,
    });
  });
});

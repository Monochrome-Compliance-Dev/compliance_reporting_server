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

  test("preserves prefix-5 RE and ZP settlement behaviour", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", 100, { clearingDocument: "500001" }),
      row("zp-2", "ZP", -100, { clearingDocument: "500001" }),
    ]);

    expect(result.observations).toHaveLength(1);
    expect(result.observations[0]).toMatchObject({
      invoiceStageRowId: "re-1",
      paymentStageRowId: "zp-2",
      amount: 100,
      partialPayment: false,
      finalSettlement: true,
      paymentTimeDays: 9,
    });
    expect(result.reconciliation.paymentAllocatedValue).toBe(100);
  });

  test("treats prefix-5 KR as an obligation settled by ZP", () => {
    const result = normalisePaymentRows([
      row("kr-1", "KR", 100, { clearingDocument: "500002" }),
      row("zp-2", "ZP", -100, { clearingDocument: "500002" }),
    ]);

    expect(result.obligations).toEqual([
      expect.objectContaining({ id: "kr-1", sourceDocumentType: "KR" }),
    ]);
    expect(result.observations).toEqual([
      expect.objectContaining({
        invoiceStageRowId: "kr-1",
        paymentStageRowId: "zp-2",
        amount: 100,
        finalSettlement: true,
      }),
    ]);
    expect(result.exceptions).toHaveLength(0);
  });

  test("settles mixed prefix-5 RE and KR obligations without manufacturing payments", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", 40, { clearingDocument: "500003" }),
      row("kr-2", "KR", 60, {
        clearingDocument: "500003",
        invoiceIssueDate: "2026-01-02",
      }),
      row("zp-3", "ZP", -100, { clearingDocument: "500003" }),
    ]);

    expect(result.payments).toHaveLength(1);
    expect(result.observations.map((observation) => observation.amount)).toEqual(
      [40, 60],
    );
    expect(result.reconciliation.paymentAllocatedValue).toBe(100);
  });

  test.each(["RE", "KR"])(
    "applies prefix-5 KG to a %s obligation before ZP settlement",
    (documentType) => {
      const result = normalisePaymentRows([
        row("invoice-1", documentType, -4791.53, {
          clearingDocument: "500004",
        }),
        row("kg-2", "KG", 530.89, { clearingDocument: "500004" }),
        row("zp-3", "ZP", 4260.64, { clearingDocument: "500004" }),
      ]);

      expect(result.adjustments).toEqual([
        expect.objectContaining({
          id: "kg-2",
          role: "KG",
          effectiveAmount: 530.89,
          allocatedAmount: 530.89,
          unmatchedAmount: 0,
          allocations: [
            expect.objectContaining({ reasonCode: "KG_PARTIAL_OFFSET" }),
          ],
        }),
      ]);
      expect(result.obligations[0].adjustedAmount).toBeCloseTo(4260.64, 8);
      expect(result.observations).toHaveLength(1);
      expect(result.observations[0]).toMatchObject({
        paymentStageRowId: "zp-3",
        finalSettlement: true,
      });
      expect(result.observations[0].amount).toBeCloseTo(4260.64, 8);
      expect(result.exceptions).not.toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            code: "UNRECOGNISED_DOCUMENT_TYPE",
            sourceStageRowId: "kg-2",
          }),
        ]),
      );
    },
  );

  test("nets offsetting prefix-5 KG rows before obligation allocation", () => {
    const result = normalisePaymentRows([
      row("kr-1", "KR", -100, { clearingDocument: "500005" }),
      row("kg-2", "KG", 20, { clearingDocument: "500005" }),
      row("kg-3", "KG", -20, { clearingDocument: "500005" }),
      row("zp-4", "ZP", 100, { clearingDocument: "500005" }),
    ]);

    expect(result.adjustments.map((adjustment) => adjustment.effectiveAmount)).toEqual(
      [0, 0],
    );
    expect(result.adjustments[0].reversalOffsetAmount).toBe(20);
    expect(result.adjustments[0].reconciliationCode).toBe(
      "KG_REVERSAL_OFFSET",
    );
    expect(result.obligations[0].adjustedAmount).toBe(100);
    expect(result.observations).toEqual([
      expect.objectContaining({ amount: 100, finalSettlement: true }),
    ]);
    expect(result.exceptions).toHaveLength(0);
  });

  test("reconciles equal-and-opposite prefix-5 ZP rows without observations", () => {
    const result = normalisePaymentRows([
      row("zp-1", "ZP", 75, { clearingDocument: "500006" }),
      row("zp-2", "ZP", -75, { clearingDocument: "500006" }),
    ]);

    expect(result.observations).toHaveLength(0);
    expect(result.payments).toEqual([
      expect.objectContaining({ unmatchedAmount: 0, clearingBalanceOffsetAmount: 75 }),
      expect.objectContaining({ unmatchedAmount: 0, clearingBalanceOffsetAmount: 75 }),
    ]);
    expect(result.exceptions).not.toEqual(
      expect.arrayContaining([expect.objectContaining({ code: "UNMATCHED_PAYMENT" })]),
    );
  });

  test.each(["SA", "AB"])(
    "reconciles a balanced prefix-5 %s and ZP event without promoting the accounting row",
    (documentType) => {
      const result = normalisePaymentRows([
        row("other-1", documentType, -100, { clearingDocument: "500007" }),
        row("zp-2", "ZP", 100, { clearingDocument: "500007" }),
      ]);

      expect(result.obligations).toHaveLength(0);
      expect(result.observations).toHaveLength(0);
      expect(result.payments[0]).toMatchObject({
        unmatchedAmount: 0,
        clearingBalanceOffsetAmount: 100,
      });
      expect(result.exceptions).toHaveLength(0);
      expect(result.clearingReconciliations).toEqual([
        expect.objectContaining({
          reconciliationCode: "BALANCED_CLEARING_RECONCILIATION",
          acceptedAsBalancedClearing: true,
          signedClearingGroupTotal: 0,
          finalUnexplainedSignedResidual: 0,
          participatingDocumentTypes: [documentType, "ZP"].sort(),
          sourceRows: expect.arrayContaining([
            expect.objectContaining({
              stageRowId: "other-1",
              normalisationRole: "UNRECOGNISED",
            }),
            expect.objectContaining({
              stageRowId: "zp-2",
              normalisationRole: "PAYMENT",
            }),
          ]),
        }),
      ]);
    },
  );

  test.each([
    ["AB", -20, -80, 100],
    ["SA", 20, -100, 80],
  ])(
    "preserves RE allocation while a balanced prefix-5 %s posting explains the clearing event",
    (documentType, otherAmount, invoiceAmount, paymentAmount) => {
      const result = normalisePaymentRows([
        row("re-1", "RE", invoiceAmount, { clearingDocument: "500008" }),
        row("other-2", documentType, otherAmount, {
          clearingDocument: "500008",
        }),
        row("zp-3", "ZP", paymentAmount, { clearingDocument: "500008" }),
      ]);

      expect(result.observations).not.toHaveLength(0);
      expect(result.exceptions).toHaveLength(0);
      expect(result.clearingReconciliations[0]).toMatchObject({
        reconciliationCode: "BALANCED_CLEARING_RECONCILIATION",
        acceptedAsBalancedClearing: true,
        finalUnexplainedSignedResidual: 0,
      });
    },
  );

  test("retains UNMATCHED_PAYMENT for a non-balanced prefix-5 group", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -100, { clearingDocument: "500009" }),
      row("zp-2", "ZP", 120, { clearingDocument: "500009" }),
    ]);

    expect(result.observations).toEqual([
      expect.objectContaining({ amount: 100, finalSettlement: true }),
    ]);
    expect(result.exceptions).toContainEqual(
      expect.objectContaining({
        code: "UNMATCHED_PAYMENT",
        sourceStageRowId: "zp-2",
        unmatchedAmount: 20,
      }),
    );
  });

  test.each(["RE", "KR"])(
    "treats prefix-4 %s as an obligation settled by KZ",
    (documentType) => {
      const result = normalisePaymentRows([
        row("invoice-1", documentType, -125, { clearingDocument: "400001" }),
        row("kz-2", "KZ", 125, { clearingDocument: "400001" }),
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
          paymentStageRowId: "kz-2",
          amount: 125,
          finalSettlement: true,
          classificationBasis:
            "outstanding_obligation_after_clearing_settlement",
        }),
      ]);
      expect(result.exceptions).toHaveLength(0);
      expect(result.clearingReconciliations).toHaveLength(0);
    },
  );

  test("nets an opposite-direction prefix-4 RE before KZ settlement", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -10810.92, { clearingDocument: "400002" }),
      row("re-2", "RE", 735.44, {
        clearingDocument: "400002",
        invoiceIssueDate: "2026-01-02",
      }),
      row("kz-3", "KZ", 10075.48, { clearingDocument: "400002" }),
    ]);

    expect(result.obligations[0]).toMatchObject({
      originalAmount: 10810.92,
      adjustedAmount: 10075.48,
      directionalOffsetAllocatedAmount: 735.44,
    });
    expect(result.obligations[1]).toMatchObject({
      originalAmount: 735.44,
      adjustedAmount: 0,
      directionalOffsetAllocatedAmount: 735.44,
      directionalOffsetUnmatchedAmount: 0,
    });
    expect(result.observations).toEqual([
      expect.objectContaining({
        invoiceStageRowId: "re-1",
        paymentStageRowId: "kz-3",
        amount: 10075.48,
        finalSettlement: true,
      }),
    ]);
    expect(result.reconciliation.unmatchedPaymentValue).toBeCloseTo(0, 8);
    expect(result.reconciliation.directionalObligationOffsetValue).toBe(735.44);
    expect(
      result.reconciliation.unmatchedDirectionalObligationOffsetValue,
    ).toBe(0);
  });

  test("reconciles a balanced prefix-4 KZ and SA event without inventing roles", () => {
    const result = normalisePaymentRows([
      row("sa-1", "SA", 100, { clearingDocument: "400003" }),
      row("kz-2", "KZ", -100, { clearingDocument: "400003" }),
    ]);

    expect(result.payments).toHaveLength(0);
    expect(result.observations).toHaveLength(0);
    expect(result.exceptions).toHaveLength(0);
    expect(result.clearingReconciliations[0]).toMatchObject({
      reconciliationCode: "BALANCED_CLEARING_RECONCILIATION",
      acceptedAsBalancedClearing: true,
      participatingDocumentTypes: ["KZ", "SA"],
      sourceRows: expect.arrayContaining([
        expect.objectContaining({
          stageRowId: "sa-1",
          normalisationRole: "UNRECOGNISED",
        }),
        expect.objectContaining({
          stageRowId: "kz-2",
          normalisationRole: "UNRECOGNISED",
        }),
      ]),
    });
  });

  test("reconciles equal-and-opposite prefix-4 KG evidence without economic output", () => {
    const result = normalisePaymentRows([
      row("kg-1", "KG", 45, { clearingDocument: "400010" }),
      row("kg-2", "KG", -45, { clearingDocument: "400010" }),
    ]);

    expect(result.obligations).toHaveLength(0);
    expect(result.adjustments).toHaveLength(0);
    expect(result.payments).toHaveLength(0);
    expect(result.observations).toHaveLength(0);
    expect(result.exceptions).toHaveLength(0);
    expect(result.clearingReconciliations[0]).toMatchObject({
      reconciliationCode: "BALANCED_CLEARING_RECONCILIATION",
      signedClearingGroupTotal: 0,
      residualExceptionAmountBeforeReconciliation: 90,
      finalUnexplainedSignedResidual: 0,
      participatingDocumentTypes: ["KG"],
    });
  });

  test("reconciles a self-reversing prefix-4 ET event without an economic adjustment", () => {
    const result = normalisePaymentRows([
      row("et-1", "ET", 8.6, { clearingDocument: "400011" }),
      row("et-2", "ET", -8.6, { clearingDocument: "400011" }),
    ]);

    expect(result.adjustments).toEqual([
      expect.objectContaining({
        allocatedAmount: 0,
        unmatchedAmountBeforeClearingReconciliation: 8.6,
        unmatchedAmount: 0,
        reconciliationCode: "BALANCED_CLEARING_RECONCILIATION",
      }),
      expect.objectContaining({
        allocatedAmount: 0,
        unmatchedAmountBeforeClearingReconciliation: 8.6,
        unmatchedAmount: 0,
        reconciliationCode: "BALANCED_CLEARING_RECONCILIATION",
      }),
    ]);
    expect(result.observations).toHaveLength(0);
    expect(result.exceptions).toHaveLength(0);
  });

  test("reconciles equal-and-opposite prefix-4 KZ evidence without a payment", () => {
    const result = normalisePaymentRows([
      row("kz-1", "KZ", 30, { clearingDocument: "400012" }),
      row("kz-2", "KZ", -30, { clearingDocument: "400012" }),
    ]);

    expect(result.payments).toHaveLength(0);
    expect(result.observations).toHaveLength(0);
    expect(result.exceptions).toHaveLength(0);
    expect(result.clearingReconciliations[0]).toMatchObject({
      acceptedAsBalancedClearing: true,
      participatingDocumentTypes: ["KZ"],
    });
  });

  test("reconciles prefix-2 AB, KR and RE residuals after preserving their semantic roles", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", -100, { clearingDocument: "200013" }),
      row("ab-2", "AB", 80, { clearingDocument: "200013" }),
      row("kr-3", "KR", 30, { clearingDocument: "200013" }),
      row("ab-4", "AB", -10, { clearingDocument: "200013" }),
    ]);

    expect(result.obligations).toEqual([
      expect.objectContaining({ id: "re-1", role: "INVOICE" }),
    ]);
    expect(result.adjustments).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: "ab-2", role: "CREDIT" }),
        expect.objectContaining({ id: "kr-3", role: "CREDIT" }),
        expect.objectContaining({ id: "ab-4", role: "CREDIT" }),
      ]),
    );
    expect(result.payments).toHaveLength(0);
    expect(result.observations).toHaveLength(0);
    expect(result.exceptions).toHaveLength(0);
    expect(result.clearingReconciliations[0]).toMatchObject({
      acceptedAsBalancedClearing: true,
      participatingDocumentTypes: ["AB", "KR", "RE"],
      finalUnexplainedSignedResidual: 0,
    });
  });

  test.each([
    [
      "prefix-4 KG",
      [
        row("kg-1", "KG", 45, { clearingDocument: "400014" }),
        row("kg-2", "KG", -40, { clearingDocument: "400014" }),
      ],
      5,
    ],
    [
      "prefix-4 ET",
      [
        row("et-1", "ET", 8.6, { clearingDocument: "400015" }),
        row("et-2", "ET", -8, { clearingDocument: "400015" }),
      ],
      0.6,
    ],
  ])(
    "keeps an unbalanced %s residual exceptional and visible",
    (_label, inputRows, expectedResidual) => {
      const result = normalisePaymentRows(inputRows);

      expect(result.exceptions.length).toBeGreaterThan(0);
      expect(result.clearingReconciliations[0]).toMatchObject({
        reconciliationCode: null,
        acceptedAsBalancedClearing: false,
        remainingSignedResidualBeforeFinalReconciliation: expect.closeTo(
          expectedResidual,
          8,
        ),
        finalUnexplainedSignedResidual: expect.closeTo(expectedResidual, 8),
      });
      expect(result.reconciliation.unexplainedClearingResidualValue).toBeCloseTo(
        expectedResidual,
        8,
      );
    },
  );

  test("keeps standalone KZ as an explicit unsupported exception", () => {
    const result = normalisePaymentRows([
      row("kz-1", "KZ", -100, { clearingDocument: "400004" }),
    ]);

    expect(result.payments).toHaveLength(0);
    expect(result.exceptions).toContainEqual(
      expect.objectContaining({
        code: "UNRECOGNISED_DOCUMENT_TYPE",
        sourceStageRowId: "kz-1",
        documentType: "KZ",
      }),
    );
  });

  test("does not globally reclassify KR outside prefix-4 and prefix-5 groups", () => {
    const result = normalisePaymentRows([
      row("kr-1", "KR", -100, { clearingDocument: "600001" }),
    ]);

    expect(result.obligations).toHaveLength(0);
    expect(result.exceptions).toContainEqual(
      expect.objectContaining({
        code: "UNRECOGNISED_DOCUMENT_TYPE",
        sourceStageRowId: "kr-1",
        documentType: "KR",
      }),
    );
  });

  test("retains an exception for a genuinely unallocatable prefix-4 offset", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", 25, { clearingDocument: "400005" }),
      row("kz-2", "KZ", 25, { clearingDocument: "400005" }),
    ]);

    expect(result.exceptions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          code: "UNMATCHED_OBLIGATION_OFFSET",
          sourceStageRowId: "re-1",
          unmatchedAmount: 25,
        }),
        expect.objectContaining({
          code: "UNMATCHED_PAYMENT",
          sourceStageRowId: "kz-2",
          unmatchedAmount: 25,
        }),
      ]),
    );
  });

  test("represents clearing-context roles and signed prefix-4 offsets in SQL", () => {
    const sql = require("./payment-normalisation.ptrs.service").buildPaymentNormalisationCte();

    expect(sql).toContain(
      "payment_normalisation_classified_source_rows AS MATERIALIZED",
    );
    expect(sql).toContain("source.document_type = 'KR'");
    expect(sql).toContain("source.document_type = 'KZ'");
    expect(sql).toContain("source.document_type = 'KG'");
    expect(sql).toContain("obligation.document_type IN ('RE', 'KR')");
    expect(sql).toContain(
      "payment_normalisation_directional_offset_allocations AS MATERIALIZED",
    );
    expect(sql).toContain("AND signed_amount < -0.005");
    expect(sql).toContain("AND signed_amount > 0.005");
    expect(sql).toContain(
      "payment_normalisation_kg_group_totals AS MATERIALIZED",
    );
    expect(sql).toContain(
      "payment_normalisation_payment_source_totals_raw AS MATERIALIZED",
    );
    expect(sql).toContain("context.signed_balance");
    expect(sql).toContain(
      "payment_normalisation_exceptions_before_clearing_reconciliation AS MATERIALIZED",
    );
    expect(sql).toContain(
      "payment_normalisation_clearing_reconciliations AS MATERIALIZED",
    );
    expect(sql).toContain("'BALANCED_CLEARING_RECONCILIATION'::text");
    expect(sql).toContain("ABS(context.signed_balance) <= 0.005");
    expect(sql).toContain(
      "COALESCE(reconciliation.accepted_as_balanced_clearing, false)",
    );
    const finalExceptionFilter = sql.slice(
      sql.lastIndexOf(
        "payment_normalisation_exceptions AS MATERIALIZED",
      ),
    );
    expect(sql).toContain("'MAPPING_EXCEPTION'::text AS reason_code");
    expect(finalExceptionFilter).not.toContain("'MAPPING_EXCEPTION'");
  });

  test("allocates one ZP progressively across multiple RE obligations", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", 40, { invoiceIssueDate: "2026-01-01" }),
      row("re-2", "RE", 60, { invoiceIssueDate: "2026-01-03" }),
      row("zp-3", "ZP", 100),
    ]);

    expect(result.observations.map((item) => item.amount)).toEqual([40, 60]);
    expect(result.observations.every((item) => item.finalSettlement)).toBe(
      true,
    );
  });

  test("retains partial value and marks only the later ZP as final settlement", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", 100),
      row("zp-2", "ZP", 30, { paymentDate: "2026-01-05" }),
      row("zp-3", "ZP", 70, { paymentDate: "2026-01-10" }),
    ]);

    expect(result.observations).toEqual([
      expect.objectContaining({
        paymentStageRowId: "zp-2",
        amount: 30,
        partialPayment: true,
        finalSettlement: false,
        reasonCode: "PARTIAL_PAYMENT",
      }),
      expect.objectContaining({
        paymentStageRowId: "zp-3",
        amount: 70,
        partialPayment: false,
        finalSettlement: true,
      }),
    ]);
    expect(result.reconciliation.partialPaymentCount).toBe(1);
    expect(result.reconciliation.finalPaymentCount).toBe(1);
  });

  test("applies full and partial ET, credit and refund offsets", () => {
    const et = normalisePaymentRows([
      row("re-1", "RE", 100, { description: "invoice-a" }),
      row("et-2", "ET", 100, { description: "invoice-a" }),
    ]);
    expect(et.adjustments[0].allocations[0].reasonCode).toBe("ET_FULL_OFFSET");

    const financial = normalisePaymentRows([
      row("re-1", "RE", 100, { clearingDocument: "200-credit" }),
      row("cr-2", "XX", 40, { clearingDocument: "200-credit" }),
      row("re-3", "RE", 60, { clearingDocument: "300-refund" }),
      row("rf-4", "XX", 60, { clearingDocument: "300-refund" }),
    ]);
    expect(financial.adjustments.flatMap((item) => item.allocations)).toEqual([
      expect.objectContaining({
        reasonCode: "CREDIT_PARTIAL_OFFSET",
        amount: 40,
      }),
      expect.objectContaining({ reasonCode: "REFUND_FULL_OFFSET", amount: 60 }),
    ]);
    expect(financial.obligations.map((item) => item.adjustedAmount)).toEqual([
      60, 0,
    ]);
  });

  test.each([
    [
      "ET",
      row("et-2", "ET", 40, { description: "invoice-a" }),
      { description: "invoice-a" },
      "ET_PARTIAL_OFFSET",
    ],
    [
      "refund",
      row("rf-2", "XX", 40, { clearingDocument: "300-refund" }),
      { clearingDocument: "300-refund" },
      "REFUND_PARTIAL_OFFSET",
    ],
  ])(
    "emits the %s partial-offset reason",
    (_kind, adjustment, invoice, reason) => {
      const result = normalisePaymentRows([
        row("re-1", "RE", 100, invoice),
        adjustment,
      ]);

      expect(result.adjustments[0].allocations[0]).toMatchObject({
        reasonCode: reason,
        amount: 40,
        obligationAfter: 60,
      });
    },
  );

  test("uses an 800 net obligation and actual ZP amount after a 200 credit", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", 1000, { clearingDocument: "200-credit" }),
      row("cr-2", "XX", 200, { clearingDocument: "200-credit" }),
      row("zp-3", "ZP", 800, { clearingDocument: "200-credit" }),
    ]);

    expect(result.obligations[0].adjustedAmount).toBe(800);
    expect(result.observations).toEqual([
      expect.objectContaining({
        amount: 800,
        adjustedObligationAmount: 800,
        finalSettlement: true,
      }),
    ]);
  });

  test("fully offsets an obligation without manufacturing a payment", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", 1000, { clearingDocument: "200-credit" }),
      row("cr-2", "XX", 1000, { clearingDocument: "200-credit" }),
    ]);

    expect(result.obligations[0].adjustedAmount).toBe(0);
    expect(result.observations).toHaveLength(0);
    expect(result.adjustments[0].allocations[0]).toMatchObject({
      reasonCode: "CREDIT_FULL_OFFSET",
      amount: 1000,
    });
  });

  test("keeps an SBI-positive ET-adjusted obligation eligible for its settling ZP observation", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", 1000, {
        description: "invoice-a",
        isSmallBusiness: true,
      }),
      row("et-2", "ET", 50, { description: "invoice-a" }),
      row("zp-3", "ZP", 950),
    ]);

    expect(result.obligations[0].adjustedAmount).toBe(950);
    expect(result.obligations[0].isSmallBusiness).toBe(true);
    expect(result.observations[0]).toMatchObject({
      amount: 950,
      partialPayment: false,
      finalSettlement: true,
    });
  });

  test("allocates 750 progressively over 600 and 400 obligations", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", 600, { clearingDocument: "200-credit" }),
      row("re-2", "RE", 400, {
        clearingDocument: "200-credit",
        invoiceIssueDate: "2026-01-03",
      }),
      row("cr-3", "XX", 750, { clearingDocument: "200-credit" }),
    ]);

    expect(result.obligations.map((item) => item.adjustedAmount)).toEqual([
      0, 250,
    ]);
    expect(result.adjustments[0]).toMatchObject({
      allocatedAmount: 750,
      unmatchedAmount: 0,
    });
    expect(result.adjustments[0].allocations).toEqual([
      expect.objectContaining({ invoiceStageRowId: "re-1", amount: 600 }),
      expect.objectContaining({ invoiceStageRowId: "re-2", amount: 150 }),
    ]);
  });

  test("sends unmatched and mixed adjustment combinations to review", () => {
    const unmatched = normalisePaymentRows([
      row("re-1", "RE", 50, { clearingDocument: "200-credit" }),
      row("cr-2", "XX", 80, { clearingDocument: "200-credit" }),
    ]);
    expect(unmatched.exceptions).toContainEqual(
      expect.objectContaining({
        code: "UNMATCHED_ADJUSTMENT",
        unmatchedAmount: 30,
      }),
    );

    const mixed = normalisePaymentRows([
      row("re-1", "RE", 100, {
        clearingDocument: "200-credit",
        description: "invoice-a",
      }),
      row("et-2", "ET", 10, {
        clearingDocument: "200-credit",
        description: "invoice-a",
      }),
      row("cr-3", "XX", 10, { clearingDocument: "200-credit" }),
    ]);
    expect(mixed.adjustments.every((item) => item.allocatedAmount === 0)).toBe(
      true,
    );
    expect(mixed.exceptions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ code: "AMBIGUOUS_ADJUSTMENT_COMBINATION" }),
      ]),
    );
  });

  test("reconciles exact net-zero financial adjustment reversals", () => {
    const result = normalisePaymentRows([
      row("ab-1", "AB", 37.59, { clearingDocument: "200-reversal" }),
      row("kr-2", "KR", -37.59, { clearingDocument: "200-reversal" }),
    ]);

    expect(result.adjustments).toEqual([
      expect.objectContaining({
        reversalOffsetAmount: 37.59,
        unmatchedAmount: 0,
        reconciliationCode: "NET_ZERO_ADJUSTMENT_REVERSAL",
      }),
      expect.objectContaining({
        reversalOffsetAmount: 37.59,
        unmatchedAmount: 0,
        reconciliationCode: "NET_ZERO_ADJUSTMENT_REVERSAL",
      }),
    ]);
    expect(result.exceptions).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ code: "UNMATCHED_ADJUSTMENT" }),
      ]),
    );
    expect(result.reconciliation.adjustmentReversalOffsetValue).toBe(75.18);
  });

  test("does not apply reversal reconciliation when invoice obligations exist", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", 100, { clearingDocument: "200-with-invoice" }),
      row("ab-2", "AB", 10, { clearingDocument: "200-with-invoice" }),
      row("kr-3", "KR", -10, { clearingDocument: "200-with-invoice" }),
    ]);

    expect(result.adjustments).toEqual([
      expect.objectContaining({ reversalOffsetAmount: 0 }),
      expect.objectContaining({ reversalOffsetAmount: 0 }),
    ]);
    expect(result.obligations[0].adjustedAmount).toBe(80);
  });

  test.each([
    [
      "both invoice dates using the earlier Ariba receipt date",
      {
        invoiceIssueDate: "2025-12-09",
        invoiceReceiptDate: "2025-12-08",
        paymentDate: "2026-01-02",
      },
      { days: 26, date: "2025-12-08", kind: "invoice_receipt" },
    ],
    [
      "both invoice dates using the later Ariba receipt date",
      { invoiceIssueDate: "2026-01-01", invoiceReceiptDate: "2026-01-04" },
      { days: 7, date: "2026-01-04", kind: "invoice_receipt" },
    ],
    [
      "matching invoice issue and receipt dates",
      { invoiceIssueDate: "2026-01-01", invoiceReceiptDate: "2026-01-01" },
      { days: 10, date: "2026-01-01", kind: "invoice_receipt" },
    ],
    [
      "invoice issue date only",
      { invoiceIssueDate: "2026-01-01", invoiceReceiptDate: null },
      { days: 10, date: "2026-01-01", kind: "invoice_issue" },
    ],
    [
      "invoice receipt date only",
      { invoiceIssueDate: null, invoiceReceiptDate: "2026-01-04" },
      { days: 7, date: "2026-01-04", kind: "invoice_receipt" },
    ],
    [
      "a payment before the sole invoice reference date",
      {
        invoiceIssueDate: "2026-01-11",
        invoiceReceiptDate: null,
        paymentDate: "2026-01-10",
      },
      { days: 0, date: "2026-01-11", kind: "invoice_issue" },
    ],
  ])("derives Payment Time from %s", (_label, invoice, expected) => {
    const { paymentDate = "2026-01-10", ...invoiceDates } = invoice;
    const result = normalisePaymentRows([
      row("re-1", "RE", 100, invoiceDates),
      row("zp-2", "ZP", 100, { paymentDate }),
    ]);

    expect(result.observations[0]).toMatchObject({
      paymentTimeDays: expected.days,
      paymentTimeReferenceDate: expected.date,
      paymentTimeReferenceKind: expected.kind,
    });
    if (expected.kind === "invoice_receipt") {
      expect(result.observations[0]).toMatchObject({
        paymentTimeReferencePolicy: "veolia_ariba_invoice_receipt_v1",
        paymentTimeReferenceReason:
          "veolia_ordinary_invoice_ariba_receipt_date",
      });
    }
    expect(result.observations[0]).not.toHaveProperty("exceptionCode");
  });

  test("does not use invoice due date when both invoice references are missing", () => {
    const result = normalisePaymentRows([
      row("re-1", "RE", 100, {
        invoiceIssueDate: null,
        invoiceReceiptDate: null,
        invoiceDueDate: "2026-01-09",
      }),
      row("zp-2", "ZP", 100),
    ]);

    expect(result.observations[0]).toMatchObject({
      paymentTimeDays: null,
      paymentTimeReferenceDate: null,
      paymentTimeReferenceKind: null,
      exceptionCode: "MAPPING_EXCEPTION",
    });
  });

  test("rejects unrecognised document types and missing grouping mappings", () => {
    const result = normalisePaymentRows([
      row("x-1", "ZZ", 10),
      row("re-2", "RE", 10, { companyCode: null }),
    ]);

    expect(result.exceptions).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ code: "UNRECOGNISED_DOCUMENT_TYPE" }),
        expect.objectContaining({
          code: "MAPPING_EXCEPTION",
          field: "normalisation_group_key",
        }),
      ]),
    );
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
      calculationVersion: "veolia-payment-normalisation-v6",
      persisted: 3,
      summary: { startingStageCount: 3, paymentAllocationCount: 1 },
      timings: {
        lookupMs: expect.any(Number),
        calculationAndPersistenceMs: expect.any(Number),
        totalMs: expect.any(Number),
      },
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
        calculationVersion: "veolia-payment-normalisation-v6",
      },
      type: "SELECT",
    });
    expect(sql).toContain("invoice_adjustment_evidence AS MATERIALIZED");
    expect(sql).toContain("invoice_payment_evidence AS MATERIALIZED");
    expect(sql).toContain(
      'INSERT INTO "tbl_ptrs_payment_normalisation_row"',
    );
    expect(sql).toContain(
      'INSERT INTO "tbl_ptrs_payment_normalisation_allocation"',
    );
    expect(sql).toContain(
      'INSERT INTO "tbl_ptrs_payment_normalisation_exception"',
    );
    expect(sql).toContain("'clearingReconciliation', jsonb_build_object(");
    expect(sql).toContain("'sourceRows', reconciliation.source_rows");
    expect(sql).toContain(
      "'reconciliationCode', reconciliation.reconciliation_code",
    );
    expect(sql).toContain(
      "WHEN reconciliation.accepted_as_balanced_clearing THEN 0",
    );
    expect(sql).toContain("updated AS (");
    expect(
      sql.match(/payment_normalisation_source_rows AS MATERIALIZED/g),
    ).toHaveLength(1);
    expect(sql).toContain(
      "payment_normalisation_payment_allocations_raw AS (",
    );
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
    db.sequelize.query.mockResolvedValueOnce([]).mockResolvedValueOnce([
      { persisted: 1, startingStageCount: 1 },
    ]);

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
      require("./payment-normalisation.ptrs.service").PAYMENT_NORMALISATION_VERSION,
    ).toBe("veolia-payment-normalisation-v6");
  });

  test("v6 does not reuse an existing successful v5 result", async () => {
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
        where.calculationVersion === "veolia-payment-normalisation-v5"
          ? { id: "norm-old", status: "succeeded", summary: {} }
          : null,
    );
    db.PtrsPaymentNormalisationResult.create.mockResolvedValue(resultRow);
    db.sequelize.query.mockResolvedValueOnce([]).mockResolvedValueOnce([
      { persisted: 2, startingStageCount: 2 },
    ]);

    const calculated = await persistPaymentNormalisationEvidence({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      profileId: "profile-1",
    });

    expect(calculated).toMatchObject({
      source: "calculated",
      normalisationResultId: "norm-new",
      calculationVersion: "veolia-payment-normalisation-v6",
    });
    expect(db.PtrsPaymentNormalisationResult.create).toHaveBeenCalledWith(
      expect.objectContaining({
        calculationVersion: "veolia-payment-normalisation-v6",
      }),
      { transaction },
    );
    expect(db.PtrsPaymentNormalisationResult.findOne).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          calculationVersion: "veolia-payment-normalisation-v6",
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
    db.sequelize.query.mockResolvedValueOnce([]).mockResolvedValueOnce([
      { persisted: 2, startingStageCount: 2 },
    ]);

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

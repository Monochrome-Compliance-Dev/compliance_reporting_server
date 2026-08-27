const { stagePtrs } = require("./stage.build.ptrs.service");
const {
  buildPersistedStageRow,
  buildStageColumnProjection,
  collectCanonicalContractFields,
  computePaymentTimeRegulator,
  parseISODateOnly,
  toSnakeCase,
} = require("./stage.payment-time.ptrs.service");

const CONTRACT = {
  identity: {
    payer_entity_name: {},
    payer_entity_abn: {},
    payee_entity_name: {},
    payee_entity_abn: {},
    invoice_reference_number: {},
  },
  transaction: { payment_amount: {}, description: {} },
  dates: {
    payment_date: {},
    invoice_issue_date: {},
    invoice_receipt_date: {},
    invoice_due_date: {},
  },
  terms: { invoice_payment_terms: {} },
  operational_source_fields: {
    source_account_code: {},
    document_type: {},
    clearing_document: {},
    entry_date: {},
    source_user: {},
    document_currency: {},
  },
  regulator_flags: {},
};

function makeDependencies(persistedRows) {
  const transaction = {
    finished: false,
    commit: jest.fn(async function commit() {
      this.finished = "commit";
    }),
    rollback: jest.fn(async function rollback() {
      this.finished = "rollback";
    }),
  };
  const sourceRow = {
    row_no: 1,
    payer_entity_name: "Veolia Water Technologies 2 Pty Ltd",
    payer_entity_abn: "30 616 561 829",
    payee_entity_name: "ILLAWARRA GROUNDS & SURROUNDS",
    payee_entity_abn: "22600186310",
    invoice_reference_number: "4000014744",
    source_account_code: "1010012308",
    document_type: "RE",
    clearing_document: "5000009966",
    payment_amount: "-9,229.02",
    payment_date: "02/01/2026",
    invoice_issue_date: "08/12/2025",
    invoice_receipt_date: "08/12/2025",
    invoice_due_date: "01/01/2026",
    invoice_payment_terms: "0027",
    entry_date: "09/12/2025",
    source_user: "ARIBA_CIG",
    document_currency: "AUD",
    supplier: "SUP-1",
  };
  const stageAttributes = Object.fromEntries(
    [
      "payerEntityName",
      "payerEntityAbn",
      "payeeEntityName",
      "payeeEntityAbn",
      "invoiceReferenceNumber",
      "sourceAccountCode",
      "description",
      "documentType",
      "documentCurrency",
      "clearingDocument",
      "paymentAmount",
      "paymentDate",
      "invoiceIssueDate",
      "invoiceReceiptDate",
      "invoiceDueDate",
      "entryDate",
      "sourceUser",
      "paymentTermRaw",
      "paymentTermDays",
      "paymentTimeDays",
    ].map((field) => [
      field,
      ["paymentTermDays", "paymentTimeDays"].includes(field)
        ? { type: { key: "INTEGER" } }
        : field === "paymentAmount"
          ? { type: { key: "DECIMAL" } }
          : { type: { key: "STRING" } },
    ]),
  );
  const loadedRevisions = new Set();

  return {
    customerId: "customer-1",
    ptrsId: "ptrs-1",
    profileId: "profile-1",
    userId: "user-1",
    force: true,
    beginTransactionWithCustomerContext: jest.fn(async () => transaction),
    createPtrsTrace: jest.fn(() => ({ write: jest.fn(), close: jest.fn() })),
    hrMsSince: jest.fn(() => 1),
    safeMeta: jest.fn((value) => value),
    slog: { info: jest.fn() },
    getStageStaleness: jest.fn(async () => ({
      inputHash: "input-hash",
      previousRunId: null,
      existingStageCount: 0,
    })),
    getLatestExecutionRun: jest.fn(async () => null),
    createExecutionRun: jest.fn(async () => ({ id: "run-1" })),
    updateExecutionRun: jest.fn(async () => undefined),
    resolveCurrentCanonicalRevisions: jest.fn(async () => [
      {
        dataset: { id: "dataset-1", purpose: "transaction" },
        datasetOrder: 0,
        revision: { id: "revision-1", materialSignature: "sig-1", rowCount: 1 },
      },
    ]),
    loadCanonicalRevisionRows: jest.fn(async ({ revisionId, afterSourceRowNo }) => {
      if (afterSourceRowNo != null || loadedRevisions.has(revisionId)) return [];
      loadedRevisions.add(revisionId);
      const datasetId = revisionId === "revision-2" ? "dataset-2" : "dataset-1";
      return [{
        ...sourceRow,
        invoice_reference_number: revisionId === "revision-2" ? "SECOND" : sourceRow.invoice_reference_number,
        _canonicalProvenance: {
          canonicalRevisionId: revisionId,
          canonicalSourceRowId: `row-${revisionId}`,
          datasetId,
          sourceRawRowId: `raw-${revisionId}`,
          sourceRowNo: 1,
          adapterType: "sap_accounting_event",
          adapterVersion: "1",
          sourceGroupScope: null,
          semanticKind: "accounting_event",
          lineage: { joinedReferences: {} },
        },
      }];
    }),
    getColumnMap: jest.fn(async () => ({
      rowRules: [
        {
          id: "description-rule",
          when: [{ field: "document_type", op: "eq", value: "RE" }],
          then: [{ op: "concat_fields", field: "description" }],
        },
      ],
    })),
    applyRules: jest.fn((rows) => {
      rows[0].description = `Invoice ${rows[0].invoice_reference_number}`;
      return {
        rows,
        stats: { rulesTried: 1, rowsAffected: 1, actions: 1 },
      };
    }),
    loadEffectiveTermChangesForRows: jest.fn(async () => new Map()),
    applyEffectiveTermChangesToRows: jest.fn((rows) => {
      rows[0].contract_po_payment_terms_effective = "CHANGED";
      rows[0].contract_po_payment_terms_effective_source = "TERM_CHANGES";
      return { rows, stats: { considered: 1, applied: 1, missingKey: 0 } };
    }),
    loadPaymentTermMap: jest.fn(async () => new Map([["CHANGED", 45]])),
    applyPaymentTermDaysFromMap: jest.fn((rows, termMap) => {
      rows[0].payment_term_days = termMap.get(
        rows[0].contract_po_payment_terms_effective,
      );
      return {
        rows,
        stats: { lookedUp: 1, filled: 1, missing: 0, unmapped: 0 },
      };
    }),
    computePaymentTimeRegulator,
    collectCanonicalContractFields,
    PTRS_CANONICAL_CONTRACT: CONTRACT,
    toSnakeCase,
    buildPersistedStageRow,
    buildStageColumnProjection,
    db: {
      PtrsFieldMap: { findAll: jest.fn(async () => []) },
      PtrsDataset: {
        findAll: jest.fn(async () => [
          { id: "dataset-1", purpose: "transaction" },
        ]),
      },
      PtrsImportRaw: { count: jest.fn(async () => 0) },
      PtrsStageRow: {
        rawAttributes: stageAttributes,
        destroy: jest.fn(async () => {
          const removed = persistedRows.length;
          persistedRows.length = 0;
          return removed;
        }),
        bulkCreate: jest.fn(async (rows) => persistedRows.push(...rows)),
      },
    },
  };
}

describe("PTRS stage preview/persist parity", () => {
  test("refuses Stage when any selected dataset lacks a current canonical revision", async () => {
    const dependencies = makeDependencies([]);
    dependencies.resolveCurrentCanonicalRevisions.mockRejectedValue(
      Object.assign(new Error("Canonical rebuild required for transaction dataset(s): B.csv"), {
        code: "CANONICAL_REVISION_REQUIRED",
      }),
    );
    await expect(stagePtrs({ ...dependencies, persist: false, limit: 50 }))
      .rejects.toThrow("B.csv");
    expect(dependencies.loadCanonicalRevisionRows).not.toHaveBeenCalled();
  });

  test("uses the same canonical transformations and persists typed columns", async () => {
    const previewRows = [];
    const previewDependencies = makeDependencies(previewRows);
    const preview = await stagePtrs({
      ...previewDependencies,
      persist: false,
      limit: 50,
    });
    expect(previewDependencies.loadCanonicalRevisionRows).toHaveBeenCalledWith(
      expect.objectContaining({ revisionId: "revision-1" }),
    );

    const persistedRows = [];
    const persisted = await stagePtrs({
      ...makeDependencies(persistedRows),
      persist: true,
    });

    expect(persisted.persistedCount).toBe(1);
    expect(persistedRows).toHaveLength(1);
    const stored = persistedRows[0];
    expect(stored.data).toMatchObject({
      description: preview.sample.description,
      payment_amount: "-9,229.02",
      contract_po_payment_terms_effective:
        preview.sample.contract_po_payment_terms_effective,
      payment_term_days: preview.sample.payment_term_days,
      payment_time_days: preview.sample.payment_time_days,
    });
    expect(stored).toMatchObject({
      payerEntityName: "Veolia Water Technologies 2 Pty Ltd",
      payerEntityAbn: "30 616 561 829",
      payeeEntityName: "ILLAWARRA GROUNDS & SURROUNDS",
      payeeEntityAbn: "22600186310",
      invoiceReferenceNumber: "4000014744",
      sourceAccountCode: "1010012308",
      documentType: "RE",
      clearingDocument: "5000009966",
      paymentAmount: "-9229.02",
      paymentDate: "2026-01-02",
      invoiceIssueDate: "2025-12-08",
      invoiceReceiptDate: "2025-12-08",
      invoiceDueDate: "2026-01-01",
      entryDate: "2025-12-09",
      sourceUser: "ARIBA_CIG",
      documentCurrency: "AUD",
      paymentTermRaw: "CHANGED",
      paymentTermDays: 45,
      paymentTimeDays: preview.sample.payment_time_days,
      canonicalRevisionId: "revision-1",
      canonicalSourceRowId: "row-revision-1",
      datasetId: "dataset-1",
      sourceRowNo: 1,
      rowNo: 1,
      semanticKind: "accounting_event",
    });
  });

  test("deterministically unions two revisions with overlapping source row numbers", async () => {
    const persistedRows = [];
    const dependencies = makeDependencies(persistedRows);
    dependencies.resolveCurrentCanonicalRevisions.mockResolvedValue([
      {
        dataset: { id: "dataset-1" }, datasetOrder: 0,
        revision: { id: "revision-1", materialSignature: "sig-1", rowCount: 1 },
      },
      {
        dataset: { id: "dataset-2" }, datasetOrder: 1,
        revision: { id: "revision-2", materialSignature: "sig-2", rowCount: 1 },
      },
    ]);
    const result = await stagePtrs({ ...dependencies, persist: true });
    expect(result.persistedCount).toBe(2);
    expect(persistedRows.map((row) => [row.datasetId, row.sourceRowNo, row.rowNo]))
      .toEqual([["dataset-1", 1, 1], ["dataset-2", 1, 2]]);
    expect(persistedRows.map((row) => row.canonicalRevisionId))
      .toEqual(["revision-1", "revision-2"]);
  });

  test("initial build and rebuild replace the current Stage population", async () => {
    const persistedRows = [];
    const initialDependencies = makeDependencies(persistedRows);
    const rebuiltDependencies = makeDependencies(persistedRows);

    const initial = await stagePtrs({
      ...initialDependencies,
      persist: true,
    });
    expect(initial.persistedCount).toBe(1);
    expect(persistedRows).toHaveLength(1);

    const rebuilt = await stagePtrs({
      ...rebuiltDependencies,
      persist: true,
    });
    expect(rebuilt.persistedCount).toBe(1);
    expect(persistedRows).toHaveLength(1);
    expect(rebuiltDependencies.db.PtrsStageRow.destroy).toHaveBeenCalledWith({
      where: {
        customerId: "customer-1",
        ptrsId: "ptrs-1",
        profileId: "profile-1",
      },
      force: true,
      transaction: expect.any(Object),
    });
    expect(rebuiltDependencies.db.PtrsStageRow.destroy.mock.invocationCallOrder[0])
      .toBeLessThan(
        rebuiltDependencies.db.PtrsStageRow.bulkCreate.mock.invocationCallOrder[0],
      );
    const rebuiltTransaction = await rebuiltDependencies
      .beginTransactionWithCustomerContext.mock.results[0].value;
    expect(rebuiltTransaction).toMatchObject({ finished: "commit" });
    expect(rebuiltTransaction.commit).toHaveBeenCalledTimes(1);
  });

  test("rolls back the Stage replacement when rebuilt rows cannot be persisted", async () => {
    const persistedRows = [{ id: "existing-stage-row" }];
    const dependencies = makeDependencies(persistedRows);
    dependencies.db.PtrsStageRow.bulkCreate.mockRejectedValue(
      new Error("persist failed"),
    );

    await expect(stagePtrs({ ...dependencies, persist: true })).rejects.toThrow(
      "persist failed",
    );

    const transaction = await dependencies
      .beginTransactionWithCustomerContext.mock.results[0].value;
    expect(dependencies.db.PtrsStageRow.destroy).toHaveBeenCalledWith(
      expect.objectContaining({ force: true, transaction }),
    );
    expect(transaction.rollback).toHaveBeenCalledTimes(1);
    expect(transaction.commit).not.toHaveBeenCalled();
  });

  test("parses ISO and Australian date-only values deterministically", () => {
    expect(parseISODateOnly("2026-01-02")?.iso).toBe("2026-01-02");
    expect(parseISODateOnly("02/01/2026")?.iso).toBe("2026-01-02");
    expect(parseISODateOnly("31/02/2026")).toBeNull();
  });

  test.each([
    ["9,229.02", "9229.02"],
    ["-9,229.02", "-9229.02"],
    ["9229.02", "9229.02"],
    [9229.02, 9229.02],
    ["0", "0"],
    [0, 0],
    ["", null],
    [null, null],
  ])("projects decimal value %p as %p", (paymentAmount, expected) => {
    expect(
      buildStageColumnProjection(
        { payment_amount: paymentAmount },
        {
          rawAttributes: {
            paymentAmount: { type: { key: "DECIMAL" } },
          },
        },
      ),
    ).toEqual({ paymentAmount: expected });
  });

  test.each(["abc", "9,22x.02", "9,22.02"])(
    "rejects invalid decimal value %p",
    (paymentAmount) => {
      expect(() =>
        buildStageColumnProjection(
          { payment_amount: paymentAmount },
          {
            rawAttributes: {
              paymentAmount: { type: { key: "DECIMAL" } },
            },
          },
        ),
      ).toThrow('Invalid numeric value for PTRS stage column "paymentAmount"');
    },
  );

  test("normalises integer columns without changing non-numeric projection", () => {
    expect(
      buildStageColumnProjection(
        {
          payment_term_days: "45",
          payment_time_days: 30,
          description: "Invoice 123",
        },
        {
          rawAttributes: {
            paymentTermDays: { type: { key: "INTEGER" } },
            paymentTimeDays: { type: { key: "INTEGER" } },
            description: { type: { key: "STRING" } },
          },
        },
      ),
    ).toEqual({
      paymentTermDays: 45,
      paymentTimeDays: 30,
      description: "Invoice 123",
    });
  });
});

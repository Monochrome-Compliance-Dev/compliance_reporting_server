jest.mock("@/db/database", () => ({
  sequelize: { query: jest.fn() },
}));
jest.mock("@/helpers/logger", () => ({
  logger: { info: jest.fn() },
}));
jest.mock("./ptrs.service", () => ({
  safeMeta: (value) => value,
  slog: {
    debug: jest.fn(),
    error: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
  },
}));

const {
  applyEffectiveTermChangesToRows,
  deriveTermReferenceDate,
  extractTermChangesJoinSpec,
  loadEffectiveTermChangesForRows,
} = require("./stage.payment-terms.ptrs.service");
const { transformStageRows } = require("./stage.build.ptrs.service");

const MAIN_DATASET_ID = "main-dataset";
const TERM_CHANGES_DATASET_ID = "term-changes-dataset";

function makeJoin(from, to) {
  return { from, to };
}

function makeContext() {
  return {
    customerId: "customer-1",
    ptrsId: "ptrs-1",
    datasets: [
      { id: MAIN_DATASET_ID, role: "main_csv" },
      { id: "vendor-dataset", role: "vendormaster" },
      { id: "entity-dataset", role: "entitystructure" },
      { id: TERM_CHANGES_DATASET_ID, role: "termschanges" },
    ],
    fieldMapRows: [
      {
        datasetId: MAIN_DATASET_ID,
        sourceRole: "main_csv",
        sourceColumn: "Account",
        canonicalField: "source_account_code",
      },
    ],
  };
}

function makeTermChangeMapRow() {
  return {
    joins: {
      conditions: [
        makeJoin(
          {
            datasetId: MAIN_DATASET_ID,
            role: "main_csv",
            column: "Account",
          },
          {
            datasetId: TERM_CHANGES_DATASET_ID,
            role: "termschanges",
            column: "Supplier",
          },
        ),
      ],
    },
  };
}

describe("PTRS effective-term join resolution", () => {
  beforeEach(() => {
    require("@/db/database").sequelize.query.mockReset();
  });

  test("selects the configured term-changes relationship from multiple joins", () => {
    const mapRow = {
      joins: {
        conditions: [
          makeJoin(
            {
              datasetId: MAIN_DATASET_ID,
              role: "main_csv",
              column: "Account",
            },
            {
              datasetId: "vendor-dataset",
              role: "vendormaster",
              column: "Supplier",
            },
          ),
          makeJoin(
            {
              datasetId: MAIN_DATASET_ID,
              role: "main_csv",
              column: "Company Code",
            },
            {
              datasetId: "entity-dataset",
              role: "entitystructure",
              column: "Company Code",
            },
          ),
          makeJoin(
            {
              datasetId: MAIN_DATASET_ID,
              role: "main_csv",
              column: "Account",
            },
            {
              datasetId: TERM_CHANGES_DATASET_ID,
              role: "support",
              column: "Supplier",
            },
          ),
        ],
      },
    };

    expect(extractTermChangesJoinSpec(mapRow, makeContext())).toEqual([
      {
        mainField: "source_account_code",
        changeColumn: "supplier",
      },
    ]);
  });

  test("uses invoice issue date only as the effective-term reference", () => {
    expect(
      deriveTermReferenceDate({
        invoice_issue_date: "2026-03-04",
        invoice_receipt_date: "2026-03-10",
      }),
    ).toBe("2026-03-04");
    expect(
      deriveTermReferenceDate({ invoice_receipt_date: "2026-03-10" }),
    ).toBeNull();
  });

  test("does not apply a future change retrospectively", () => {
    const rows = [
      {
        source_account_code: "SUP-1",
        invoice_issue_date: "2026-03-04",
        invoice_payment_terms_effective: "0027",
      },
    ];

    applyEffectiveTermChangesToRows(
      rows,
      new Map([["SUP-1::2026-03-25", { term: "NT60", changedAt: "2026-03-06" }]]),
      makeTermChangeMapRow(),
      makeContext(),
    );

    expect(rows[0]).toMatchObject({ invoice_payment_terms_effective: "0027" });
    expect(rows[0].contract_po_payment_terms_effective).toBeUndefined();
  });

  test.each([
    ["2026-03-06", "on the invoice issue date"],
    ["2026-03-25", "before the invoice issue date"],
  ])("applies a change effective %s (%s)", (invoiceIssueDate) => {
    const rows = [
      {
        source_account_code: "SUP-1",
        invoice_issue_date: invoiceIssueDate,
        invoice_payment_terms_effective: "0027",
      },
    ];
    const changeMap = new Map([
      [
        `SUP-1::${invoiceIssueDate}`,
        { term: "NT60", changedAt: "2026-03-06" },
      ],
    ]);

    applyEffectiveTermChangesToRows(
      rows,
      changeMap,
      makeTermChangeMapRow(),
      makeContext(),
    );

    expect(rows[0]).toMatchObject({
      contract_po_payment_terms_effective: "NT60",
      contract_po_payment_terms_effective_source: "TERM_CHANGES",
      contract_po_payment_terms_effective_changed_at: "2026-03-06",
    });
  });

  test("keeps each invoice-date selection when multiple historical changes exist", async () => {
    const db = require("@/db/database");
    db.sequelize.query.mockResolvedValue([
      {
        supplier: "SUP-1",
        refDate: "2026-02-01",
        newRaw: "NT30",
        changedAt: "2026-01-15",
      },
      {
        supplier: "SUP-1",
        refDate: "2026-04-01",
        newRaw: "NT60",
        changedAt: "2026-03-06",
      },
    ]);
    const rows = [
      { source_account_code: "SUP-1", invoice_issue_date: "2026-02-01" },
      { source_account_code: "SUP-1", invoice_issue_date: "2026-04-01" },
    ];

    const changeMap = await loadEffectiveTermChangesForRows({
      customerId: "customer-1",
      profileId: "profile-1",
      rows,
      mapRow: makeTermChangeMapRow(),
      joinContext: makeContext(),
      transaction: {},
    });
    applyEffectiveTermChangesToRows(
      rows,
      changeMap,
      makeTermChangeMapRow(),
      makeContext(),
    );

    expect(rows.map((row) => row.contract_po_payment_terms_effective)).toEqual([
      "NT30",
      "NT60",
    ]);
    expect(db.sequelize.query.mock.calls[0][0]).toContain(
      'AND (c."changedAt"::date) <= i."refDate"',
    );
    expect(db.sequelize.query.mock.calls[0][0]).toContain(
      'ORDER BY c."changedAt" DESC',
    );
  });

  test("preserves the invoice term when there is no applicable historical change", () => {
    const rows = [
      {
        source_account_code: "SUP-1",
        invoice_issue_date: "2025-12-01",
        invoice_payment_terms_effective: "0027",
      },
    ];

    applyEffectiveTermChangesToRows(
      rows,
      new Map(),
      makeTermChangeMapRow(),
      makeContext(),
    );

    expect(rows[0].invoice_payment_terms_effective).toBe("0027");
    expect(rows[0].contract_po_payment_terms_effective_source).toBeUndefined();
  });

  test("records history only for the effective change selected for that invoice", async () => {
    const rows = [
      {
        source_account_code: "SUP-1",
        invoice_issue_date: "2026-03-04",
        invoice_payment_terms_effective: "0027",
      },
      {
        source_account_code: "SUP-1",
        invoice_issue_date: "2026-03-25",
        invoice_payment_terms_effective: "0027",
      },
    ];
    const changeMap = new Map([
      [
        "SUP-1::2026-03-25",
        { term: "NT60", changedAt: "2026-03-06" },
      ],
    ]);

    const result = await transformStageRows({
      rows,
      rowRules: [],
      customerId: "customer-1",
      profileId: "profile-1",
      mapRow: makeTermChangeMapRow(),
      joinContext: makeContext(),
      transaction: {},
      applyRules: (inputRows) => ({ rows: inputRows, stats: {} }),
      loadEffectiveTermChangesForRows: async () => changeMap,
      applyEffectiveTermChangesToRows,
      termMap: new Map(),
      applyPaymentTermDaysFromMap: (inputRows) => ({
        rows: inputRows,
        stats: {},
      }),
      computePaymentTimeRegulator: () => ({ days: null }),
    });

    expect(result.rows[0]._transformationMeta).toBeUndefined();
    expect(
      result.rows[1]._transformationMeta.transformationHistory,
    ).toEqual([
      expect.objectContaining({
        key: "payment-term-change:2026-03-06:NT60",
        kind: "payment_term_override",
        details: {
          previousTerm: "0027",
          effectiveTerm: "NT60",
          effectiveDate: "2026-03-06",
          source: "TERM_CHANGES",
        },
      }),
    ]);
  });

  test("resolves main and support headers when the saved edge is reversed", () => {
    const mapRow = {
      joins: {
        conditions: [
          makeJoin(
            {
              datasetId: TERM_CHANGES_DATASET_ID,
              role: "termschanges",
              column: "Supplier",
            },
            {
              datasetId: MAIN_DATASET_ID,
              role: "main_csv",
              column: "Account",
            },
          ),
        ],
      },
    };

    expect(extractTermChangesJoinSpec(mapRow, makeContext())).toEqual([
      {
        mainField: "source_account_code",
        changeColumn: "supplier",
      },
    ]);
  });

  test("throws clearly when the configured relationship cannot be resolved", async () => {
    const mapRow = {
      joins: {
        conditions: [
          makeJoin(
            {
              datasetId: MAIN_DATASET_ID,
              role: "main_csv",
              column: "Unmapped account header",
            },
            {
              datasetId: TERM_CHANGES_DATASET_ID,
              role: "termschanges",
              column: "Supplier",
            },
          ),
        ],
      },
    };

    await expect(
      loadEffectiveTermChangesForRows({
        customerId: "customer-1",
        profileId: "profile-1",
        rows: [
          {
            source_account_code: "supplier-1",
            invoice_issue_date: "2026-01-01",
          },
        ],
        mapRow,
        joinContext: makeContext(),
        transaction: {},
      }),
    ).rejects.toThrow(
      "Effective-dated payment term changes require an explicit resolvable join spec",
    );
  });
});

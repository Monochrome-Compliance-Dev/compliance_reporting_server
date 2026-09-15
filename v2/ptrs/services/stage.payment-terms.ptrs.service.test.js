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
  applyPaymentTermDays,
  deriveTermReferenceDate,
  extractTermChangesJoinSpec,
  inferTermDaysFromCode,
  loadEffectiveTermChangesForRows,
} = require("./stage.payment-terms.ptrs.service");
const { transformStageRows } = require("./stage.build.ptrs.service");

const TRANSACTION_DATASET_ID = "transaction-dataset";
const TERM_CHANGES_DATASET_ID = "term-changes-dataset";

function makeJoin(from, to) {
  return { from, to };
}

function makeContext() {
  return {
    customerId: "customer-1",
    ptrsId: "ptrs-1",
    transactionDatasetId: TRANSACTION_DATASET_ID,
    datasets: [
      {
        id: TRANSACTION_DATASET_ID,
        purpose: "transaction",
        referenceKind: null,
      },
      {
        id: "vendor-dataset",
        purpose: "reference",
        referenceKind: "vendormaster",
      },
      {
        id: "entity-dataset",
        purpose: "reference",
        referenceKind: "entitystructure",
      },
      {
        id: TERM_CHANGES_DATASET_ID,
        purpose: "reference",
        referenceKind: "termschanges",
      },
    ],
    fieldMapRows: [
      {
        datasetId: TRANSACTION_DATASET_ID,
        sourceRole: "transaction",
        sourceColumn: "Account",
        canonicalField: "sourceAccountCode",
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
            datasetId: TRANSACTION_DATASET_ID,
            role: "transaction",
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
              datasetId: TRANSACTION_DATASET_ID,
              role: "transaction",
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
              datasetId: TRANSACTION_DATASET_ID,
              role: "transaction",
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
              datasetId: TRANSACTION_DATASET_ID,
              role: "transaction",
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
        transactionField: "sourceAccountCode",
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

  test("returns no term changes when the transaction has no configured term-change join", async () => {
    await expect(
      loadEffectiveTermChangesForRows({
        customerId: "customer-1",
        profileId: "profile-1",
        rows: [{ payment_term: "E62", invoice_issue_date: "2026-03-04" }],
        mapRow: { joins: { conditions: [] } },
        joinContext: makeContext(),
        transaction: {},
      }),
    ).resolves.toEqual(new Map());
    expect(require("@/db/database").sequelize.query).not.toHaveBeenCalled();
  });

  test("derives a joinless raw term through the generic Stage rule", async () => {
    const rows = [
      {
        payment_term: "E62",
        payment_term_days: null,
        invoice_issue_date: "2026-03-04",
      },
    ];

    const result = await transformStageRows({
      rows,
      rowRules: [],
      customerId: "customer-1",
      profileId: "profile-1",
      mapRow: { joins: { conditions: [] } },
      joinContext: makeContext(),
      transaction: {},
      applyRules: (inputRows) => ({ rows: inputRows, stats: {} }),
      loadEffectiveTermChangesForRows,
      applyEffectiveTermChangesToRows,
      termMap: new Map(),
      applyPaymentTermDays,
      computePaymentTimeRegulator: () => ({ days: null }),
      semanticKind: "direct_payment",
    });

    expect(result.rows[0]).toMatchObject({
      payment_term: "E62",
      payment_term_days: 62,
    });
    expect(result.rows[0]._stageErrors || []).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ code: "PAYMENT_TERM_UNMAPPED" }),
      ]),
    );
    expect(require("@/db/database").sequelize.query).not.toHaveBeenCalled();
  });

  test.each([
    ["45", 45],
    ["0040", 40],
    ["Y14F", 14],
    ["E61", 61],
    ["30I", 30],
    ["COD", 0],
    [" cod ", 0],
    ["", null],
    ["UNSUPPORTED", null],
  ])("derives payment-term code %p as %p", (sourceTerm, expected) => {
    expect(inferTermDaysFromCode(sourceTerm)).toBe(expected);
  });

  test("preserves raw evidence while deriving typed payment-term days", () => {
    const rows = [
      { invoice_payment_terms: "Y14F", payment_term_days: null },
      { invoice_payment_terms: "COD", payment_term_days: null },
      { invoice_payment_terms: "UNSUPPORTED", payment_term_days: null },
      { invoice_payment_terms: "  ", payment_term_days: null },
    ];

    const result = applyPaymentTermDays(rows);

    expect(rows.map((row) => row.invoice_payment_terms)).toEqual([
      "Y14F",
      "COD",
      "UNSUPPORTED",
      "  ",
    ]);
    expect(rows.map((row) => row.payment_term_days)).toEqual([
      14,
      0,
      null,
      null,
    ]);
    expect(result.stats).toMatchObject({
      filled: 2,
      derived: 2,
      mapped: 0,
      missing: 2,
      unmapped: 1,
    });
    expect(rows[2]._stageErrors).toEqual([
      expect.objectContaining({
        code: "PAYMENT_TERM_UNMAPPED",
        value: "UNSUPPORTED",
      }),
    ]);
    expect(rows[3]._stageErrors).toEqual([
      expect.objectContaining({ code: "PAYMENT_TERM_MISSING" }),
    ]);
  });

  test("numeric source evidence takes precedence over a conflicting map", () => {
    const rows = [{ invoice_payment_terms: "E61" }];

    applyPaymentTermDays(rows, new Map([["E61", 30]]));

    expect(rows[0]).toMatchObject({
      invoice_payment_terms: "E61",
      payment_term_days: 61,
    });
  });

  test("retains configured mappings for supported nonnumeric terms", () => {
    const rows = [{ invoice_payment_terms: "ON_DELIVERY" }];

    const result = applyPaymentTermDays(
      rows,
      new Map([["ON_DELIVERY", 0]]),
    );

    expect(rows[0].payment_term_days).toBe(0);
    expect(result.stats).toMatchObject({ filled: 1, derived: 0, mapped: 1 });
  });

  test("does not apply a future change retrospectively", () => {
    const rows = [
      {
        sourceAccountCode: "SUP-1",
        invoice_issue_date: "2026-03-04",
        invoice_payment_terms_effective: "0027",
      },
    ];

    applyEffectiveTermChangesToRows(
      rows,
      new Map([
        ["SUP-1::2026-03-25", { term: "NT60", changedAt: "2026-03-06" }],
      ]),
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
        sourceAccountCode: "SUP-1",
        invoice_issue_date: invoiceIssueDate,
        invoice_payment_terms_effective: "0027",
      },
    ];
    const changeMap = new Map([
      [`SUP-1::${invoiceIssueDate}`, { term: "NT60", changedAt: "2026-03-06" }],
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
      { sourceAccountCode: "SUP-1", invoice_issue_date: "2026-02-01" },
      { sourceAccountCode: "SUP-1", invoice_issue_date: "2026-04-01" },
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
        sourceAccountCode: "SUP-1",
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

  test("retains selected effective change evidence without Stage history", async () => {
    const rows = [
      {
        sourceAccountCode: "SUP-1",
        invoice_issue_date: "2026-03-04",
        invoice_payment_terms_effective: "0027",
      },
      {
        sourceAccountCode: "SUP-1",
        invoice_issue_date: "2026-03-25",
        invoice_payment_terms_effective: "0027",
      },
    ];
    const changeMap = new Map([
      ["SUP-1::2026-03-25", { term: "NT60", changedAt: "2026-03-06" }],
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
      applyPaymentTermDays: (inputRows) => ({
        rows: inputRows,
        stats: {},
      }),
      computePaymentTimeRegulator: () => ({ days: null }),
    });

    expect(result.rows[0]._transformationMeta).toBeUndefined();
    expect(result.rows[1]._transformationMeta).toBeUndefined();
    expect(result.rows[1]).toMatchObject({
      contract_po_payment_terms_effective: "NT60",
      contract_po_payment_terms_effective_changed_at: "2026-03-06",
      contract_po_payment_terms_effective_source: "TERM_CHANGES",
    });
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
              datasetId: TRANSACTION_DATASET_ID,
              role: "transaction",
              column: "Account",
            },
          ),
        ],
      },
    };

    expect(extractTermChangesJoinSpec(mapRow, makeContext())).toEqual([
      {
        transactionField: "sourceAccountCode",
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
              datasetId: TRANSACTION_DATASET_ID,
              role: "transaction",
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
            sourceAccountCode: "supplier-1",
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

  test("throws clearly when the governed join field is absent from the row shape", async () => {
    await expect(
      loadEffectiveTermChangesForRows({
        customerId: "customer-1",
        profileId: "profile-1",
        rows: [
          {
            Account: "SUP-1",
            invoice_issue_date: "2026-01-01",
          },
        ],
        mapRow: makeTermChangeMapRow(),
        joinContext: makeContext(),
        transaction: {},
      }),
    ).rejects.toThrow(
      "Term changes join requires staged fields missing from row shape: sourceAccountCode",
    );
  });
});

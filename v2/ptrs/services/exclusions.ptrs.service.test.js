jest.mock("@/db/database", () => ({
  sequelize: { query: jest.fn() },
}));
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(),
}));
jest.mock("./ptrs.service", () => ({
  slog: {
    info: jest.fn(),
  },
}));
jest.mock("./exclusions.summary", () => ({
  getExclusionsSummary: jest.fn(),
}));
jest.mock("./exclusions.keywords", () => ({
  listKeywordExclusions: jest.fn(),
  createKeywordExclusion: jest.fn(),
  updateKeywordExclusion: jest.fn(),
  deleteKeywordExclusion: jest.fn(),
}));
jest.mock("./exclusions.keyword.engine", () => ({
  applyKeywordExclusion: jest.fn(),
  previewKeywordExclusion: jest.fn(),
}));
jest.mock("./exclusions.docType", () => ({
  applyDocTypeExclusion: jest.fn(),
  previewDocTypeExclusion: jest.fn(),
}));
jest.mock("./exclusions.creditApplied", () => ({
  applyCreditAppliedExclusion: jest.fn(),
  previewCreditAppliedExclusion: jest.fn(),
}));
jest.mock("./exclusions.employee", () => ({
  applyEmployeeExclusion: jest.fn(),
  previewEmployeeExclusion: jest.fn(),
}));
jest.mock("./exclusions.gov", () => ({
  applyGovExclusion: jest.fn(),
  previewGovExclusion: jest.fn(),
}));
jest.mock("./exclusions.intraCompany", () => ({
  applyIntraCompanyExclusion: jest.fn(),
  previewIntraCompanyExclusion: jest.fn(),
}));
jest.mock("./exclusions.prepaid", () => ({
  applyPrepaidExclusion: jest.fn(),
  previewPrepaidExclusion: jest.fn(),
}));
jest.mock("./exclusions.international", () => ({
  applyInternationalExclusion: jest.fn(),
  previewInternationalExclusion: jest.fn(),
}));
jest.mock("./exclusions.gov.enrichment", () => ({
  enrichGovReferenceFromStageRows: jest.fn(),
}));

const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const db = require("@/db/database");
const { applyGovExclusion, previewGovExclusion } = require("./exclusions.gov");
const {
  applyCreditAppliedExclusion,
  previewCreditAppliedExclusion,
} = require("./exclusions.creditApplied");
const {
  enrichGovReferenceFromStageRows,
} = require("./exclusions.gov.enrichment");
const {
  applyExclusionsAndPersist,
  previewExclusions,
} = require("./exclusions.ptrs.service");

function makeTransaction() {
  return {
    finished: false,
    commit: jest.fn(async function commit() {
      this.finished = "commit";
    }),
    rollback: jest.fn(async function rollback() {
      this.finished = "rollback";
    }),
  };
}

describe("PTRS gov exclusions preflight", () => {
  beforeEach(() => {
    beginTransactionWithCustomerContext.mockReset();
    enrichGovReferenceFromStageRows.mockReset();
    applyGovExclusion.mockReset();
    previewGovExclusion.mockReset();
    applyCreditAppliedExclusion.mockReset();
    previewCreditAppliedExclusion.mockReset();
    db.sequelize.query.mockReset();
  });

  test("runs shared government enrichment before gov preview", async () => {
    const transaction = makeTransaction();
    beginTransactionWithCustomerContext.mockResolvedValue(transaction);
    enrichGovReferenceFromStageRows.mockResolvedValue({
      distinctStageAbnsToCheck: 1,
    });
    previewGovExclusion.mockImplementation(async () => ({
      matched: 4,
      alreadyExcluded: 1,
      sampleRows: [{ row_no: 1 }],
    }));

    await previewExclusions({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      category: "gov",
      limit: 10,
    });

    expect(enrichGovReferenceFromStageRows).toHaveBeenCalledWith({
      sequelize: expect.any(Object),
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });
    expect(previewGovExclusion).toHaveBeenCalledWith({
      sequelize: expect.any(Object),
      transaction,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      effectiveLimit: 10,
    });
    expect(
      enrichGovReferenceFromStageRows.mock.invocationCallOrder[0],
    ).toBeLessThan(
      beginTransactionWithCustomerContext.mock.invocationCallOrder[0],
    );
    expect(
      beginTransactionWithCustomerContext.mock.invocationCallOrder[0],
    ).toBeLessThan(previewGovExclusion.mock.invocationCallOrder[0]);
    expect(applyGovExclusion).not.toHaveBeenCalled();
  });

  test("runs shared government enrichment before gov apply", async () => {
    const transaction = makeTransaction();
    beginTransactionWithCustomerContext.mockResolvedValue(transaction);
    enrichGovReferenceFromStageRows.mockResolvedValue({
      distinctStageAbnsToCheck: 1,
    });
    applyGovExclusion.mockResolvedValue(7);

    const result = await applyExclusionsAndPersist({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      category: "gov",
    });

    expect(enrichGovReferenceFromStageRows).toHaveBeenCalledWith({
      sequelize: expect.any(Object),
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });
    expect(applyGovExclusion).toHaveBeenCalledWith({
      sequelize: expect.any(Object),
      transaction,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });
    expect(
      enrichGovReferenceFromStageRows.mock.invocationCallOrder[0],
    ).toBeLessThan(
      beginTransactionWithCustomerContext.mock.invocationCallOrder[0],
    );
    expect(
      beginTransactionWithCustomerContext.mock.invocationCallOrder[0],
    ).toBeLessThan(applyGovExclusion.mock.invocationCallOrder[0]);
    expect(result.persisted).toBe(7);
  });

  test("does not scan or rewrite Stage rows to stamp profile metadata", async () => {
    const transaction = makeTransaction();
    beginTransactionWithCustomerContext.mockResolvedValue(transaction);
    applyCreditAppliedExclusion.mockResolvedValue(0);

    const result = await applyExclusionsAndPersist({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      profileId: "profile-1",
      category: "credit_applied",
    });

    expect(db.sequelize.query).not.toHaveBeenCalled();
    expect(result.stats).not.toHaveProperty("profileRowsStamped");
    expect(result.stats.timings).toEqual({
      creditAppliedMs: expect.any(Number),
    });
  });

  test("previews credit_applied independently", async () => {
    const transaction = makeTransaction();
    beginTransactionWithCustomerContext.mockResolvedValue(transaction);
    previewCreditAppliedExclusion.mockResolvedValue({
      matched: 4,
      alreadyExcluded: 1,
      sampleRows: [{ row_no: 2 }],
    });

    const response = await previewExclusions({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      category: "credit_applied",
      limit: 10,
    });

    expect(previewCreditAppliedExclusion).toHaveBeenCalledWith({
      sequelize: expect.any(Object),
      transaction,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      effectiveLimit: 10,
    });
    expect(response.result.counts.credit_applied).toBe(4);
    expect(response.result.alreadyExcludedCounts.credit_applied).toBe(1);
    expect(response.result.samples.credit_applied).toEqual([{ row_no: 2 }]);
  });

  test("applies credit_applied independently", async () => {
    const transaction = makeTransaction();
    beginTransactionWithCustomerContext.mockResolvedValue(transaction);
    applyCreditAppliedExclusion.mockResolvedValue(4);

    const response = await applyExclusionsAndPersist({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      category: "credit_applied",
    });

    expect(applyCreditAppliedExclusion).toHaveBeenCalledWith({
      sequelize: expect.any(Object),
      transaction,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });
    expect(response.persisted).toBe(4);
  });

  test("includes credit_applied when applying all exclusions", async () => {
    const transaction = makeTransaction();
    beginTransactionWithCustomerContext.mockResolvedValue(transaction);
    enrichGovReferenceFromStageRows.mockResolvedValue({});

    const applyMocks = [
      jest.requireMock("./exclusions.gov").applyGovExclusion,
      jest.requireMock("./exclusions.intraCompany").applyIntraCompanyExclusion,
      jest.requireMock("./exclusions.employee").applyEmployeeExclusion,
      jest.requireMock("./exclusions.docType").applyDocTypeExclusion,
      jest.requireMock("./exclusions.keyword.engine").applyKeywordExclusion,
      jest.requireMock("./exclusions.prepaid").applyPrepaidExclusion,
      jest.requireMock("./exclusions.international")
        .applyInternationalExclusion,
    ];
    for (const applyMock of applyMocks) {
      applyMock.mockResolvedValue(0);
    }
    applyCreditAppliedExclusion.mockResolvedValue(4);

    const response = await applyExclusionsAndPersist({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      category: "all",
    });

    expect(applyCreditAppliedExclusion).toHaveBeenCalledWith({
      sequelize: expect.any(Object),
      transaction,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });
    expect(response.persisted).toBe(4);
  });

  test("includes credit_applied when previewing all exclusions", async () => {
    const transaction = makeTransaction();
    beginTransactionWithCustomerContext.mockResolvedValue(transaction);
    enrichGovReferenceFromStageRows.mockResolvedValue({});

    const emptyPreview = {
      matched: 0,
      alreadyExcluded: 0,
      sampleRows: [],
    };
    const previewMocks = [
      jest.requireMock("./exclusions.gov").previewGovExclusion,
      jest.requireMock("./exclusions.intraCompany")
        .previewIntraCompanyExclusion,
      jest.requireMock("./exclusions.employee").previewEmployeeExclusion,
      jest.requireMock("./exclusions.docType").previewDocTypeExclusion,
      jest.requireMock("./exclusions.keyword.engine").previewKeywordExclusion,
      jest.requireMock("./exclusions.prepaid").previewPrepaidExclusion,
      jest.requireMock("./exclusions.international")
        .previewInternationalExclusion,
    ];
    for (const previewMock of previewMocks) {
      previewMock.mockResolvedValue(emptyPreview);
    }
    previewCreditAppliedExclusion.mockResolvedValue({
      matched: 4,
      alreadyExcluded: 1,
      sampleRows: [{ row_no: 2 }],
    });

    const response = await previewExclusions({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      category: "all",
      limit: 10,
    });

    expect(previewCreditAppliedExclusion).toHaveBeenCalledWith({
      sequelize: expect.any(Object),
      transaction,
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      effectiveLimit: 10,
    });
    expect(response.result.counts.credit_applied).toBe(4);
  });
});

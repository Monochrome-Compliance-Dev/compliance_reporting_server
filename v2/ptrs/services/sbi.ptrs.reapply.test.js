const mockStageRows = [];

jest.mock("@/db/database", () => ({
  PtrsSbiUpload: { findOne: jest.fn() },
  PtrsSbiResult: { findAll: jest.fn() },
  PtrsStageRow: {
    findAll: jest.fn(async () => mockStageRows),
  },
  PtrsSbiRowChange: { bulkCreate: jest.fn() },
}));
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(async () => ({
    finished: false,
    commit: jest.fn(function commit() {
      this.finished = "commit";
    }),
    rollback: jest.fn(),
  })),
}));

const db = require("@/db/database");
const { reapplyLatestResults } = require("./sbi.ptrs.service");

describe("reapplyLatestResults", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockStageRows.splice(0);
    db.PtrsSbiUpload.findOne.mockResolvedValue({
      id: "upload0001",
      status: "APPLIED",
    });
    db.PtrsSbiResult.findAll.mockResolvedValue([
      {
        abn: "12345678901",
        outcome: "Small business for payment times reporting",
        isValidAbn: true,
      },
    ]);
  });

  test("reapplies the latest evidence once and honours standard exclusions", async () => {
    const included = {
      id: "stage00001",
      rowNo: 1,
      data: { payee_entity_abn: "12 345 678 901" },
      meta: {},
      save: jest.fn(),
    };
    const excluded = {
      id: "stage00002",
      rowNo: 2,
      data: {
        payee_entity_abn: "12345678901",
        exclude_from_metrics: true,
      },
      meta: {},
      save: jest.fn(),
    };
    mockStageRows.push(included, excluded);

    const first = await reapplyLatestResults({
      customerId: "customer01",
      ptrsId: "ptrs000001",
      userId: "user000001",
    });
    const second = await reapplyLatestResults({
      customerId: "customer01",
      ptrsId: "ptrs000001",
      userId: "user000001",
    });

    expect(first.counts).toEqual(
      expect.objectContaining({
        affectedRows: 1,
        historyRows: 1,
        excludedRows: 1,
      }),
    );
    expect(second.counts).toEqual(
      expect.objectContaining({ affectedRows: 0, historyRows: 0 }),
    );
    expect(included.save).toHaveBeenCalledTimes(1);
    expect(excluded.save).not.toHaveBeenCalled();
    expect(included.meta.transformationHistory).toEqual([
      expect.objectContaining({
        key: "sbi:upload0001:true",
        sourceStageRowIds: ["stage00001"],
        targetStageRowIds: ["stage00001"],
      }),
    ]);
    expect(db.PtrsSbiRowChange.bulkCreate).toHaveBeenCalledTimes(1);
  });
});

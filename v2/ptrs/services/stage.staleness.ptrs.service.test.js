const mockQuery = jest.fn();
const mockFindOne = jest.fn();

jest.mock("@/db/database", () => ({
  sequelize: { query: mockQuery },
  PtrsColumnMap: { findOne: mockFindOne },
}));

jest.mock("@/v2/ptrs/services/maps.staleness.ptrs.service", () => ({
  buildMaterialMapSignature: jest.fn(() => "map-signature"),
}));

jest.mock("@/v2/ptrs/services/canonical.ptrs.service", () => ({
  resolveCurrentCanonicalRevisions: jest.fn(),
}));

const { buildStageInputSnapshot } = require("./stage.staleness.ptrs.service");

describe("PTRS Stage input snapshot database access", () => {
  test("aggregates term state once before reading Stage configuration", async () => {
    const transaction = { id: "transaction-1" };
    mockQuery.mockResolvedValueOnce([
      {
        paymentTermMapCount: 21,
        paymentTermMapUpdatedAt: "2026-08-28T00:00:00.000Z",
        paymentTermChangeCount: 3,
        paymentTermChangeUpdatedAt: "2026-08-28T01:00:00.000Z",
      },
    ]);
    mockFindOne.mockResolvedValueOnce({
      id: "column-map-1",
      mappings: [],
      joins: [],
      customFields: [],
      rowRules: [],
      updatedAt: "2026-08-28T02:00:00.000Z",
    });

    const snapshot = await buildStageInputSnapshot({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      profileId: "profile-1",
      canonicalSelections: [],
      transaction,
    });

    expect(mockQuery).toHaveBeenCalledTimes(1);
    expect(mockQuery.mock.calls[0][0]).toContain("payment_term_map_stats AS");
    expect(mockQuery.mock.calls[0][0]).toContain(
      "payment_term_change_stats AS",
    );
    expect(mockQuery.mock.invocationCallOrder[0]).toBeLessThan(
      mockFindOne.mock.invocationCallOrder[0],
    );
    expect(snapshot.paymentTermMap).toEqual({
      profileId: "profile-1",
      count: 21,
      maxUpdatedAt: "2026-08-28T00:00:00.000Z",
    });
    expect(snapshot.paymentTermChanges).toEqual({
      profileId: "profile-1",
      count: 3,
      maxUpdatedAt: "2026-08-28T01:00:00.000Z",
    });
  });
});

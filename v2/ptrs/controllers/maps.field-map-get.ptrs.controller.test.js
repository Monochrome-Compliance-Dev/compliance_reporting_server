const mockFieldMap = [
  {
    canonicalField: "paymentAmount",
    sourceRole: "transaction",
    datasetId: "transaction-1",
  },
  {
    canonicalField: "payeeEntityAbn",
    sourceRole: "vendormaster",
    datasetId: "vendor-1",
  },
];

const mockGetPtrs = jest.fn(async () => ({ id: "ptrs-1" }));
const mockGetFieldMap = jest.fn(async () => mockFieldMap);
const mockLogEvent = jest.fn(async () => undefined);

jest.mock("@/audit/audit.service", () => ({ logEvent: mockLogEvent }));
jest.mock("@/helpers/logger", () => ({
  logger: { logEvent: jest.fn() },
}));
jest.mock("@/v2/ptrs/controllers/ptrs.controller", () => ({
  safeLog: jest.fn(),
}));
jest.mock("@/v2/ptrs/services/ptrs.service", () => ({
  getPtrs: mockGetPtrs,
  safeMeta: (value) => value,
  slog: { info: jest.fn(), error: jest.fn() },
}));
jest.mock("@/v2/ptrs/services/maps.config.ptrs.service", () => ({
  getFieldMap: mockGetFieldMap,
}));
jest.mock("@/v2/ptrs/services/maps.headers.ptrs.service", () => ({}));

const { getFieldMap } = require("./maps.ptrs.controller");

function makeResponse() {
  return {
    status: jest.fn(function status() {
      return this;
    }),
    json: jest.fn(function json() {
      return this;
    }),
  };
}

describe("GET PTRS field map", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test("returns the complete profile-scoped map without datasetId", async () => {
    const req = {
      effectiveCustomerId: "customer-1",
      auth: { id: "user-1" },
      ip: "127.0.0.1",
      headers: { "user-agent": "jest" },
      params: { id: "ptrs-1" },
      query: { profileId: "profile-1" },
    };
    const res = makeResponse();
    const next = jest.fn();

    await getFieldMap(req, res, next);

    expect(next).not.toHaveBeenCalled();
    expect(res.status).toHaveBeenCalledWith(200);
    expect(res.json).toHaveBeenCalledWith({
      status: "success",
      data: { fieldMap: mockFieldMap },
    });
    expect(mockGetPtrs).toHaveBeenCalledWith({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
    });
    expect(mockGetFieldMap).toHaveBeenCalledWith({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      profileId: "profile-1",
    });
    expect(mockFieldMap.map((row) => row.datasetId)).toEqual([
      "transaction-1",
      "vendor-1",
    ]);
  });
});

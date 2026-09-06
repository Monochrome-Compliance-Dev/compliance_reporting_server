jest.mock("@/audit/audit.service", () => ({
  logEvent: jest.fn(async () => {}),
}));
jest.mock("@/v2/ptrs/services/ptrs.service", () => ({
  getPtrs: jest.fn(async () => ({ profileId: "profile001" })),
}));
jest.mock("@/v2/ptrs/services/canonical.ptrs.service", () => ({
  materializeCanonicalRevision: jest.fn(),
}));

const service = require("../services/canonical.ptrs.service");
const audit = require("@/audit/audit.service");
const { materializeRevision } = require("./canonical.ptrs.controller");

test.each([
  ["building", 202, "PtrsV2CanonicalRevisionInProgress"],
  ["succeeded", 200, "PtrsV2CanonicalRevisionMaterialised"],
])(
  "%s returns the existing revision shape with HTTP %s",
  async (status, httpStatus, action) => {
    const result = {
      revision: {
        id: "revision01",
        status,
        rowCount: status === "succeeded" ? 2000 : null,
      },
      reused: true,
    };
    service.materializeCanonicalRevision.mockResolvedValue(result);
    const req = {
      id: "request01",
      effectiveCustomerId: "customer01",
      params: { id: "ptrs000001", datasetId: "dataset001" },
      body: { profileId: "profile001" },
      auth: { id: "user000001" },
      headers: {},
    };
    const res = { status: jest.fn().mockReturnThis(), json: jest.fn() };
    const next = jest.fn();
    await materializeRevision(req, res, next);
    expect(next).not.toHaveBeenCalled();
    expect(service.materializeCanonicalRevision).toHaveBeenCalledWith(
      expect.objectContaining({
        requestId: "request01",
        customerId: "customer01",
      }),
    );
    expect(res.status).toHaveBeenCalledWith(httpStatus);
    expect(res.json).toHaveBeenCalledWith({ status: "success", data: result });
    expect(audit.logEvent).toHaveBeenCalledWith(
      expect.objectContaining({ action }),
    );
  },
);

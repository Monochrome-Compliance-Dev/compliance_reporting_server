jest.mock("@/platform/audit/audit.service", () => ({
  recordFoundationAudit: jest.fn(),
}));

const auditService = require("@/platform/audit/audit.service");
const foundationService = require("@/platform/foundation/foundation.service");

describe("foundation.service", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    auditService.recordFoundationAudit.mockResolvedValue({
      eventType: "platform.foundation.executed",
    });
  });

  describe("executeFoundation", () => {
    it("returns a successful foundation envelope using req.auth after Audit succeeds", async () => {
      const req = {
        auth: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        ip: "127.0.0.1",
        headers: {
          "user-agent": "jest-agent",
        },
      };

      const result = await foundationService.executeFoundation(req);

      expect(result).toEqual({
        success: true,
        foundationId: expect.any(String),
        capability: "foundation",
        message: "Platform foundation executed successfully.",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
      });

      expect(auditService.recordFoundationAudit).toHaveBeenCalledWith({
        foundationId: result.foundationId,
        capability: "foundation",
        outcome: "success",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        request: req,
      });
    });

    it("returns a successful foundation envelope using req.user", async () => {
      const result = await foundationService.executeFoundation({
        user: {
          userId: "user-456",
          role: "User",
          customerId: "customer-456",
        },
      });

      expect(result).toEqual({
        success: true,
        foundationId: expect.any(String),
        capability: "foundation",
        message: "Platform foundation executed successfully.",
        actor: {
          id: "user-456",
          role: "User",
          customerId: "customer-456",
        },
      });
    });

    it("returns a successful foundation envelope using req.currentUser", async () => {
      const result = await foundationService.executeFoundation({
        currentUser: {
          id: "user-789",
          role: "Boss",
          customerId: "customer-789",
        },
      });

      expect(result).toEqual({
        success: true,
        foundationId: expect.any(String),
        capability: "foundation",
        message: "Platform foundation executed successfully.",
        actor: {
          id: "user-789",
          role: "Boss",
          customerId: "customer-789",
        },
      });
    });

    it("throws a 401 error when authenticated user context is missing", async () => {
      await expect(foundationService.executeFoundation({})).rejects.toThrow(
        "Authenticated user context is required.",
      );

      try {
        await foundationService.executeFoundation({});
      } catch (error) {
        expect(error.status).toBe(401);
      }
    });

    it("fails the foundation when Audit persistence fails", async () => {
      auditService.recordFoundationAudit.mockRejectedValue(
        new Error("audit persistence failed"),
      );

      await expect(
        foundationService.executeFoundation({
          auth: {
            id: "user-123",
            role: "Admin",
            customerId: "customer-123",
          },
        }),
      ).rejects.toThrow("audit persistence failed");
    });
  });
});

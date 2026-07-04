jest.mock("@/platform/audit/audit.service", () => ({
  recordInteractionAudit: jest.fn(),
}));

const auditService = require("@/platform/audit/audit.service");
const interactionsService = require("@/platform/interactions/interactions.service");

describe("interactions.service", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    auditService.recordInteractionAudit.mockResolvedValue({
      eventType: "platform.interaction.executed",
    });
  });

  describe("executeInteraction", () => {
    it("returns a successful interaction envelope using req.auth after Audit succeeds", async () => {
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

      const result = await interactionsService.executeInteraction(req);

      expect(result).toEqual({
        success: true,
        interactionId: expect.any(String),
        capability: "interactions",
        message: "Platform interaction executed successfully.",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
      });

      expect(auditService.recordInteractionAudit).toHaveBeenCalledWith({
        interactionId: result.interactionId,
        capability: "interactions",
        outcome: "success",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        request: req,
      });
    });

    it("returns a successful interaction envelope using req.user", async () => {
      const result = await interactionsService.executeInteraction({
        user: {
          userId: "user-456",
          role: "User",
          customerId: "customer-456",
        },
      });

      expect(result).toEqual({
        success: true,
        interactionId: expect.any(String),
        capability: "interactions",
        message: "Platform interaction executed successfully.",
        actor: {
          id: "user-456",
          role: "User",
          customerId: "customer-456",
        },
      });
    });

    it("returns a successful interaction envelope using req.currentUser", async () => {
      const result = await interactionsService.executeInteraction({
        currentUser: {
          id: "user-789",
          role: "Boss",
          customerId: "customer-789",
        },
      });

      expect(result).toEqual({
        success: true,
        interactionId: expect.any(String),
        capability: "interactions",
        message: "Platform interaction executed successfully.",
        actor: {
          id: "user-789",
          role: "Boss",
          customerId: "customer-789",
        },
      });
    });

    it("throws a 401 error when authenticated user context is missing", async () => {
      await expect(interactionsService.executeInteraction({})).rejects.toThrow(
        "Authenticated user context is required.",
      );

      try {
        await interactionsService.executeInteraction({});
      } catch (error) {
        expect(error.status).toBe(401);
      }
    });

    it("fails the interaction when Audit persistence fails", async () => {
      auditService.recordInteractionAudit.mockRejectedValue(
        new Error("audit persistence failed"),
      );

      await expect(
        interactionsService.executeInteraction({
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

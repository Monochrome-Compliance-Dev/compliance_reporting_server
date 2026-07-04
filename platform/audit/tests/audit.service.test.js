jest.mock("@/helpers/logger", () => ({
  auditLogger: {
    info: jest.fn(),
  },
}));

const logger = require("@/helpers/logger");
const auditService = require("@/platform/audit/audit.service");

describe("audit.service", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("recordInteractionAudit", () => {
    it("writes platform interaction audit evidence", () => {
      const result = auditService.recordInteractionAudit({
        interactionId: "interaction-123",
        capability: "interactions",
        outcome: "success",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
      });

      expect(result).toEqual({
        eventType: "platform.interaction.executed",
        interactionId: "interaction-123",
        capability: "interactions",
        outcome: "success",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        occurredAt: expect.any(String),
      });

      expect(logger.auditLogger.info).toHaveBeenCalledWith(result);
    });

    it("normalises missing actor fields to null", () => {
      const result = auditService.recordInteractionAudit({
        interactionId: "interaction-456",
        capability: "interactions",
        outcome: "success",
        actor: {},
      });

      expect(result.actor).toEqual({
        id: null,
        role: null,
        customerId: null,
      });
    });

    it("throws when interactionId is missing", () => {
      expect(() =>
        auditService.recordInteractionAudit({
          capability: "interactions",
          outcome: "success",
          actor: null,
        }),
      ).toThrow("interactionId is required for audit evidence.");

      try {
        auditService.recordInteractionAudit({
          capability: "interactions",
          outcome: "success",
          actor: null,
        });
      } catch (error) {
        expect(error.status).toBe(500);
      }
    });
  });
});

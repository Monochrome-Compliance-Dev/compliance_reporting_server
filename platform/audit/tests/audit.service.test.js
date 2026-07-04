jest.mock("@/helpers/logger", () => ({
  auditLogger: {
    info: jest.fn(),
  },
}));

jest.mock("@/platform/audit/audit.repository", () => ({
  createInteractionAuditEvent: jest.fn(),
}));

const logger = require("@/helpers/logger");
const auditRepository = require("@/platform/audit/audit.repository");
const auditService = require("@/platform/audit/audit.service");

describe("audit.service", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    auditRepository.createInteractionAuditEvent.mockResolvedValue({
      id: "audit-123",
    });
  });

  describe("recordInteractionAudit", () => {
    it("writes logger-backed and persistent platform interaction audit evidence", async () => {
      const request = {
        ip: "127.0.0.1",
        headers: {
          "user-agent": "jest-agent",
        },
      };

      const result = await auditService.recordInteractionAudit({
        interactionId: "interaction-123",
        capability: "interactions",
        outcome: "success",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        request,
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
      expect(auditRepository.createInteractionAuditEvent).toHaveBeenCalledWith({
        interactionId: "interaction-123",
        capability: "interactions",
        outcome: "success",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        occurredAt: result.occurredAt,
        request,
      });
    });

    it("normalises missing actor fields to null", async () => {
      const result = await auditService.recordInteractionAudit({
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

    it("throws when interactionId is missing", async () => {
      await expect(
        auditService.recordInteractionAudit({
          capability: "interactions",
          outcome: "success",
          actor: null,
        }),
      ).rejects.toThrow("interactionId is required for audit evidence.");

      try {
        await auditService.recordInteractionAudit({
          capability: "interactions",
          outcome: "success",
          actor: null,
        });
      } catch (error) {
        expect(error.status).toBe(500);
      }

      expect(
        auditRepository.createInteractionAuditEvent,
      ).not.toHaveBeenCalled();
    });

    it("fails loudly when persistent audit storage fails", async () => {
      auditRepository.createInteractionAuditEvent.mockRejectedValue(
        new Error("audit persistence failed"),
      );

      await expect(
        auditService.recordInteractionAudit({
          interactionId: "interaction-789",
          capability: "interactions",
          outcome: "success",
          actor: {
            id: "user-789",
            role: "Boss",
            customerId: "customer-789",
          },
        }),
      ).rejects.toThrow("audit persistence failed");
    });
  });
});

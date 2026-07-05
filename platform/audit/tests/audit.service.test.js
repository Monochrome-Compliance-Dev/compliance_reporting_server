jest.mock("@/helpers/logger", () => ({
  auditLogger: {
    info: jest.fn(),
  },
}));

jest.mock("@/platform/audit/audit.repository", () => ({
  createFoundationAuditEvent: jest.fn(),
}));

const logger = require("@/helpers/logger");
const auditRepository = require("@/platform/audit/audit.repository");
const auditService = require("@/platform/audit/audit.service");

describe("audit.service", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    auditRepository.createFoundationAuditEvent.mockResolvedValue({
      id: "audit-123",
    });
  });

  describe("recordFoundationAudit", () => {
    it("writes logger-backed and persistent platform foundation audit evidence", async () => {
      const request = {
        ip: "127.0.0.1",
        headers: {
          "user-agent": "jest-agent",
        },
      };

      const securityObservation = {
        eventType: "platform.security.foundation_observed",
        outcome: "allowed",
      };

      const result = await auditService.recordFoundationAudit({
        foundationId: "foundation-123",
        capability: "foundation",
        outcome: "success",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        request,
        securityObservation,
      });

      expect(result).toEqual({
        eventType: "platform.foundation.executed",
        foundationId: "foundation-123",
        capability: "foundation",
        outcome: "success",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        occurredAt: expect.any(String),
        security: securityObservation,
      });

      expect(logger.auditLogger.info).toHaveBeenCalledWith(result);
      expect(auditRepository.createFoundationAuditEvent).toHaveBeenCalledWith({
        foundationId: "foundation-123",
        capability: "foundation",
        outcome: "success",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        occurredAt: result.occurredAt,
        request,
        securityObservation,
      });
    });

    it("normalises missing actor fields to null", async () => {
      const result = await auditService.recordFoundationAudit({
        foundationId: "foundation-456",
        capability: "foundation",
        outcome: "success",
        actor: {},
      });

      expect(result.actor).toEqual({
        id: null,
        role: null,
        customerId: null,
      });

      expect(result.security).toBeNull();
    });

    it("throws when foundationId is missing", async () => {
      await expect(
        auditService.recordFoundationAudit({
          capability: "foundation",
          outcome: "success",
          actor: null,
        }),
      ).rejects.toThrow("foundationId is required for audit evidence.");

      try {
        await auditService.recordFoundationAudit({
          capability: "foundation",
          outcome: "success",
          actor: null,
        });
      } catch (error) {
        expect(error.status).toBe(500);
      }

      expect(auditRepository.createFoundationAuditEvent).not.toHaveBeenCalled();
    });

    it("fails loudly when persistent audit storage fails", async () => {
      auditRepository.createFoundationAuditEvent.mockRejectedValue(
        new Error("audit persistence failed"),
      );

      await expect(
        auditService.recordFoundationAudit({
          foundationId: "foundation-789",
          capability: "foundation",
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

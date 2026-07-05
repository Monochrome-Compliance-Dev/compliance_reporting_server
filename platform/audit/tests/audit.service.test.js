jest.mock("@/helpers/logger", () => ({
  auditLogger: {
    info: jest.fn(),
  },
}));

jest.mock("@/platform/audit/audit.repository", () => ({
  createDataDatasetAuditEvent: jest.fn(),
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
    auditRepository.createDataDatasetAuditEvent.mockResolvedValue({
      id: "audit-data-123",
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
        error: null,
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
        error: null,
      });
    });

    it("writes logger-backed and persistent denied platform foundation audit evidence", async () => {
      const request = {
        ip: "127.0.0.1",
        headers: {
          "user-agent": "jest-agent",
        },
      };

      const securityObservation = {
        eventType: "platform.security.foundation_observed",
        outcome: "denied",
        reason: "role_not_allowed",
      };

      const error = new Error("role is not allowed for governed execution");

      const result = await auditService.recordFoundationAudit({
        foundationId: "foundation-denied-123",
        capability: "foundation",
        outcome: "denied",
        actor: {
          id: "user-123",
          role: "Viewer",
          customerId: "customer-123",
        },
        request,
        securityObservation,
        error,
      });

      expect(result).toEqual({
        eventType: "platform.foundation.denied",
        foundationId: "foundation-denied-123",
        capability: "foundation",
        outcome: "denied",
        actor: {
          id: "user-123",
          role: "Viewer",
          customerId: "customer-123",
        },
        occurredAt: expect.any(String),
        security: securityObservation,
        error: {
          message: "role is not allowed for governed execution",
        },
      });

      expect(logger.auditLogger.info).toHaveBeenCalledWith(result);
      expect(auditRepository.createFoundationAuditEvent).toHaveBeenCalledWith({
        foundationId: "foundation-denied-123",
        capability: "foundation",
        outcome: "denied",
        actor: {
          id: "user-123",
          role: "Viewer",
          customerId: "customer-123",
        },
        occurredAt: result.occurredAt,
        request,
        securityObservation,
        error: {
          message: "role is not allowed for governed execution",
        },
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
      expect(result.error).toBeNull();
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

  describe("recordDataDatasetAudit", () => {
    it("writes logger-backed and persistent Data dataset creation audit evidence", async () => {
      const request = {
        ip: "127.0.0.1",
        headers: {
          "user-agent": "jest-agent",
        },
      };

      const securityObservation = {
        eventType: "platform.security.data_dataset_observed",
        outcome: "allowed",
      };

      const result = await auditService.recordDataDatasetAudit({
        datasetId: "dataset123",
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
        eventType: "platform.data.dataset.created",
        datasetId: "dataset123",
        capability: "data",
        action: "dataset.create",
        outcome: "success",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        occurredAt: expect.any(String),
        security: securityObservation,
        error: null,
      });

      expect(logger.auditLogger.info).toHaveBeenCalledWith(result);
      expect(auditRepository.createDataDatasetAuditEvent).toHaveBeenCalledWith({
        datasetId: "dataset123",
        capability: "data",
        action: "dataset.create",
        outcome: "success",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        occurredAt: result.occurredAt,
        request,
        securityObservation,
        error: null,
      });
    });

    it("writes logger-backed and persistent denied Data dataset audit evidence", async () => {
      const securityObservation = {
        eventType: "platform.security.data_dataset_observed",
        outcome: "denied",
        reason: "role_not_allowed",
      };
      const error = new Error("Role is not allowed for governed execution.");

      const result = await auditService.recordDataDatasetAudit({
        datasetId: "dataset-denied-123",
        outcome: "denied",
        actor: {
          id: "user-123",
          role: "Viewer",
          customerId: "customer-123",
        },
        securityObservation,
        error,
      });

      expect(result).toEqual({
        eventType: "platform.data.dataset.denied",
        datasetId: "dataset-denied-123",
        capability: "data",
        action: "dataset.create",
        outcome: "denied",
        actor: {
          id: "user-123",
          role: "Viewer",
          customerId: "customer-123",
        },
        occurredAt: expect.any(String),
        security: securityObservation,
        error: {
          message: "Role is not allowed for governed execution.",
        },
      });

      expect(logger.auditLogger.info).toHaveBeenCalledWith(result);
      expect(auditRepository.createDataDatasetAuditEvent).toHaveBeenCalledWith({
        datasetId: "dataset-denied-123",
        capability: "data",
        action: "dataset.create",
        outcome: "denied",
        actor: {
          id: "user-123",
          role: "Viewer",
          customerId: "customer-123",
        },
        occurredAt: result.occurredAt,
        request: undefined,
        securityObservation,
        error: {
          message: "Role is not allowed for governed execution.",
        },
      });
    });

    it("normalises missing actor fields to null", async () => {
      const result = await auditService.recordDataDatasetAudit({
        datasetId: "dataset456",
        outcome: "success",
        actor: {},
      });

      expect(result.actor).toEqual({
        id: null,
        role: null,
        customerId: null,
      });
      expect(result.security).toBeNull();
      expect(result.error).toBeNull();
    });

    it("throws when datasetId is missing", async () => {
      await expect(
        auditService.recordDataDatasetAudit({
          outcome: "success",
          actor: null,
        }),
      ).rejects.toThrow("datasetId is required for audit evidence.");

      expect(
        auditRepository.createDataDatasetAuditEvent,
      ).not.toHaveBeenCalled();
    });

    it("fails loudly when persistent Data audit storage fails", async () => {
      auditRepository.createDataDatasetAuditEvent.mockRejectedValue(
        new Error("data audit persistence failed"),
      );

      await expect(
        auditService.recordDataDatasetAudit({
          datasetId: "dataset789",
          outcome: "success",
          actor: {
            id: "user-789",
            role: "Boss",
            customerId: "customer-789",
          },
        }),
      ).rejects.toThrow("data audit persistence failed");
    });
  });
});

jest.mock("@/db/database", () => ({
  AuditEvent: {
    create: jest.fn(),
  },
}));

jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(),
}));

const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const auditRepository = require("@/platform/audit/audit.repository");

describe("audit.repository", () => {
  let transaction;

  beforeEach(() => {
    transaction = {
      commit: jest.fn(),
      rollback: jest.fn(),
    };

    jest.clearAllMocks();
    beginTransactionWithCustomerContext.mockResolvedValue(transaction);
    db.AuditEvent.create.mockImplementation(async (auditRow) => auditRow);
  });

  describe("createFoundationAuditEvent", () => {
    it("creates an foundation audit row using customer-scoped transaction", async () => {
      const result = await auditRepository.createFoundationAuditEvent({
        foundationId: "foundation-123",
        capability: "foundation",
        outcome: "success",
        occurredAt: "2026-07-04T07:00:00.000Z",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        request: {
          ip: "127.0.0.1",
          headers: {
            "user-agent": "jest-agent",
          },
        },
      });

      expect(beginTransactionWithCustomerContext).toHaveBeenCalledWith(
        "customer-123",
      );

      expect(db.AuditEvent.create).toHaveBeenCalledWith(
        {
          id: expect.any(String),
          customerId: "customer-123",
          userId: "user-123",
          action: "Execute",
          entity: "platform.foundation",
          entityId: "foundation-123",
          details: {
            eventType: "platform.foundation.executed",
            foundationId: "foundation-123",
            capability: "foundation",
            outcome: "success",
            actor: {
              id: "user-123",
              role: "Admin",
              customerId: "customer-123",
            },
            occurredAt: "2026-07-04T07:00:00.000Z",
          },
          ip: "127.0.0.1",
          device: "jest-agent",
        },
        {
          transaction,
        },
      );

      expect(result.id).toEqual(expect.any(String));
      expect(result.id).toHaveLength(10);
      expect(transaction.commit).toHaveBeenCalledTimes(1);
      expect(transaction.rollback).not.toHaveBeenCalled();
    });

    it("rolls back and rethrows when audit row creation fails", async () => {
      const error = new Error("database failed");
      db.AuditEvent.create.mockRejectedValue(error);

      await expect(
        auditRepository.createFoundationAuditEvent({
          foundationId: "foundation-123",
          capability: "foundation",
          outcome: "success",
          occurredAt: "2026-07-04T07:00:00.000Z",
          actor: {
            id: "user-123",
            role: "Admin",
            customerId: "customer-123",
          },
        }),
      ).rejects.toThrow("database failed");

      expect(transaction.rollback).toHaveBeenCalledTimes(1);
      expect(transaction.commit).not.toHaveBeenCalled();
    });

    it("rejects missing foundationId", async () => {
      await expect(
        auditRepository.createFoundationAuditEvent({
          capability: "foundation",
          outcome: "success",
          actor: {
            id: "user-123",
            customerId: "customer-123",
          },
        }),
      ).rejects.toThrow("foundationId is required for audit persistence.");

      expect(beginTransactionWithCustomerContext).not.toHaveBeenCalled();
    });

    it("rejects missing customerId", async () => {
      await expect(
        auditRepository.createFoundationAuditEvent({
          foundationId: "foundation-123",
          capability: "foundation",
          outcome: "success",
          actor: {
            id: "user-123",
          },
        }),
      ).rejects.toThrow("customerId is required for audit persistence.");

      expect(beginTransactionWithCustomerContext).not.toHaveBeenCalled();
    });

    it("rejects missing userId", async () => {
      await expect(
        auditRepository.createFoundationAuditEvent({
          foundationId: "foundation-123",
          capability: "foundation",
          outcome: "success",
          actor: {
            customerId: "customer-123",
          },
        }),
      ).rejects.toThrow("userId is required for audit persistence.");

      expect(beginTransactionWithCustomerContext).not.toHaveBeenCalled();
    });
  });
});

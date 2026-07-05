const securityService = require("@/platform/security/security.service");

describe("security.service", () => {
  describe("observeFoundationCommand", () => {
    it("creates an allowed Foundation command security observation", () => {
      const result = securityService.observeFoundationCommand({
        foundationId: "foundation-123",
        actor: {
          id: "user-123",
          role: "Boss",
          customerId: "customer-123",
        },
        request: {
          method: "POST",
          originalUrl: "/api/platform/foundation",
          ip: "127.0.0.1",
          headers: {
            "user-agent": "jest-agent",
          },
        },
      });

      expect(result).toEqual({
        eventType: "platform.security.foundation_observed",
        outcome: "allowed",
        capability: "foundation",
        foundationId: "foundation-123",
        actor: {
          id: "user-123",
          role: "Boss",
          customerId: "customer-123",
        },
        request: {
          method: "POST",
          path: "/api/platform/foundation",
          ip: "127.0.0.1",
          userAgent: "jest-agent",
        },
        reason: null,
        occurredAt: expect.any(String),
      });
    });

    it("uses actor.userId when actor.id is not present", () => {
      const result = securityService.observeFoundationCommand({
        foundationId: "foundation-456",
        actor: {
          userId: "user-456",
          role: "User",
          customerId: "customer-456",
        },
      });

      expect(result.actor).toEqual({
        id: "user-456",
        role: "User",
        customerId: "customer-456",
      });
    });

    it("normalises missing optional request fields to null", () => {
      const result = securityService.observeFoundationCommand({
        foundationId: "foundation-789",
        actor: {
          id: "user-789",
          customerId: "customer-789",
        },
        request: {},
      });

      expect(result.request).toEqual({
        method: null,
        path: null,
        ip: null,
        userAgent: null,
      });
    });

    it("throws when foundationId is missing", () => {
      expect(() =>
        securityService.observeFoundationCommand({
          actor: {
            id: "user-123",
            customerId: "customer-123",
          },
        }),
      ).toThrow("foundationId is required for security observation.");
    });

    it("throws when actor id is missing", () => {
      expect(() =>
        securityService.observeFoundationCommand({
          foundationId: "foundation-123",
          actor: {
            customerId: "customer-123",
          },
        }),
      ).toThrow("actor id is required for security observation.");
    });

    it("throws when customerId is missing", () => {
      expect(() =>
        securityService.observeFoundationCommand({
          foundationId: "foundation-123",
          actor: {
            id: "user-123",
          },
        }),
      ).toThrow("customerId is required for security observation.");
    });
  });

  describe("observeDeniedFoundationCommand", () => {
    it("creates a denied Foundation command security observation", () => {
      const result = securityService.observeDeniedFoundationCommand({
        foundationId: "foundation-denied-123",
        actor: {
          id: "user-123",
          role: "User",
          customerId: null,
        },
        request: {
          method: "POST",
          path: "/api/platform/foundation",
          ip: "127.0.0.1",
          headers: {
            "user-agent": "jest-agent",
          },
        },
        reason: "missing_customer_context",
      });

      expect(result).toEqual({
        eventType: "platform.security.foundation_observed",
        outcome: "denied",
        capability: "foundation",
        foundationId: "foundation-denied-123",
        actor: {
          id: "user-123",
          role: "User",
          customerId: null,
        },
        request: {
          method: "POST",
          path: "/api/platform/foundation",
          ip: "127.0.0.1",
          userAgent: "jest-agent",
        },
        reason: "missing_customer_context",
        occurredAt: expect.any(String),
      });
    });

    it("allows role_not_allowed as a denied reason", () => {
      const result = securityService.observeDeniedFoundationCommand({
        foundationId: "foundation-denied-456",
        actor: {
          id: "user-456",
          role: "Viewer",
          customerId: "customer-456",
        },
        reason: "role_not_allowed",
      });

      expect(result.reason).toBe("role_not_allowed");
      expect(result.outcome).toBe("denied");
    });

    it("allows customer_mismatch as a denied reason", () => {
      const result = securityService.observeDeniedFoundationCommand({
        foundationId: "foundation-denied-789",
        actor: {
          id: "user-789",
          role: "Boss",
          customerId: "customer-789",
        },
        reason: "customer_mismatch",
      });

      expect(result.reason).toBe("customer_mismatch");
      expect(result.outcome).toBe("denied");
    });

    it("throws when denied reason is missing", () => {
      expect(() =>
        securityService.observeDeniedFoundationCommand({
          foundationId: "foundation-denied-123",
          actor: {
            id: "user-123",
          },
        }),
      ).toThrow("reason is required for denied security observation.");
    });

    it("throws when denied reason is unsupported", () => {
      expect(() =>
        securityService.observeDeniedFoundationCommand({
          foundationId: "foundation-denied-123",
          actor: {
            id: "user-123",
          },
          reason: "bad_reason",
        }),
      ).toThrow("Unsupported denied security observation reason.");
    });

    it("throws when actor id is missing", () => {
      expect(() =>
        securityService.observeDeniedFoundationCommand({
          foundationId: "foundation-denied-123",
          actor: {
            customerId: "customer-123",
          },
          reason: "role_not_allowed",
        }),
      ).toThrow("actor id is required for security observation.");
    });
  });
});

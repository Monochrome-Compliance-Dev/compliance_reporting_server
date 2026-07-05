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

  describe("enforceFoundationCommand", () => {
    it("returns an allowed observation when the actor role is permitted", () => {
      const result = securityService.enforceFoundationCommand({
        foundationId: "foundation-allowed-123",
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
        requiredRoles: ["Admin", "Boss"],
      });

      expect(result).toEqual({
        eventType: "platform.security.foundation_observed",
        outcome: "allowed",
        capability: "foundation",
        foundationId: "foundation-allowed-123",
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

    it("uses the default Foundation required roles when requiredRoles is not provided", () => {
      const result = securityService.enforceFoundationCommand({
        foundationId: "foundation-default-roles-123",
        actor: {
          id: "user-123",
          role: "User",
          customerId: "customer-123",
        },
      });

      expect(result.outcome).toBe("allowed");
      expect(result.reason).toBeNull();
    });

    it("throws a 403 denial error with securityObservation when the actor role is not permitted", () => {
      try {
        securityService.enforceFoundationCommand({
          foundationId: "foundation-denied-123",
          actor: {
            id: "user-123",
            role: "Viewer",
            customerId: "customer-123",
          },
          request: {
            method: "POST",
            path: "/api/platform/foundation",
            ip: "127.0.0.1",
            headers: {
              "user-agent": "jest-agent",
            },
          },
          requiredRoles: ["Admin", "Boss", "User"],
        });
      } catch (error) {
        expect(error.message).toBe(
          "Role is not allowed for governed execution.",
        );
        expect(error.status).toBe(403);
        expect(error.securityObservation).toEqual({
          eventType: "platform.security.foundation_observed",
          outcome: "denied",
          capability: "foundation",
          foundationId: "foundation-denied-123",
          actor: {
            id: "user-123",
            role: "Viewer",
            customerId: "customer-123",
          },
          request: {
            method: "POST",
            path: "/api/platform/foundation",
            ip: "127.0.0.1",
            userAgent: "jest-agent",
          },
          reason: "role_not_allowed",
          occurredAt: expect.any(String),
        });
        return;
      }

      throw new Error("Expected security enforcement to throw.");
    });

    it("throws when requiredRoles is missing", () => {
      expect(() =>
        securityService.enforceFoundationCommand({
          foundationId: "foundation-123",
          actor: {
            id: "user-123",
            role: "Boss",
            customerId: "customer-123",
          },
          requiredRoles: null,
        }),
      ).toThrow("requiredRoles is required for security enforcement.");
    });

    it("throws when customerId is missing", () => {
      expect(() =>
        securityService.enforceFoundationCommand({
          foundationId: "foundation-123",
          actor: {
            id: "user-123",
            role: "Boss",
          },
        }),
      ).toThrow("customerId is required for security enforcement.");
    });
  });

  describe("observeDataDatasetCreation", () => {
    it("creates an allowed Data dataset security observation", () => {
      const result = securityService.observeDataDatasetCreation({
        datasetId: "dataset123",
        actor: {
          id: "user-123",
          role: "Boss",
          customerId: "customer-123",
        },
        request: {
          method: "POST",
          originalUrl: "/api/platform/data/datasets",
          ip: "127.0.0.1",
          headers: {
            "user-agent": "jest-agent",
          },
        },
      });

      expect(result).toEqual({
        eventType: "platform.security.data_dataset_observed",
        outcome: "allowed",
        capability: "data",
        action: "dataset.create",
        datasetId: "dataset123",
        actor: {
          id: "user-123",
          role: "Boss",
          customerId: "customer-123",
        },
        request: {
          method: "POST",
          path: "/api/platform/data/datasets",
          ip: "127.0.0.1",
          userAgent: "jest-agent",
        },
        reason: null,
        occurredAt: expect.any(String),
      });
    });

    it("throws when datasetId is missing", () => {
      expect(() =>
        securityService.observeDataDatasetCreation({
          actor: {
            id: "user-123",
            customerId: "customer-123",
          },
        }),
      ).toThrow("datasetId is required for security observation.");
    });
  });

  describe("observeDeniedDataDatasetCreation", () => {
    it("creates a denied Data dataset security observation", () => {
      const result = securityService.observeDeniedDataDatasetCreation({
        datasetId: "dataset-denied-123",
        actor: {
          id: "user-123",
          role: "Viewer",
          customerId: "customer-123",
        },
        reason: "role_not_allowed",
      });

      expect(result).toEqual({
        eventType: "platform.security.data_dataset_observed",
        outcome: "denied",
        capability: "data",
        action: "dataset.create",
        datasetId: "dataset-denied-123",
        actor: {
          id: "user-123",
          role: "Viewer",
          customerId: "customer-123",
        },
        request: {
          method: null,
          path: null,
          ip: null,
          userAgent: null,
        },
        reason: "role_not_allowed",
        occurredAt: expect.any(String),
      });
    });
  });

  describe("enforceDataDatasetCreation", () => {
    it("returns an allowed observation when the actor can create a Data dataset", () => {
      const result = securityService.enforceDataDatasetCreation({
        datasetId: "dataset123",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        customerId: "customer-123",
      });

      expect(result).toEqual({
        eventType: "platform.security.data_dataset_observed",
        outcome: "allowed",
        capability: "data",
        action: "dataset.create",
        datasetId: "dataset123",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        request: {
          method: null,
          path: null,
          ip: null,
          userAgent: null,
        },
        reason: null,
        occurredAt: expect.any(String),
      });
    });

    it("throws a 403 denial error when customer context is missing", () => {
      expect(() =>
        securityService.enforceDataDatasetCreation({
          datasetId: "dataset123",
          actor: {
            id: "user-123",
            role: "Admin",
          },
          customerId: null,
        }),
      ).toThrow("Customer context is required for governed execution.");
    });

    it("throws a 403 denial error when customer context does not match", () => {
      expect(() =>
        securityService.enforceDataDatasetCreation({
          datasetId: "dataset123",
          actor: {
            id: "user-123",
            role: "Admin",
            customerId: "customer-123",
          },
          customerId: "customer-456",
        }),
      ).toThrow("Customer context does not match governed execution.");
    });

    it("throws a 403 denial error with securityObservation when the actor role is not permitted", () => {
      try {
        securityService.enforceDataDatasetCreation({
          datasetId: "dataset123",
          actor: {
            id: "user-123",
            role: "Viewer",
            customerId: "customer-123",
          },
          customerId: "customer-123",
        });
      } catch (error) {
        expect(error.message).toBe(
          "Role is not allowed for governed execution.",
        );
        expect(error.status).toBe(403);
        expect(error.securityObservation).toEqual({
          eventType: "platform.security.data_dataset_observed",
          outcome: "denied",
          capability: "data",
          action: "dataset.create",
          datasetId: "dataset123",
          actor: {
            id: "user-123",
            role: "Viewer",
            customerId: "customer-123",
          },
          request: {
            method: null,
            path: null,
            ip: null,
            userAgent: null,
          },
          reason: "role_not_allowed",
          occurredAt: expect.any(String),
        });
        return;
      }

      throw new Error("Expected security enforcement to throw.");
    });
  });
});

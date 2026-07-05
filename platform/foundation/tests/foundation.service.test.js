jest.mock("@/platform/audit/audit.service", () => ({
  recordFoundationAudit: jest.fn(),
}));

jest.mock("@/platform/identity/identity.service", () => ({
  normaliseExecutionContext: jest.fn(),
}));

jest.mock("@/platform/security/security.service", () => ({
  enforceFoundationCommand: jest.fn(),
}));

const auditService = require("@/platform/audit/audit.service");
const identityService = require("@/platform/identity/identity.service");
const foundationService = require("@/platform/foundation/foundation.service");
const securityService = require("@/platform/security/security.service");

describe("foundation.service", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    auditService.recordFoundationAudit.mockResolvedValue({
      eventType: "platform.foundation.executed",
    });
    identityService.normaliseExecutionContext.mockReturnValue({
      actorId: "user-123",
      role: "Admin",
      customerId: "customer-123",
      source: "auth",
    });

    securityService.enforceFoundationCommand.mockReturnValue({
      eventType: "platform.security.foundation_observed",
      outcome: "allowed",
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

      expect(identityService.normaliseExecutionContext).toHaveBeenCalledWith(
        req,
      );

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

      expect(securityService.enforceFoundationCommand).toHaveBeenCalledWith({
        foundationId: result.foundationId,
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        request: req,
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
        securityObservation: {
          eventType: "platform.security.foundation_observed",
          outcome: "allowed",
        },
      });
    });

    it("returns a successful foundation envelope using req.user", async () => {
      identityService.normaliseExecutionContext.mockReturnValue({
        actorId: "user-456",
        role: "User",
        customerId: "customer-456",
        source: "user",
      });

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
      identityService.normaliseExecutionContext.mockReturnValue({
        actorId: "user-789",
        role: "Boss",
        customerId: "customer-789",
        source: "currentUser",
      });

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

    it("throws a 401 error when authenticated execution context is missing", async () => {
      const error = new Error("Authenticated execution context is required.");
      error.status = 401;
      identityService.normaliseExecutionContext.mockImplementation(() => {
        throw error;
      });

      await expect(foundationService.executeFoundation({})).rejects.toThrow(
        "Authenticated execution context is required.",
      );

      try {
        await foundationService.executeFoundation({});
      } catch (caughtError) {
        expect(caughtError.status).toBe(401);
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

    it("fails the foundation when Security enforcement fails without denied observation", async () => {
      securityService.enforceFoundationCommand.mockImplementation(() => {
        throw new Error("security enforcement failed");
      });

      await expect(
        foundationService.executeFoundation({
          auth: {
            id: "user-123",
            role: "Admin",
            customerId: "customer-123",
          },
        }),
      ).rejects.toThrow("security enforcement failed");

      expect(auditService.recordFoundationAudit).not.toHaveBeenCalled();
    });

    it("records denied audit evidence and rethrows when Security enforcement denies", async () => {
      const securityError = new Error(
        "Role is not allowed for governed execution.",
      );
      securityError.status = 403;
      securityError.securityObservation = {
        eventType: "platform.security.foundation_observed",
        outcome: "denied",
        reason: "role_not_allowed",
      };

      securityService.enforceFoundationCommand.mockImplementation(() => {
        throw securityError;
      });

      const req = {
        auth: {
          id: "user-123",
          role: "Viewer",
          customerId: "customer-123",
        },
      };

      await expect(foundationService.executeFoundation(req)).rejects.toThrow(
        "Role is not allowed for governed execution.",
      );

      expect(auditService.recordFoundationAudit).toHaveBeenCalledWith({
        foundationId: expect.any(String),
        capability: "foundation",
        outcome: "denied",
        actor: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
        request: req,
        securityObservation: securityError.securityObservation,
        error: securityError,
      });
    });
  });
});

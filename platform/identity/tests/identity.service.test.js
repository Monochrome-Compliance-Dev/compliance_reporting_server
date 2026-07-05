const identityService = require("@/platform/identity/identity.service");

describe("identity.service", () => {
  describe("normaliseExecutionContext", () => {
    it("normalises req.auth into a governed execution context", () => {
      const result = identityService.normaliseExecutionContext({
        auth: {
          id: "user-123",
          role: "Boss",
          customerId: "customer-123",
        },
      });

      expect(result).toEqual({
        actorId: "user-123",
        role: "Boss",
        customerId: "customer-123",
        source: "auth",
      });
    });

    it("normalises req.user when req.auth is not present", () => {
      const result = identityService.normaliseExecutionContext({
        user: {
          id: "user-456",
          role: "Admin",
          customerId: "customer-456",
        },
      });

      expect(result).toEqual({
        actorId: "user-456",
        role: "Admin",
        customerId: "customer-456",
        source: "user",
      });
    });

    it("normalises req.currentUser when req.auth and req.user are not present", () => {
      const result = identityService.normaliseExecutionContext({
        currentUser: {
          id: "user-789",
          role: "User",
          customerId: "customer-789",
        },
      });

      expect(result).toEqual({
        actorId: "user-789",
        role: "User",
        customerId: "customer-789",
        source: "currentUser",
      });
    });

    it("uses userId when id is not present", () => {
      const result = identityService.normaliseExecutionContext({
        auth: {
          userId: "user-abc",
          role: "User",
          customerId: "customer-abc",
        },
      });

      expect(result).toEqual({
        actorId: "user-abc",
        role: "User",
        customerId: "customer-abc",
        source: "auth",
      });
    });

    it("prefers req.auth over req.user and req.currentUser", () => {
      const result = identityService.normaliseExecutionContext({
        auth: {
          id: "auth-user",
          role: "Boss",
          customerId: "auth-customer",
        },
        user: {
          id: "user-user",
          role: "Admin",
          customerId: "user-customer",
        },
        currentUser: {
          id: "current-user",
          role: "User",
          customerId: "current-customer",
        },
      });

      expect(result).toEqual({
        actorId: "auth-user",
        role: "Boss",
        customerId: "auth-customer",
        source: "auth",
      });
    });

    it("normalises missing role to null", () => {
      const result = identityService.normaliseExecutionContext({
        auth: {
          id: "user-123",
          customerId: "customer-123",
        },
      });

      expect(result).toEqual({
        actorId: "user-123",
        role: null,
        customerId: "customer-123",
        source: "auth",
      });
    });

    it("throws a 401 error when authenticated context is missing", () => {
      expect(() => identityService.normaliseExecutionContext({})).toThrow(
        "Authenticated execution context is required.",
      );
    });

    it("throws when actorId is missing", () => {
      expect(() =>
        identityService.normaliseExecutionContext({
          auth: {
            role: "Boss",
            customerId: "customer-123",
          },
        }),
      ).toThrow("actorId is required for governed execution.");
    });

    it("throws when customerId is missing", () => {
      expect(() =>
        identityService.normaliseExecutionContext({
          auth: {
            id: "user-123",
            role: "Boss",
          },
        }),
      ).toThrow("customerId is required for governed execution.");
    });
  });
});

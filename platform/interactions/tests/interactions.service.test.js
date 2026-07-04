const interactionsService = require("@/platform/interactions/interactions.service");

describe("interactions.service", () => {
  describe("executeInteraction", () => {
    it("returns a successful interaction envelope using req.auth", () => {
      const result = interactionsService.executeInteraction({
        auth: {
          id: "user-123",
          role: "Admin",
          customerId: "customer-123",
        },
      });

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
    });

    it("returns a successful interaction envelope using req.user", () => {
      const result = interactionsService.executeInteraction({
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

    it("returns a successful interaction envelope using req.currentUser", () => {
      const result = interactionsService.executeInteraction({
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

    it("throws a 401 error when authenticated user context is missing", () => {
      expect(() => interactionsService.executeInteraction({})).toThrow(
        "Authenticated user context is required.",
      );

      try {
        interactionsService.executeInteraction({});
      } catch (error) {
        expect(error.status).toBe(401);
      }
    });
  });
});

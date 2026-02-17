const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const usersController = require("@/v2/users/users.controller");

// -----------------------------------------------------------------------------
// Public / Auth (no feature gate)
// -----------------------------------------------------------------------------

router.post("/authenticate", usersController.authenticate);
router.post("/refresh-token", usersController.refreshToken);
router.post("/revoke-token", usersController.revokeToken);

router.post("/register", usersController.register);
router.post("/register-first-user", usersController.registerFirstUser);

router.post("/verify-token", usersController.verifyToken);
router.post("/verify-email", usersController.verifyEmail);

router.post("/forgot-password", usersController.forgotPassword);
router.post("/validate-reset-token", usersController.validateResetToken);
router.post("/reset-password", usersController.resetPassword);

// -----------------------------------------------------------------------------
// Protected (Admin / Boss / User within tenant)
// -----------------------------------------------------------------------------

const requireUsers = authorise({
  roles: ["Admin", "Boss", "User"],
});

// List all users (Boss or scoped by effectiveCustomerId)
router.get("/", requireUsers, usersController.listUsers);

// List users for current effective customer
router.get("/by-customer", requireUsers, usersController.listUsersByCustomer);

// Single user
router.get("/:id", requireUsers, usersController.getUserById);

// Create / update / delete
router.post("/", requireUsers, usersController.createUser);
router.put("/:id", requireUsers, usersController.updateUser);
router.delete("/:id", requireUsers, usersController.deleteUser);

// Invite + resource creation (Pulse / resource integration)
router.post(
  "/invite-with-resource",
  requireUsers,
  usersController.inviteWithResource,
);

module.exports = router;

const express = require("express");

const authorise = require("@/middleware/authorise");
const interactionsController = require("./interactions.controller");

const router = express.Router();

const requirePlatformAccess = authorise({
  roles: ["Admin", "Boss", "User"],
});

router.post(
  "/",
  requirePlatformAccess,
  interactionsController.executeInteraction,
);

module.exports = router;

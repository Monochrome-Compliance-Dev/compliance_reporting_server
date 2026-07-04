const express = require("express");

const authorise = require("@/middleware/authorise");
const foundationController = require("./foundation.controller");

const router = express.Router();

const requirePlatformAccess = authorise({
  roles: ["Admin", "Boss", "User"],
});

router.post("/", requirePlatformAccess, foundationController.executeFoundation);

module.exports = router;

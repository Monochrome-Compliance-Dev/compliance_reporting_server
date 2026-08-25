const express = require("express");
const authorise = require("@/middleware/authorise");
const processController = require("@/v2/ptrs/controllers/process.ptrs.controller");

const router = express.Router();
const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

router.post("/:id/process", requirePtrs, processController.processPtrs);

module.exports = router;

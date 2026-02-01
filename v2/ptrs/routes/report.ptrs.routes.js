const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const reportController = require("@/v2/ptrs/controllers/report.ptrs.controller");

const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Report (read-only snapshot for review / board pack)
router.get("/:id/report", requirePtrs, reportController.getReport);

module.exports = router;

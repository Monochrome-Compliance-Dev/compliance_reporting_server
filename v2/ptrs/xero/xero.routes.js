const express = require("express");

const authorise = require("@/middleware/authorise");
const xeroController = require("./xero.controller");

const router = express.Router({ mergeParams: true });

const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Mounted at /api/v2/ptrs/:id/xero
router.post("/import", requirePtrs, xeroController.startImport);
router.get("/status", requirePtrs, xeroController.getStatus);

module.exports = router;

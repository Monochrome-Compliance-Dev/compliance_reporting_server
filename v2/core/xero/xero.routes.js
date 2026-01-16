const express = require("express");

// Reuse the existing PTRS Xero controller callback handler
const xeroController = require("@/v2/ptrs/xero/xero.controller");

const router = express.Router();

// Mounted at /api/v2/xero
router.get("/callback", xeroController.callback);

module.exports = router;

const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const ptrsController = require("@/v2/ptrs/controllers/tablesAndMaps.ptrs.controller");

// Restrict to users who have the ptrs feature (same as v1)
const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Column mapping (get/save)
router.get("/:id/map", requirePtrs, ptrsController.getMap);
router.post("/:id/map", requirePtrs, ptrsController.saveMap);

module.exports = router;

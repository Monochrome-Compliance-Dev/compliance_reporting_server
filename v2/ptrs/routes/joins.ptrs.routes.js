const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const joinsController = require("@/v2/ptrs/controllers/joins.ptrs.controller");

// Restrict to users who have the ptrs feature (same as v1)
const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Joins + custom fields (get/save) - persisted to tbl_ptrs_column_map.joins/customFields
router.get("/:id/joins", requirePtrs, joinsController.getJoins);
router.get(
  "/:id/compatible-joins",
  requirePtrs,
  joinsController.listCompatibleJoins,
);
router.put("/:id/joins", requirePtrs, joinsController.saveJoins);

module.exports = router;

const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const rulesController = require("@/v2/ptrs/controllers/rules.ptrs.controller");

// Restrict to users who have the ptrs feature (same as v1)
const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Rules (preview/apply)
router.get("/:id/rules/preview", requirePtrs, rulesController.rulesPreview);
router.post("/:id/rules/apply", requirePtrs, rulesController.rulesApply);
// Rules (get/save) — FE writes rules without resending mappings
router.get("/:id/rules", requirePtrs, rulesController.getRules);
router.post("/:id/rules", requirePtrs, rulesController.saveRules);

// return any rules used at a profile level
router.get("/:id/rules/sources", requirePtrs, rulesController.getProfileRules);

// sandbox route for previewing SQL statements against a PTRS upload
router.post(
  "/:id/rules/sandbox/preview",
  requirePtrs,
  rulesController.rulesSandboxPreview
);

module.exports = router;

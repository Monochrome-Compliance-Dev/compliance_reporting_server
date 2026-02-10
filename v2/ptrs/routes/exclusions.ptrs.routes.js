const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const exclusionsController = require("@/v2/ptrs/controllers/exclusions.ptrs.controller");

// Restrict to users who have the ptrs feature (same as Rules)
const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Exclusions (apply)
router.post(
  "/:id/exclusions/apply",
  requirePtrs,
  exclusionsController.exclusionsApply,
);

// Exclusions (preview)
router.get(
  "/:id/exclusions/preview",
  requirePtrs,
  exclusionsController.exclusionsPreview,
);

router.get(
  "/:id/exclusions/keywords",
  requirePtrs,
  exclusionsController.exclusionKeywordsList,
);

router.post(
  "/:id/exclusions/keywords",
  requirePtrs,
  exclusionsController.exclusionKeywordsCreate,
);

router.put(
  "/:id/exclusions/keywords/:keywordId",
  requirePtrs,
  exclusionsController.exclusionKeywordsUpdate,
);

router.delete(
  "/:id/exclusions/keywords/:keywordId",
  requirePtrs,
  exclusionsController.exclusionKeywordsDelete,
);

module.exports = router;

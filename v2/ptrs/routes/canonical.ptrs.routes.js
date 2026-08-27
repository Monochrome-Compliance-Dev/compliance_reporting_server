const express = require("express");
const authorise = require("@/middleware/authorise");
const controller = require("@/v2/ptrs/controllers/canonical.ptrs.controller");

const router = express.Router();
const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

router.post(
  "/:id/datasets/:datasetId/canonical-revisions",
  requirePtrs,
  controller.materializeRevision,
);
router.get(
  "/:id/canonical-revisions",
  requirePtrs,
  controller.listRevisionStatus,
);

module.exports = router;

const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const metricsController = require("@/v2/ptrs/controllers/metrics.ptrs.controller");

const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Metrics (Report preview)
// GET returns merged preview (header + saved draft declarations + computed values)
router.get("/:id/metrics", requirePtrs, metricsController.getMetrics);

// PATCH updates only the saved draft declarations/comments (read-only dataset)
router.patch("/:id/metrics", requirePtrs, metricsController.updateMetricsDraft);

module.exports = router;

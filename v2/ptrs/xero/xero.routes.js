const express = require("express");

const authorise = require("@/middleware/authorise");
const xeroController = require("./xero.controller");

const router = express.Router({ mergeParams: true });

const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Mounted at /api/v2/ptrs/:id/xero
router.post("/connect", requirePtrs, xeroController.connect);
router.get("/organisations", requirePtrs, xeroController.getOrganisations);
router.post("/organisations", requirePtrs, xeroController.selectOrganisations);
router.delete(
  "/organisations/:tenantId",
  requirePtrs,
  xeroController.removeOrganisation,
);

router.post("/import", requirePtrs, xeroController.startImport);
router.get("/status", requirePtrs, xeroController.getStatus);
router.get("/readiness", requirePtrs, xeroController.getReadiness);

router.get(
  "/import/exceptions",
  requirePtrs,
  xeroController.getImportExceptions,
);

router.get(
  "/import/exceptions/summary",
  requirePtrs,
  xeroController.getImportExceptionsSummary,
);

router.get(
  "/import/exceptions.csv",
  requirePtrs,
  xeroController.downloadImportExceptionsCsv,
);

module.exports = router;

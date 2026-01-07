const express = require("express");
const multer = require("multer");

const authorise = require("@/middleware/authorise");
const sbiController = require("@/v2/ptrs/controllers/sbi.ptrs.controller");

const router = express.Router();

const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Store uploads in memory; service will hash + parse
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 }, // 25MB
});

// SBI Check
router.post(
  "/:id/sbi/import",
  requirePtrs,
  upload.single("file"),
  sbiController.importSbiResults
);

router.get("/:id/sbi/status", requirePtrs, sbiController.getSbiStatus);

module.exports = router;

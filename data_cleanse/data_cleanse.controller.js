const express = require("express");
const router = express.Router();
const authorise = require("../middleware/authorise");
const dcService = require("./data_cleanse.service");
const validateRequest = require("../middleware/validate-request");
const { validateAbnLookupSchema } = require("./data_cleanse.validator");
const { logger } = require("../helpers/logger");

// routes
router.post(
  "/abn-lookup",
  authorise(),
  validateRequest(validateAbnLookupSchema()),
  async (req, res, next) => {
    try {
      const input = req.body;
      // console.log("ABN lookup input:", input);

      // Determine if single or batch
      const isBatch = Array.isArray(input);

      // Call service to get ABN candidates
      const results = isBatch
        ? (
            await Promise.all(
              input
                .filter(
                  (entry) =>
                    entry && typeof entry.name === "string" && entry.name.trim()
                )
                .map((entry) => dcService.getAbnCandidatesForName(entry.name))
            )
          ).flat()
        : await dcService.getAbnCandidatesForName(input.name);

      if (!results || results.length === 0) {
        return res.status(404).json({ message: "No results found" });
      }

      // Log the event
      logger.logEvent("info", "ABN lookup performed", {
        action: "ABNLookup",
        name: isBatch ? "BATCH" : input.name,
        resultsCount: results.length,
      });

      res.status(200).json(results);
    } catch (error) {
      next(error);
    }
  }
);

module.exports = router;

const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
// const Joi = require("joi"); // Uncomment if you need validation schemas

// Routes
router.post("/refresh-token", authorise(), refreshToken);
router.post("/fetch-contacts", authorise(), fetchContacts);
router.post("/extract", authorise(), extract);
router.get("/transformed-data", authorise(), getTransformedData);

module.exports = router;

// Import xero service
const xeroService = require("./xero.service");

async function refreshToken(req, res, next) {
  try {
    const result = await xeroService.refreshToken(req.body, req.user);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message || "Failed to refresh token" });
  }
}

async function fetchContacts(req, res, next) {
  try {
    const result = await xeroService.fetchContacts(req.body, req.user);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message || "Failed to fetch contacts" });
  }
}

async function extract(req, res, next) {
  try {
    const result = await xeroService.extract(req.body, req.user);
    res.json(result);
  } catch (err) {
    res
      .status(500)
      .json({ error: err.message || "Failed to extract and transform data" });
  }
}

async function getTransformedData(req, res, next) {
  try {
    const result = await xeroService.getTransformedData(req.query, req.user);
    res.json(result);
  } catch (err) {
    res
      .status(500)
      .json({ error: err.message || "Failed to get transformed data" });
  }
}

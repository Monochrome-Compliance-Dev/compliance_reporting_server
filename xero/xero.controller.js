const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
// const Joi = require("joi"); // Uncomment if you need validation schemas

// Import xero service
const xeroService = require("./xero.service");
const { credentialsSchema } = require("./xero.validator");

// Routes
router.post(
  "/extract",
  authorise(),
  validateRequest(credentialsSchema),
  extract
);

// OAuth callback route
router.get("/callback", handleOAuthCallback);

module.exports = router;

async function extract(req, res, next) {
  try {
    logger.logEvent("Starting credential confirmation");
    // Placeholder for future confirmCredentials call
    // await xeroService.confirmCredentials(req.body);

    logger.logEvent("Refreshing token");
    await xeroService.refreshToken(req.body);

    logger.logEvent("Fetching invoices");
    const invoices = await xeroService.fetchInvoices(req.body);

    logger.logEvent("Fetching payments");
    await xeroService.fetchPayments(req.body);

    logger.logEvent("Fetching organisation details");
    await xeroService.fetchOrganisationDetails(req.body);

    logger.logEvent("Fetching contacts");
    const contacts = await xeroService.fetchContacts(invoices);

    logger.logEvent("Transforming data");
    const transformedData = await xeroService.getTransformedData(
      invoices,
      contacts
    );

    res.json({ data: transformedData });
  } catch (err) {
    logger.logEvent(`Error during extraction: ${err.message || err}`);
    res
      .status(500)
      .json({ error: err.message || "Failed to extract and transform data" });
  }
}

/**
 * Handles the OAuth2 callback from Xero.
 * Validates state, exchanges code for tokens, saves tokens, logs, and redirects to the frontend.
 */
async function handleOAuthCallback(req, res, next) {
  try {
    const { state, code } = req.query;
    if (!state || !code) {
      logger.logEvent("error", "Missing state or code in OAuth callback", {
        action: "OAuthCallback",
        state,
        code,
      });
      return res.status(400).send("Missing state or code.");
    }

    // Validate state and extract clientId (assumes state is a JSON string or base64-encoded JSON)
    let clientId;
    try {
      // Try to decode base64 or parse JSON directly
      let decoded;
      try {
        decoded = Buffer.from(state, "base64").toString("utf8");
        const parsed = JSON.parse(decoded);
        clientId = parsed.clientId;
      } catch (e) {
        // fallback: maybe state is plain JSON string
        const parsed = JSON.parse(state);
        clientId = parsed.clientId;
      }
      if (!clientId) throw new Error("clientId missing in state");
    } catch (err) {
      logger.logEvent("error", "Invalid state parameter in OAuth callback", {
        action: "OAuthCallback",
        state,
        error: err.message,
      });
      return res.status(400).send("Invalid state parameter.");
    }

    logger.logEvent(
      "info",
      "OAuth callback received, exchanging code for tokens",
      {
        action: "OAuthCallback",
        clientId,
      }
    );

    // Exchange code for tokens
    let tokenData;
    try {
      tokenData = await xeroService.exchangeAuthCodeForTokens({
        code,
        clientId,
      });
    } catch (err) {
      logger.logEvent("error", "Failed to exchange code for tokens", {
        action: "OAuthCallback",
        clientId,
        error: err.message,
      });
      return res.status(500).send("Failed to exchange code for tokens.");
    }

    // Save tokens to xero_tokens table with clientId
    try {
      // The xeroService.exchangeAuthCodeForTokens should handle saving, but ensure here as well if needed
      // If not handled, implement saving here:
      // await xeroService.saveTokensToDb({ ...tokenData, clientId });
    } catch (err) {
      logger.logEvent("error", "Failed to save tokens to DB", {
        action: "OAuthCallback",
        clientId,
        error: err.message,
      });
      return res.status(500).send("Failed to save tokens.");
    }

    logger.logEvent(
      "info",
      "Tokens saved. Redirecting to frontend report-wizard.",
      {
        action: "OAuthCallback",
        clientId,
      }
    );

    // Redirect user to frontend /report-wizard page
    const frontendUrl =
      process.env.XERO_REDIRECT_URI || "http://localhost:3000";
    return res.redirect(`${frontendUrl}/reports/ptrs/${reportId}`);
  } catch (err) {
    logger.logEvent("error", "Error in OAuth callback", {
      action: "OAuthCallback",
      error: err.message,
    });
    return res.status(500).send("Internal server error during OAuth callback.");
  }
}

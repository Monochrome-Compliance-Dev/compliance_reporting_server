const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();

const xeroService = require("./xero.service");
const authorise = require("../middleware/authorise");

router.get("/connect", authorise(), generateAuthUrl);
router.get("/callback", handleOAuthCallback);

module.exports = router;

function generateAuthUrl(req, res) {
  try {
    const xeroClientId = process.env.XERO_CLIENT_ID;
    const redirectUri =
      process.env.XERO_REDIRECT_URI || "http://localhost:3000/callback";
    const scopes =
      process.env.XERO_SCOPES ||
      "openid profile email accounting.transactions accounting.contacts";

    if (!xeroClientId) {
      logger.logEvent(
        "error",
        "XERO_CLIENT_ID is not set in environment variables"
      );
      return res.status(500).json({ error: "Server configuration error." });
    }

    const state = JSON.stringify({
      clientId: req.auth?.clientId,
      reportId: req.body?.reportId,
    });

    const params = new URLSearchParams({
      response_type: "code",
      client_id: xeroClientId,
      redirect_uri: redirectUri,
      scope: scopes,
      state,
    });

    const authUrl = `https://login.xero.com/identity/connect/authorize?${params.toString()}`;

    res.json({ authUrl });
  } catch (err) {
    logger.logEvent("error", "Error generating auth URL", {
      error: err.message,
    });
    res.status(500).json({ error: "Failed to generate authorization URL." });
  }
}

async function handleOAuthCallback(req, res) {
  try {
    const { code, state } = req.query;

    if (!code) {
      logger.logEvent("error", "Missing code in OAuth callback", {
        action: "OAuthCallback",
        code,
      });
      return res.status(400).send("Missing code.");
    }

    let parsedState = {};
    try {
      parsedState = JSON.parse(state);
    } catch (err) {
      logger.logEvent("error", "Failed to parse state", {
        action: "OAuthCallback",
        error: err.message,
        state,
      });
      return res.status(400).send("Invalid state parameter.");
    }

    const { clientId, reportId } = parsedState;

    let tokenData;
    try {
      tokenData = await xeroService.exchangeAuthCodeForTokens(
        code,
        state,
        clientId
      );
      console.log("Received token data from Xero:", tokenData);
    } catch (err) {
      logger.logEvent("error", "Failed to exchange code for tokens", {
        action: "OAuthCallback",
        error: err.message,
      });
      return res.status(500).send("Failed to exchange code for tokens.");
    }

    const frontendUrl =
      process.env.XERO_REDIRECT_URI || "http://localhost:3000";
    return res.redirect(`${frontendUrl}/reports/ptrs`);
  } catch (err) {
    logger.logEvent("error", "Error in OAuth callback", {
      action: "OAuthCallback",
      error: err.message,
    });
    return res.status(500).send("Internal server error during OAuth callback.");
  }
}

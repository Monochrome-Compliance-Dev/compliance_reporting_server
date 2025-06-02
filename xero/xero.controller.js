const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();

const xeroService = require("./xero.service");
const authorise = require("../middleware/authorise");

router.get("/connect/:reportId", authorise(), generateAuthUrl);
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
      reportId: req.params?.reportId,
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

    // 1️⃣ Exchange the code for tokens
    let tokenData;
    try {
      tokenData = await xeroService.exchangeAuthCodeForTokens(code, state, req);
    } catch (err) {
      logger.logEvent("error", "Failed to exchange code for tokens", {
        action: "OAuthCallback",
        error: err.message,
      });
      return res.status(500).send("Failed to exchange code for tokens.");
    }

    let { access_token: accessToken } = tokenData;
    if (typeof accessToken !== "string") {
      accessToken = String(accessToken);
    }

    // New step: get connections to obtain tenantId
    let tenantId;
    try {
      const connections = await xeroService.getConnections(accessToken);
      tenantId = connections?.[0]?.tenantId;
      if (typeof tenantId !== "string") {
        tenantId = String(tenantId);
      }
    } catch (err) {
      logger.logEvent("error", "Failed to get connections", {
        action: "OAuthCallback",
        error: err.message,
      });
      return res.status(500).send("Failed to get connections.");
    }

    // Added check to ensure tenantId is retrieved properly
    if (!tenantId) {
      logger.logEvent(
        "error",
        "Tenant ID is missing after connection retrieval",
        { action: "OAuthCallback" }
      );
      return res
        .status(500)
        .send("Failed to retrieve Tenant ID from connections.");
    }

    try {
      await xeroService.fetchOrganisationDetails({
        accessToken,
        tenantId,
        clientId,
        reportId,
      });

      await xeroService.fetchInvoices(
        accessToken,
        tenantId,
        clientId,
        reportId
      );

      await xeroService.fetchPayments(
        accessToken,
        tenantId,
        clientId,
        reportId
      );

      await xeroService.fetchContacts(
        accessToken,
        tenantId,
        clientId,
        reportId
      );

      console.log("Xero data fetched and saved successfully.");
    } catch (fetchErr) {
      logger.logEvent("error", "Error fetching Xero data", {
        action: "OAuthCallback-FetchingData",
        error: fetchErr.message,
      });
      console.log(`Error fetching data: ${fetchErr.message}`);
    }

    // Ensure accessToken and tenantId are strings before redirect
    if (typeof accessToken !== "string") {
      accessToken = String(accessToken);
    }
    if (typeof tenantId !== "string") {
      tenantId = String(tenantId);
    }

    // 3️⃣ Redirect user to the frontend with updates
    const frontendUrl = process.env.FRONTEND_URL || "http://localhost:3000";
    const updatesParam = encodeURIComponent(JSON.stringify([]));
    return res.redirect(`${frontendUrl}/reports/ptrs/${reportId}`);
  } catch (err) {
    logger.logEvent("error", "Error in OAuth callback", {
      action: "OAuthCallback",
      error: err.message,
    });
    return res.status(500).send("Internal server error during OAuth callback.");
  }
}

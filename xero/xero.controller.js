const csv = require("csv-parser");
const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();
const fs = require("fs");
const path = require("path");

const xeroService = require("./xero.service");
const authorise = require("../middleware/authorise");
const { transformXeroData } = require("../scripts/xero/transformXeroData");
const tcpService = require("../tcp/tcp.service");

router.get(
  "/connect/:reportId/:createdBy/:startDate/:endDate",
  authorise(),
  generateAuthUrl
);
router.get("/callback", handleOAuthCallback);
router.post("/extract", authorise(), startXeroExtractionHandler);

// Route to remove a tenant
router.delete("/tenants/:tenantId", authorise(), async (req, res) => {
  try {
    const { tenantId } = req.params;
    await xeroService.removeTenant(tenantId);
    logger.auditEvent("tenantRemoved", {
      tenantId,
      removedBy: req.auth?.userId,
    });
    res
      .status(200)
      .json({
        success: true,
        message: `Tenant ${tenantId} removed successfully.`,
      });
  } catch (err) {
    logger.logEvent("error", "Failed to remove tenant", {
      action: "removeTenant",
      error: err.message,
      stack: err.stack,
    });
    res
      .status(500)
      .json({ success: false, message: "Failed to remove tenant." });
  }
});

module.exports = router;

function generateAuthUrl(req, res) {
  try {
    const xeroClientId = process.env.XERO_CLIENT_ID;
    const redirectUri =
      process.env.XERO_REDIRECT_URI || "http://localhost:3000/callback";
    const scopes =
      process.env.XERO_SCOPES ||
      "openid profile email accounting.transactions.read accounting.contacts.read accounting.settings.read";

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
      createdBy: req.params?.createdBy,
      startDate: req.params?.startDate,
      endDate: req.params?.endDate,
    });

    const params = new URLSearchParams({
      response_type: "code",
      client_id: xeroClientId,
      redirect_uri: redirectUri,
      scope: scopes,
      state,
    });

    const authUrl = `https://login.xero.com/identity/connect/authorize?${params.toString()}`;

    res.json({
      success: true,
      message: "Authorization URL generated.",
      data: { authUrl },
    });
  } catch (err) {
    logger.logEvent("error", "Error generating auth URL", {
      error: err.message,
      stack: err.stack,
    });
    res
      .status(500)
      .json({
        success: false,
        message: "Failed to generate authorization URL.",
      });
  }
}

async function handleOAuthCallback(req, res) {
  const { code, state, error, error_description } = req.query;

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

  const { reportId } = parsedState;

  try {
    if (error) {
      const description =
        error_description || "Access was denied or revoked by the user.";
      logger.logEvent("warn", "Xero auth denied or revoked", {
        error,
        description,
      });

      const frontendUrl = process.env.FRONTEND_URL || "http://localhost:3000";
      if (description.includes("TenantConsent status DENIED")) {
        return res.redirect(`${frontendUrl}/user/dashboard`);
      }

      return res.redirect(
        `${frontendUrl}/reports/ptrs/${reportId}/selection?error=${encodeURIComponent(description)}`
      );
    }

    if (!code) {
      const description =
        "Missing code in OAuth callback — the connection may have failed or been cancelled unexpectedly.";
      logger.logEvent("error", description, {
        action: "OAuthCallback",
        reportId,
      });

      const frontendUrl = process.env.FRONTEND_URL || "http://localhost:3000";
      return res.redirect(
        `${frontendUrl}/reports/ptrs/${reportId}/selection?error=${encodeURIComponent(description)}`
      );
    }

    // Exchange the code for tokens
    let tokenData;
    try {
      tokenData = await xeroService.exchangeAuthCodeForTokens(code, state, req);
    } catch (err) {
      logger.logEvent("error", "Failed to exchange code for tokens", {
        action: "OAuthCallback",
        error: err.message,
        stack: err.stack,
      });
      return res.status(500).send("Failed to exchange code for tokens.");
    }

    let { access_token: accessToken } = tokenData;
    if (typeof accessToken !== "string") {
      accessToken = String(accessToken);
    }

    // Get connections for multiple tenants support
    let connections = [];
    try {
      connections = await xeroService.getConnections(accessToken);
    } catch (err) {
      logger.logEvent("error", "Failed to get connections", {
        action: "OAuthCallback",
        error: err.message,
        stack: err.stack,
      });
      return res.status(500).send("Failed to get connections.");
    }

    if (connections.length === 0) {
      logger.logEvent(
        "error",
        "No tenant IDs found after connection retrieval",
        { action: "OAuthCallback" }
      );
      return res.status(500).send("No tenant IDs found.");
    }

    // console.log("Retrieved connections:", connections);

    // Save token per tenant (removed validation)
    await Promise.all(
      connections.map(async (connection) => {
        const tokenPayload = {
          accessToken,
          refreshToken: tokenData.refresh_token,
          expiresIn: tokenData.expires_in,
          idToken: tokenData.id_token,
          clientId: parsedState.clientId,
          tenantId: connection.tenantId,
          createdBy: parsedState.createdBy,
        };
        await xeroService.saveToken(tokenPayload);
      })
    );

    // Redirect the user immediately to the frontend selection page with orgs in query string
    const frontendUrl = process.env.FRONTEND_URL || "http://localhost:3000";
    const encodedOrgs = encodeURIComponent(JSON.stringify(connections));
    res.redirect(
      `${frontendUrl}/reports/ptrs/${reportId}/selection?organisations=${encodedOrgs}`
    );
  } catch (err) {
    logger.logEvent("error", "Error in OAuth callback", {
      action: "OAuthCallback",
      error: err.message,
      stack: err.stack,
    });
    return res.status(500).send("Internal server error during OAuth callback.");
  }
}

async function startXeroExtractionHandler(req, res) {
  const { clientId, reportId, createdBy, startDate, endDate, tenantIds } =
    req.body;

  if (!Array.isArray(tenantIds) || tenantIds.length === 0) {
    return res
      .status(400)
      .json({ success: false, message: "No tenantIds provided." });
  }

  try {
    await xeroService.startXeroExtraction({
      clientId,
      reportId,
      createdBy,
      startDate,
      endDate,
      tenantIds,
    });
    logger.logEvent("info", "Xero extraction started for specified tenants.", {
      clientId,
      reportId,
      createdBy,
      tenantIds,
      startDate,
      endDate,
    });
    res
      .status(202)
      .json({
        success: true,
        message: "Extraction started for specified tenants.",
      });
  } catch (err) {
    logger.logEvent("error", "Failed to start Xero extraction", {
      action: "startXeroExtractionHandler",
      error: err.message,
      stack: err.stack,
    });
    res
      .status(500)
      .json({ success: false, message: "Failed to start extraction." });
  }
}

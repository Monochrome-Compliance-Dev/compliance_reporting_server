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
    res
      .status(200)
      .json({ message: `Tenant ${tenantId} removed successfully.` });
  } catch (err) {
    logger.logEvent("error", "Failed to remove tenant", {
      action: "removeTenant",
      error: err.message,
    });
    res.status(500).json({ error: "Failed to remove tenant." });
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

    res.json({ authUrl });
  } catch (err) {
    logger.logEvent("error", "Error generating auth URL", {
      error: err.message,
    });
    res.status(500).json({ error: "Failed to generate authorization URL." });
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
    });
    return res.status(500).send("Internal server error during OAuth callback.");
  }
}

async function startXeroExtractionHandler(req, res) {
  const { clientId, reportId, createdBy, startDate, endDate, tenantIds } =
    req.body;

  // Need to hardcode dates for testing
  // const { clientId, reportId, createdBy, tenantIds } = req.body;
  // const startDate = "2025-03-01";
  // const endDate = "2025-03-31";

  if (!Array.isArray(tenantIds) || tenantIds.length === 0) {
    return res.status(400).json({ error: "No tenantIds provided." });
  }

  for (const tenantId of tenantIds) {
    const tokenRecord = await xeroService.getLatestToken(clientId, tenantId);
    if (!tokenRecord || !tokenRecord.access_token) {
      logger.logEvent("error", "Access token not found for client and tenant", {
        clientId,
        tenantId,
      });
      continue; // skip this one, but keep going
    }

    const accessToken = tokenRecord.access_token;

    setImmediate(() => {
      startXeroExtraction({
        accessToken,
        clientId,
        reportId,
        createdBy,
        startDate,
        endDate,
        tenantIds: [tenantId], // pass as array for downstream compatibility
      });
    });
  }

  res
    .status(202)
    .json({ message: "Extraction started for specified tenants." });
}

async function startXeroExtraction({
  accessToken,
  clientId,
  reportId,
  createdBy,
  startDate,
  endDate,
  tenantIds,
}) {
  try {
    // Prepare arrays to collect data from all tenants
    const organisations = [];
    const payments = [];
    const invoices = [];
    const contacts = [];
    const bankTransactions = [];

    for (const tenantId of tenantIds) {
      const tenant = { tenantId };

      const baseDelay = 10000; // 10s minimum delay
      const jitter = Math.floor(Math.random() * 10000); // add 0–10s
      const waitTime = baseDelay + jitter;

      logger.logEvent(
        "info",
        `Waiting ${waitTime / 1000}s before syncing tenant ${tenant}`,
        {
          tenantId,
          delayMs: waitTime,
        }
      );
      if (global.sendWebSocketUpdate) {
        global.sendWebSocketUpdate({
          status: "info",
          message: `Waiting ${Math.round(waitTime / 1000)}s before syncing tenant ${tenantId}`,
          timestamp: new Date().toISOString(),
        });
      }
      await new Promise((resolve) => setTimeout(resolve, waitTime));

      try {
        const orgs = await xeroService.fetchOrganisationDetails({
          accessToken,
          tenantId,
          clientId,
          reportId,
          createdBy,
        });
        if (Array.isArray(orgs)) {
          organisations.push(...orgs);
        } else {
          organisations.push(orgs);
        }

        console.log(
          `Fetching BankTransactions from ${startDate} to ${endDate}`
        );

        const tenantBankTransactions = await xeroService.fetchBankTransactions(
          accessToken,
          tenantId,
          clientId,
          reportId,
          startDate,
          endDate,
          createdBy
        );
        bankTransactions.push(...tenantBankTransactions);

        const tenantPayments = await xeroService.fetchPayments(
          accessToken,
          tenantId,
          clientId,
          reportId,
          startDate,
          endDate,
          createdBy
        );
        payments.push(...tenantPayments);

        const tenantInvoices = await xeroService.fetchInvoices(
          accessToken,
          tenantId,
          clientId,
          reportId,
          tenantPayments,
          createdBy
        );
        invoices.push(...tenantInvoices);

        const tenantContacts = await xeroService.fetchContacts(
          accessToken,
          tenantId,
          clientId,
          reportId,
          [...tenantPayments, ...tenantBankTransactions],
          createdBy
        );
        contacts.push(...tenantContacts);
      } catch (err) {
        logger.logEvent("error", "Tenant sync failed", {
          tenantId,
          error: err,
        });
        if (global.sendWebSocketUpdate) {
          global.sendWebSocketUpdate({
            status: "error",
            message: `Tenant ${tenantId} failed: ${err.message}`,
            timestamp: new Date().toISOString(),
          });
        }
      }
    }

    logger.logEvent(
      "info",
      "Xero data fetched and saved successfully to xero_[tables]."
    );

    const xeroData = {
      organisations,
      invoices,
      payments,
      contacts,
      bankTransactions,
    };
    const transformedXeroData = await transformXeroData(xeroData);

    try {
      const source = "xero";
      const result = await tcpService.saveTransformedDataToTcp(
        transformedXeroData,
        reportId,
        clientId,
        createdBy,
        source
      );
      if (result) {
        logger.logEvent("info", "Inserted TCP records successfully", {
          count: transformedXeroData.length,
        });
      }
    } catch (err) {
      logger.logEvent(
        "error",
        "Failed to save transformed data to tcp with transaction",
        {
          action: "OAuthCallback-Background",
          error: err.message,
        }
      );
      return;
    }

    logger.logEvent("info", "All Xero data saved successfully to tcp table.");
  } catch (err) {
    logger.logEvent("error", "Error during manual extract", {
      action: "startXeroExtraction",
      error: err.message,
    });
  }
}

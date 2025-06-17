const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();

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

    const { clientId, reportId, createdBy, startDate, endDate } = parsedState;

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
  const { accessToken, clientId, reportId, createdBy, startDate, endDate } =
    req.body;

  setImmediate(() => {
    startXeroExtraction({
      accessToken,
      clientId,
      reportId,
      createdBy,
      startDate,
      endDate,
    });
  });

  res.status(202).json({ message: "Extraction started." });
}

async function startXeroExtraction({
  accessToken,
  clientId,
  reportId,
  createdBy,
  startDate,
  endDate,
}) {
  try {
    // Prepare arrays to collect data from all tenants
    const organisations = [];
    const payments = [];
    const invoices = [];
    const contacts = [];

    // Enhanced: Add tenant-level delay, error handling, and WebSocket updates
    for (const tenant of await xeroService.getConnections(accessToken)) {
      const waitTime = 30000 + Math.floor(Math.random() * 15000); // 30–45s
      logger.logEvent(
        "info",
        `Waiting ${waitTime / 1000}s before syncing tenant ${tenant.id}`
      );
      if (global.sendWebSocketUpdate) {
        global.sendWebSocketUpdate({
          status: "info",
          message: `Waiting ${Math.round(waitTime / 1000)}s before syncing tenant ${tenant.tenantName || tenant.name || tenant.id}`,
          timestamp: new Date().toISOString(),
        });
      }
      await new Promise((resolve) => setTimeout(resolve, waitTime));

      try {
        const tenantId = tenant.tenantId || tenant.id;
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
          tenantPayments,
          createdBy
        );
        contacts.push(...tenantContacts);
      } catch (err) {
        logger.logEvent("error", "Tenant sync failed", {
          tenantId: tenant.id,
          error: err,
        });
        if (global.sendWebSocketUpdate) {
          global.sendWebSocketUpdate({
            status: "error",
            message: `Tenant ${tenant.tenantName || tenant.name || tenant.id} failed: ${err.message}`,
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
    };
    const transformedXeroData = await transformXeroData(xeroData);

    try {
      const result = await tcpService.saveTransformedDataToTcp(
        transformedXeroData,
        reportId,
        clientId,
        createdBy
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

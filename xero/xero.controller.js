const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();

const xeroService = require("./xero.service");
const authorise = require("../middleware/authorise");
const { transformXeroData } = require("../scripts/xero/transformXeroData");
const tcpService = require("../tcp/tcp.service");

router.get("/connect/:reportId/:createdBy", authorise(), generateAuthUrl);
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
      createdBy: req.params?.createdBy,
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

    const { clientId, reportId, createdBy } = parsedState;

    // Redirect the user immediately to the frontend progress page
    const frontendUrl = process.env.FRONTEND_URL || "http://localhost:3000";
    res.redirect(`${frontendUrl}/reports/ptrs/${reportId}/progress`);

    // Continue background processing
    setImmediate(async () => {
      let transaction;
      try {
        // Exchange the code for tokens
        let tokenData;
        try {
          tokenData = await xeroService.exchangeAuthCodeForTokens(
            code,
            state,
            req
          );
        } catch (err) {
          logger.logEvent("error", "Failed to exchange code for tokens", {
            action: "OAuthCallback",
            error: err.message,
          });
          return;
        }

        let { access_token: accessToken } = tokenData;
        if (typeof accessToken !== "string") {
          accessToken = String(accessToken);
        }

        // Get tenant ID
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
          return;
        }

        if (!tenantId) {
          logger.logEvent(
            "error",
            "Tenant ID is missing after connection retrieval",
            { action: "OAuthCallback" }
          );
          return;
        }

        // Hardcoding the start and end dates for testing purposes
        const startDate = "2025-03-01";
        const endDate = "2025-03-31";

        const organisations = await xeroService.fetchOrganisationDetails({
          accessToken,
          tenantId,
          clientId,
          reportId,
        });

        const payments = await xeroService.fetchPayments(
          accessToken,
          tenantId,
          clientId,
          reportId,
          startDate,
          endDate
        );

        const invoices = await xeroService.fetchInvoices(
          accessToken,
          tenantId,
          clientId,
          reportId,
          payments
        );

        const contacts = await xeroService.fetchContacts(
          accessToken,
          tenantId,
          clientId,
          reportId,
          payments
        );

        logger.logEvent(
          "info",
          "Xero data fetched and saved successfully to xero_[tables]."
        );

        // console.log("Organisations:", organisations);
        // console.log("Invoices:", invoices);
        // console.log("Contacts:", contacts);
        // console.log("Payments:", payments);

        const xeroData = { organisations, invoices, payments, contacts };
        const transformedXeroData = await transformXeroData(xeroData);

        try {
          await tcpService.saveTransformedDataToTcp(
            transformedXeroData,
            reportId,
            clientId,
            createdBy
          );

          await transaction.commit();
        } catch (err) {
          if (transaction) await transaction.rollback();
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

        logger.logEvent(
          "info",
          "All Xero data saved successfully to tcp table."
        );
      } catch (err) {
        if (transaction) await transaction.rollback();
        logger.logEvent(
          "error",
          "Error in background OAuth callback processing",
          {
            action: "OAuthCallback-Background",
            error: err.message,
          }
        );
      }
    });
  } catch (err) {
    logger.logEvent("error", "Error in OAuth callback", {
      action: "OAuthCallback",
      error: err.message,
    });
    return res.status(500).send("Internal server error during OAuth callback.");
  }
}

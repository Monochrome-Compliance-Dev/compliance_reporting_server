const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();

const xeroService = require("./xero.service");
const { extractScopeFromTokenData } = require("./xero.service");
const authorise = require("../middleware/authorise");
const requirePtrs = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

router.get(
  "/connect/:ptrsId/:createdBy/:startDate/:endDate",
  requirePtrs,
  generateAuthUrl
);
router.get("/callback", handleOAuthCallback);
router.post("/extract", requirePtrs, startXeroExtractionHandler);
router.post("/contacts/dump", requirePtrs, dumpAllContactsHandler);

// Route to remove a tenant
router.delete("/tenants/:tenantId", requirePtrs, async (req, res, next) => {
  try {
    const { tenantId } = req.params;
    await xeroService.removeTenant(tenantId);
    logger.auditEvent("tenantRemoved", {
      tenantId,
      removedBy: req.auth?.userId,
    });
    return res.status(204).send();
  } catch (err) {
    logger.logEvent("error", "Failed to remove tenant", {
      action: "removeTenant",
      error: err.message,
      stack: err.stack,
      ...(err.context || {}),
      statusCode: err.status || 500,
      timestamp: new Date().toISOString(),
    });
    return next(err);
  }
});

module.exports = router;

function generateAuthUrl(req, res) {
  try {
    const xeroClientId = process.env.XERO_CLIENT_ID;
    const redirectUri =
      process.env.XERO_REDIRECT_URI || "http://localhost:3000/callback";
    // const scopes =
    //   process.env.XERO_SCOPES ||
    //   "openid profile email accounting.transactions.read accounting.contacts.read accounting.settings.read";

    const scopes =
      // process.env.XERO_SCOPES ||
      "openid profile email offline_access accounting.transactions.read accounting.contacts.read accounting.contacts accounting.settings.read";

    if (!xeroClientId) {
      logger.logEvent(
        "error",
        "XERO_CLIENT_ID is not set in environment variables"
      );
      return res
        .status(500)
        .json({ status: "error", message: "Server configuration error." });
    }

    const state = JSON.stringify({
      customerId: req.effectiveCustomerId,
      ptrsId: req.params?.ptrsId,
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
      prompt: "consent", // force re-consent so new scopes (e.g., accounting.contacts) are granted
    });

    const authUrl = `https://login.xero.com/identity/connect/authorize?${params.toString()}`;

    return res.status(200).json({ status: "success", data: { authUrl } });
  } catch (err) {
    logger.logEvent("error", "Error generating auth URL", {
      error: err.message,
      stack: err.stack,
    });
    return res.status(500).json({
      status: "error",
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

  const { ptrsId } = parsedState;

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
        `${frontendUrl}/ptrs/${ptrsId}/selection?error=${encodeURIComponent(description)}`
      );
    }

    if (!code) {
      const description =
        "Missing code in OAuth callback — the connection may have failed or been cancelled unexpectedly.";
      logger.logEvent("error", description, {
        action: "OAuthCallback",
        ptrsId,
      });

      const frontendUrl = process.env.FRONTEND_URL || "http://localhost:3000";
      return res.redirect(
        `${frontendUrl}/ptrs/${ptrsId}/selection?error=${encodeURIComponent(description)}`
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
        ...(err.context || {}),
        statusCode: err.status || 500,
        timestamp: new Date().toISOString(),
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
        ...(err.context || {}),
        statusCode: err.status || 500,
        timestamp: new Date().toISOString(),
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

    // Save token per tenant (removed validation)
    await Promise.all(
      connections.map(async (connection) => {
        const tokenPayload = {
          accessToken,
          refreshToken: tokenData.refresh_token,
          expiresIn: tokenData.expires_in,
          idToken: tokenData.id_token,
          customerId: parsedState.customerId,
          tenantId: connection.tenantId,
          createdBy: parsedState.createdBy,
          scope: extractScopeFromTokenData(tokenData, accessToken),
        };
        await xeroService.saveToken(tokenPayload);
      })
    );

    // Redirect the user immediately to the frontend selection page with orgs in query string
    const frontendUrl = process.env.FRONTEND_URL || "http://localhost:3000";
    const encodedOrgs = encodeURIComponent(JSON.stringify(connections));
    res.redirect(
      `${frontendUrl}/ptrs/${ptrsId}/selection?organisations=${encodedOrgs}`
    );
  } catch (err) {
    logger.logEvent("error", "Error in OAuth callback", {
      action: "OAuthCallback",
      error: err.message,
      stack: err.stack,
      ...(err.context || {}),
    });
    return res.status(500).send("Internal server error during OAuth callback.");
  }
}

async function startXeroExtractionHandler(req, res, next) {
  // Only support ptrsId (no ptrsId fallback)
  const { customerId, ptrsId, createdBy, startDate, endDate, tenantIds } =
    req.body;

  const effectiveCreatedBy = createdBy || req.auth?.userId || "system";

  if (!Array.isArray(tenantIds) || tenantIds.length === 0) {
    return res
      .status(400)
      .json({ status: "error", message: "No tenantIds provided." });
  }

  try {
    for (const tenantId of tenantIds) {
      // Retrieve latest access token for this customer/tenant
      const tokenRecord = await xeroService.getLatestToken({
        customerId,
        tenantId,
      });
      if (!tokenRecord)
        throw new Error(`No access token found for tenant ${tenantId}`);
      const accessToken = tokenRecord.access_token;

      xeroService
        .startXeroExtraction({
          customerId,
          ptrsId,
          createdBy: effectiveCreatedBy,
          startDate,
          endDate,
          tenantId,
          accessToken,
          onProgress: (status) => {
            const logData = {
              ...status,
              customerId,
              ptrsId,
              createdBy: effectiveCreatedBy,
              tenantId,
              timestamp: new Date().toISOString(),
            };
            logger.logEvent("info", "Extraction progress", logData);

            if (req.wss && typeof req.wss.broadcast === "function") {
              try {
                // FLAT payload so the customer can destructure it directly
                req.wss.broadcast(JSON.stringify(logData));
              } catch (wsErr) {
                logger.logEvent(
                  "warn",
                  "Failed to broadcast extraction progress",
                  {
                    error: wsErr.message,
                    stack: wsErr.stack,
                  }
                );
              }
            }
          },
        })
        .catch((err) => {
          logger.logEvent("error", "Xero extraction error", {
            error: err.message,
            stack: err.stack,
            customerId,
            ptrsId,
            tenantId,
          });
        });
    }

    logger.logEvent("info", "Xero extraction started for tenants.", {
      customerId,
      ptrsId,
      createdBy: effectiveCreatedBy,
      tenantIds,
      startDate,
      endDate,
    });
    return res
      .status(202)
      .json({ status: "success", data: { started: true, tenantIds, ptrsId } });
  } catch (err) {
    logger.logEvent("error", "Failed during Xero extraction for tenants", {
      action: "startXeroExtractionHandler",
      error: err.message,
      stack: err.stack,
      ...(err.context || {}),
    });
    return next(err);
  }
}

// apply Xero contact patch (demo/testing -> live via service)
router.post("/apply", requirePtrs, async (req, res, next) => {
  try {
    const { tenantId, dryRun = true, payload } = req.body || {};

    if (!tenantId) {
      return res.status(400).json({ message: "tenantId is required" });
    }
    if (!payload || typeof payload !== "object") {
      return res.status(400).json({ message: "payload must be an object" });
    }

    logger.logEvent("info", "Xero contact patch requested", {
      action: "ApplyPatch",
      tenantId,
      dryRun,
    });

    const result = await xeroService.applyContactUpdate({
      customerId: req.effectiveCustomerId,
      tenantId,
      payload,
      dryRun,
    });

    return res.status(200).json(result);
  } catch (error) {
    next(error);
  }
});

async function dumpAllContactsHandler(req, res, next) {
  const { tenantIds } = req.body || {};
  const customerId = req.effectiveCustomerId;
  const createdBy = req.auth?.userId || "system";

  if (!customerId) {
    return res.status(400).json({
      status: "error",
      message: "Missing customerId in auth context.",
    });
  }

  try {
    const result = await xeroService.dumpAllContactsForAllTenants({
      customerId,
      tenantIds,
      createdBy,
      onProgress: (status) => {
        const logData = {
          ...status,
          customerId,
          createdBy,
          timestamp: new Date().toISOString(),
        };
        logger.logEvent("info", "Dump contacts progress", logData);

        if (req.wss && typeof req.wss.broadcast === "function") {
          try {
            req.wss.broadcast(JSON.stringify(logData));
          } catch (wsErr) {
            logger.logEvent(
              "warn",
              "Failed to broadcast dump contacts progress",
              {
                error: wsErr.message,
                stack: wsErr.stack,
              }
            );
          }
        }
      },
    });

    logger.logEvent("info", "Dump contacts completed", {
      customerId,
      createdBy,
      tenantsProcessed: Array.isArray(result?.tenants)
        ? result.tenants.length
        : 0,
    });

    return res.status(202).json({ status: "success", data: result });
  } catch (err) {
    logger.logEvent("error", "Failed to dump contacts", {
      action: "dumpAllContactsHandler",
      error: err.message,
      stack: err.stack,
      ...(err.context || {}),
    });
    return next(err);
  }
}

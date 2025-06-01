const { logger } = require("../helpers/logger");
const axios = require("axios");
const XeroToken = require("./xero_tokens.model"); // Assuming the model is in this path
const db = require("../db/database");

async function refreshToken() {
  logger.logEvent("info", "Starting token refresh process...", {
    action: "RefreshToken",
  });
  try {
    const response = await axios.post(
      "https://identity.xero.com/connect/token",
      null,
      {
        params: {
          grant_type: "refresh_token",
          refresh_token: "your_refresh_token_here", // Replace with actual refresh token
          client_id: "your_client_id_here", // Replace with actual client ID
          client_secret: "your_client_secret_here", // Replace with actual client secret
        },
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
      }
    );

    const tokenData = response.data;
    logger.logEvent("info", "Token refreshed successfully", {
      action: "RefreshToken",
    });

    // Save the new token data to the database
    await XeroToken.updateOne(
      {},
      {
        accessToken: tokenData.access_token,
        refreshToken: tokenData.refresh_token,
        expiresIn: tokenData.expires_in,
        tokenType: tokenData.token_type,
        scope: tokenData.scope,
        updatedAt: new Date(),
      },
      { upsert: true }
    );

    logger.logEvent("info", "Token data saved to database", {
      action: "RefreshToken",
    });
    return tokenData;
  } catch (error) {
    logger.logEvent("error", "Error refreshing token", {
      action: "RefreshToken",
      error: error.message,
    });
    throw error;
  }
}

/**
 * Fetch contacts from Xero API and save them to the database.
 * @param {Object} options - { accessToken, tenantId, clientId }
 */
async function fetchContacts({ accessToken, tenantId, clientId }) {
  logger.logEvent("info", "Fetching contacts from Xero", {
    action: "FetchContacts",
    clientId,
  });
  try {
    const response = await axios.get(
      "https://api.xero.com/api.xro/2.0/Contacts",
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "Xero-tenant-id": tenantId,
          Accept: "application/json",
        },
      }
    );
    const contacts = response.data.Contacts || [];

    // Save contacts to DB, associating with clientId for RLS
    for (const contact of contacts) {
      await db.XeroContact.upsert({
        ...contact,
        clientId,
      });
    }
    logger.logEvent("info", "Contacts fetched and saved", {
      action: "FetchContacts",
      clientId,
      count: contacts.length,
    });
    return contacts;
  } catch (error) {
    logger.logEvent("error", "Error fetching contacts from Xero", {
      action: "FetchContacts",
      clientId,
      error: error.message,
    });
    throw error;
  }
}

/**
 * Extract data from Xero and store in DB for the given client.
 * @param {Object} options - { accessToken, tenantId, clientId }
 */
async function extract({ accessToken, tenantId, clientId }) {
  logger.logEvent("info", "Extracting data from Xero", {
    action: "ExtractXeroData",
    clientId,
  });
  try {
    // Example: extract Invoices
    const response = await axios.get(
      "https://api.xero.com/api.xro/2.0/Invoices",
      {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "Xero-tenant-id": tenantId,
          Accept: "application/json",
        },
      }
    );
    const invoices = response.data.Invoices || [];
    // Save invoices to DB with clientId for RLS
    for (const invoice of invoices) {
      await db.XeroInvoice.upsert({
        ...invoice,
        clientId,
      });
    }
    logger.logEvent("info", "Xero data extracted and saved", {
      action: "ExtractXeroData",
      clientId,
      count: invoices.length,
    });
    return invoices;
  } catch (error) {
    logger.logEvent("error", "Error extracting data from Xero", {
      action: "ExtractXeroData",
      clientId,
      error: error.message,
    });
    throw error;
  }
}

/**
 * Get transformed data for the current client.
 * @param {Object} options - { clientId }
 */
async function getTransformedData({ clientId }) {
  logger.logEvent("info", "Retrieving transformed data for client", {
    action: "GetTransformedData",
    clientId,
  });
  try {
    // Example: get transformed invoices for this client
    const transformed = await db.TransformedXeroData.findAll({
      where: { clientId },
    });
    logger.logEvent("info", "Transformed data retrieved", {
      action: "GetTransformedData",
      clientId,
      count: transformed.length,
    });
    return transformed;
  } catch (error) {
    logger.logEvent("error", "Error retrieving transformed data", {
      action: "GetTransformedData",
      clientId,
      error: error.message,
    });
    throw error;
  }
}

module.exports = {
  refreshToken,
  fetchContacts,
  extract,
  getTransformedData,
};

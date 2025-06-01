const { logger } = require("../helpers/logger");
const { get, post } = require("../scripts/xero/xeroApi");
const XeroToken = require("./xero_tokens.model"); // Assuming the model is in this path
const db = require("../db/database");
const pLimit = require("p-limit");
const querystring = require("querystring");

module.exports = {
  refreshToken,
  fetchContacts,
  fetchInvoices,
  fetchPayments,
  fetchOrganisationDetails,
  getTransformedData,
  exchangeAuthCodeForTokens,
};

async function exchangeAuthCodeForTokens(code, state) {
  logger.logEvent("info", "Starting exchange of auth code for tokens...", {
    action: "ExchangeAuthCodeForTokens",
  });
  try {
    const params = querystring.stringify({
      grant_type: "authorization_code",
      code,
      redirect_uri: process.env.XERO_REDIRECT_URI,
      client_id: process.env.XERO_CLIENT_ID,
      client_secret: process.env.XERO_CLIENT_SECRET,
    });

    const tokenData = await post(
      "https://identity.xero.com/connect/token",
      params,
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
      }
    );

    logger.logEvent("info", "Auth code exchanged for tokens successfully", {
      action: "ExchangeAuthCodeForTokens",
    });

    // Extract clientId from decoded state (assuming JSON string)
    let clientId;
    try {
      const decodedState = JSON.parse(state);
      clientId = decodedState.clientId;
    } catch (err) {
      logger.logEvent("warn", "Failed to parse state parameter", {
        action: "ExchangeAuthCodeForTokens",
        error: err.message,
      });
    }

    // Save the new token data to the database, including clientId if available
    const updateData = {
      accessToken: tokenData.access_token,
      refreshToken: tokenData.refresh_token,
      expiresIn: tokenData.expires_in,
      tokenType: tokenData.token_type,
      scope: tokenData.scope,
      updatedAt: new Date(),
    };
    if (clientId) {
      updateData.clientId = clientId;
    }

    await XeroToken.updateOne({}, updateData, { upsert: true });

    logger.logEvent("info", "Token data saved to database", {
      action: "ExchangeAuthCodeForTokens",
      clientId,
    });
    return tokenData;
  } catch (error) {
    logger.logEvent("error", "Error exchanging auth code for tokens", {
      action: "ExchangeAuthCodeForTokens",
      error: error.message,
    });
    throw error;
  }
}

async function refreshToken() {
  logger.logEvent("info", "Starting token refresh process...", {
    action: "RefreshToken",
  });
  try {
    const tokenData = await post(
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
    // Collect unique ContactIDs from invoices and payments for the client
    const invoices = await db.XeroInvoice.findAll({ where: { clientId } });
    const payments = await db.XeroPayment.findAll({ where: { clientId } });

    const contactIdsSet = new Set();
    for (const invoice of invoices) {
      if (invoice.Contact && invoice.Contact.ContactID) {
        contactIdsSet.add(invoice.Contact.ContactID);
      }
    }
    for (const payment of payments) {
      if (payment.Contact && payment.Contact.ContactID) {
        contactIdsSet.add(payment.Contact.ContactID);
      }
    }
    const contactIds = Array.from(contactIdsSet);

    const limit = pLimit(5);

    const fetchAndSaveContact = async (contactId) => {
      try {
        const data = await get(`/Contacts/${contactId}`);
        const contact = data.Contact;
        if (contact) {
          await db.XeroContact.upsert({
            ...contact,
            clientId,
          });
        }
      } catch (error) {
        logger.logEvent("error", `Error fetching contact ${contactId}`, {
          action: "FetchContacts",
          clientId,
          contactId,
          error: error.message,
        });
      }
    };

    await Promise.all(
      contactIds.map((id) => limit(() => fetchAndSaveContact(id)))
    );

    logger.logEvent("info", "Contacts fetched and saved", {
      action: "FetchContacts",
      clientId,
      count: contactIds.length,
    });
    return contactIds;
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
async function fetchInvoices({ accessToken, tenantId, clientId }) {
  logger.logEvent("info", "Extracting data from Xero", {
    action: "ExtractXeroData",
    clientId,
  });
  try {
    // Example: extract Invoices
    const data = await get("/Invoices");
    const invoices = data.Invoices || [];
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

/**
 * Fetch payments from Xero API and save them to the database.
 * @param {Object} options - { accessToken, tenantId, clientId }
 */
async function fetchPayments({ accessToken, tenantId, clientId }) {
  logger.logEvent("info", "Extracting payments from Xero", {
    action: "ExtractXeroPayments",
    clientId,
  });
  try {
    const data = await get("/Payments");
    const payments = data.Payments || [];
    // Save payments to DB with clientId for RLS
    for (const payment of payments) {
      await db.XeroPayment.upsert({
        ...payment,
        clientId,
      });
    }
    logger.logEvent("info", "Xero payments extracted and saved", {
      action: "ExtractXeroPayments",
      clientId,
      count: payments.length,
    });
    return payments;
  } catch (error) {
    logger.logEvent("error", "Error extracting payments from Xero", {
      action: "ExtractXeroPayments",
      clientId,
      error: error.message,
    });
    throw error;
  }
}

/**
 * Fetch organisation details from Xero API and save them to the database.
 * @param {Object} options - { accessToken, tenantId, clientId }
 */
async function fetchOrganisationDetails({ accessToken, tenantId, clientId }) {
  logger.logEvent("info", "Extracting organisation details from Xero", {
    action: "ExtractXeroOrganisationDetails",
    clientId,
  });
  try {
    const data = await get("/Organisation");
    const organisations = data.Organisations || [];
    // Save organisation details to DB with clientId for RLS
    for (const org of organisations) {
      await db.XeroOrganisation.upsert({
        ...org,
        clientId,
      });
    }
    logger.logEvent("info", "Xero organisation details extracted and saved", {
      action: "ExtractXeroOrganisationDetails",
      clientId,
      count: organisations.length,
    });
    return organisations;
  } catch (error) {
    logger.logEvent(
      "error",
      "Error extracting organisation details from Xero",
      {
        action: "ExtractXeroOrganisationDetails",
        clientId,
        error: error.message,
      }
    );
    throw error;
  }
}

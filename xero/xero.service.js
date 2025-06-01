const { logger } = require("../helpers/logger");
const { get, post } = require("../scripts/xero/xeroApi");
const defineXeroTokenModel = require("./xero_tokens.model");
const db = require("../db/database");
const pLimit = require("p-limit");
const querystring = require("querystring");

// Ensure XeroToken model is initialized with the sequelize instance from db
const XeroToken = defineXeroTokenModel(db.sequelize);

module.exports = {
  refreshToken,
  fetchContacts,
  fetchInvoices,
  fetchPayments,
  fetchOrganisationDetails,
  getTransformedData,
  exchangeAuthCodeForTokens,
};

async function exchangeAuthCodeForTokens(code, state, req) {
  logger.logEvent("info", "Starting exchange of auth code for tokens...", {
    action: "ExchangeAuthCodeForTokens",
    state,
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
      state,
    });

    // Extract clientId and reportId securely from state if encoded, else fallback to req.auth.clientId
    let clientId = null;
    let reportId = null;
    try {
      const parsedState = JSON.parse(state);
      clientId = parsedState.clientId || req.auth.clientId;
      reportId = parsedState.reportId || null;
    } catch (e) {
      logger.logEvent("warn", "State parsing failed, using req.auth.clientId", {
        action: "ExchangeAuthCodeForTokens",
        state,
        error: e.message,
      });
      clientId = req.auth.clientId;
    }

    if (!clientId) {
      throw new Error(
        "Missing clientId in request or state. Cannot proceed with token exchange."
      );
    }
    logger.logEvent(
      "info",
      "Using clientId and reportId from state or request",
      {
        action: "ExchangeAuthCodeForTokens",
        clientId,
        reportId,
      }
    );

    // Save the new token data to the database, including clientId (mandatory)
    const updateData = {
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token,
      expires: new Date(Date.now() + (tokenData.expires_in || 0) * 1000),
      created: new Date(),
      createdByIp: "", // You may want to pass this in if available
      revoked: null,
      revokedByIp: null,
      replacedByToken: null,
      clientId, // Mandatory for auditing and security
      reportId, // Save reportId if needed for auditing
    };

    await XeroToken.upsert(updateData);

    logger.logEvent("info", "Token data saved to database", {
      action: "ExchangeAuthCodeForTokens",
      clientId,
      reportId,
    });
    return tokenData;
  } catch (error) {
    logger.logEvent("error", "Error exchanging auth code for tokens", {
      action: "ExchangeAuthCodeForTokens",
      error: error.message,
      state,
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
    await XeroToken.upsert({
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token,
      expires: new Date(Date.now() + (tokenData.expires_in || 0) * 1000),
      created: new Date(),
      createdByIp: "", // You may want to pass this in if available
      revoked: null,
      revokedByIp: null,
      replacedByToken: null,
      clientId: null, // You may want to provide actual clientId if available
      reportId: null,
    });

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
 * @param {Object} options - { accessToken, tenantId, clientId, reportId }
 */
async function fetchContacts({ accessToken, tenantId, clientId, reportId }) {
  logger.logEvent("info", "Fetching contacts from Xero", {
    action: "FetchContacts",
    clientId,
    reportId,
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
            reportId,
          });
        }
      } catch (error) {
        logger.logEvent("error", `Error fetching contact ${contactId}`, {
          action: "FetchContacts",
          clientId,
          reportId,
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
      reportId,
      count: contactIds.length,
    });
    return contactIds;
  } catch (error) {
    logger.logEvent("error", "Error fetching contacts from Xero", {
      action: "FetchContacts",
      clientId,
      reportId,
      error: error.message,
    });
    throw error;
  }
}

/**
 * Extract data from Xero and store in DB for the given client.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId }
 */
async function fetchInvoices({ accessToken, tenantId, clientId, reportId }) {
  logger.logEvent("info", "Extracting data from Xero", {
    action: "ExtractXeroData",
    clientId,
    reportId,
  });
  try {
    // Example: extract Invoices
    const data = await get("/Invoices");
    const invoices = data.Invoices || [];
    // Save invoices to DB with clientId and reportId for RLS and auditing
    for (const invoice of invoices) {
      await db.XeroInvoice.upsert({
        ...invoice,
        clientId,
        reportId,
      });
    }
    logger.logEvent("info", "Xero data extracted and saved", {
      action: "ExtractXeroData",
      clientId,
      reportId,
      count: invoices.length,
    });
    return invoices;
  } catch (error) {
    logger.logEvent("error", "Error extracting data from Xero", {
      action: "ExtractXeroData",
      clientId,
      reportId,
      error: error.message,
    });
    throw error;
  }
}

/**
 * Get transformed data for the current client.
 * @param {Object} options - { clientId, reportId }
 */
async function getTransformedData({ clientId, reportId }) {
  logger.logEvent("info", "Retrieving transformed data for client", {
    action: "GetTransformedData",
    clientId,
    reportId,
  });
  try {
    // Example: get transformed invoices for this client and reportId
    const transformed = await db.TransformedXeroData.findAll({
      where: { clientId, reportId },
    });
    logger.logEvent("info", "Transformed data retrieved", {
      action: "GetTransformedData",
      clientId,
      reportId,
      count: transformed.length,
    });
    return transformed;
  } catch (error) {
    logger.logEvent("error", "Error retrieving transformed data", {
      action: "GetTransformedData",
      clientId,
      reportId,
      error: error.message,
    });
    throw error;
  }
}

/**
 * Fetch payments from Xero API and save them to the database.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId }
 */
async function fetchPayments({ accessToken, tenantId, clientId, reportId }) {
  logger.logEvent("info", "Extracting payments from Xero", {
    action: "ExtractXeroPayments",
    clientId,
    reportId,
  });
  try {
    const data = await get("/Payments");
    const payments = data.Payments || [];
    // Save payments to DB with clientId and reportId for RLS and auditing
    for (const payment of payments) {
      await db.XeroPayment.upsert({
        ...payment,
        clientId,
        reportId,
      });
    }
    logger.logEvent("info", "Xero payments extracted and saved", {
      action: "ExtractXeroPayments",
      clientId,
      reportId,
      count: payments.length,
    });
    return payments;
  } catch (error) {
    logger.logEvent("error", "Error extracting payments from Xero", {
      action: "ExtractXeroPayments",
      clientId,
      reportId,
      error: error.message,
    });
    throw error;
  }
}

/**
 * Fetch organisation details from Xero API and save them to the database.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId }
 */
async function fetchOrganisationDetails({
  accessToken,
  tenantId,
  clientId,
  reportId,
}) {
  logger.logEvent("info", "Extracting organisation details from Xero", {
    action: "ExtractXeroOrganisationDetails",
    clientId,
    reportId,
  });
  try {
    const data = await get("/Organisation");
    const organisations = data.Organisations || [];
    // Save organisation details to DB with clientId and reportId for RLS and auditing
    for (const org of organisations) {
      await db.XeroOrganisation.upsert({
        ...org,
        clientId,
        reportId,
      });
    }
    logger.logEvent("info", "Xero organisation details extracted and saved", {
      action: "ExtractXeroOrganisationDetails",
      clientId,
      reportId,
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
        reportId,
        error: error.message,
      }
    );
    throw error;
  }
}

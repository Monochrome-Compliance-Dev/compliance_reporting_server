const { logger } = require("../helpers/logger");
const { get, post } = require("./xeroApi");
const {
  retryWithExponentialBackoff,
  paginateXeroApi,
  extractErrorDetails,
  prepareHeaders,
  rateLimitHandler,
  logApiCall,
  handleXeroApiError,
} = require("./xeroApiUtils");
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
  getConnections,
  getAllXeroData,
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

    // Use retryWithExponentialBackoff for robust token exchange
    const tokenData = await retryWithExponentialBackoff(
      () =>
        post("https://identity.xero.com/connect/token", params, {
          headers: {
            "Content-Type": "application/x-www-form-urlencoded",
          },
        }),
      3,
      1000,
      global.sendWebSocketUpdate
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
      reportId = parsedState.reportId || req.query?.reportId || null;
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
    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        status: "error",
        message: "Error exchanging auth code for tokens: " + error.message,
        code: error.statusCode || error.response?.status || 500,
      });
    }
    throw error;
  }
}

async function refreshToken() {
  logger.logEvent("info", "Starting token refresh process...", {
    action: "RefreshToken",
  });
  try {
    // Use retryWithExponentialBackoff for robust token refresh
    logger.logEvent(
      "debug",
      "[refreshToken] Initiating Xero token refresh request",
      {
        action: "RefreshToken",
        url: "https://identity.xero.com/connect/token",
      }
    );
    const tokenData = await retryWithExponentialBackoff(
      () =>
        post("https://identity.xero.com/connect/token", null, {
          params: {
            grant_type: "refresh_token",
            refresh_token: "your_refresh_token_here", // Replace with actual refresh token
            client_id: "your_client_id_here", // Replace with actual client ID
            client_secret: "your_client_secret_here", // Replace with actual client secret
          },
          headers: {
            "Content-Type": "application/x-www-form-urlencoded",
          },
        }),
      3,
      1000,
      global.sendWebSocketUpdate
    );

    logger.logEvent("debug", "Token data returned from Xero", {
      tokenData,
      statusCode: tokenData?.statusCode || "N/A",
    });

    logger.logEvent("info", "Token refreshed successfully", {
      action: "RefreshToken",
      statusCode: tokenData?.statusCode || "N/A",
    });

    // Prepare data to upsert
    const dbTokenRecord = {
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
    };
    logger.logEvent("debug", "[refreshToken] Writing token values to DB", {
      dbTokenRecord,
    });
    // Save the new token data to the database
    await XeroToken.upsert(dbTokenRecord);

    logger.logEvent("info", "Token data saved to database", {
      action: "RefreshToken",
      savedFields: Object.keys(dbTokenRecord),
    });
    return tokenData;
  } catch (error) {
    logger.logEvent("error", "Error refreshing token", {
      action: "RefreshToken",
      error: error.message,
      statusCode: error.statusCode || error.response?.status || "N/A",
    });
    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        status: "error",
        message: "Error refreshing token: " + error.message,
        code: error.statusCode || error.response?.status || 500,
      });
    }
    throw error;
  }
}

/**
 * Fetch contacts from Xero API and save them to the database.
 * Enhanced logging for API responses, errors, retries, and upserts.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId, payments }
 */
async function fetchContacts(
  accessToken,
  tenantId,
  clientId,
  reportId,
  payments
) {
  logger.logEvent("info", "Fetching contacts from Xero", {
    action: "FetchContacts",
    clientId,
    reportId,
  });
  if (global.sendWebSocketUpdate) {
    global.sendWebSocketUpdate({
      message: "Fetching contacts...",
      type: "status",
    });
  }
  try {
    logger.logEvent(
      "debug",
      "Number of invoices and payments before processing",
      {
        action: "FetchContacts",
        clientId,
        reportId,
        paymentCount: payments.length,
      }
    );

    const contactIdsSet = new Set();
    // Extract ContactID from nested Invoice.Contact object in payments
    for (let i = 0; i < payments.length; i++) {
      const payment = payments[i];
      const invoice = payment.Invoice || {};
      const contact = invoice.Contact || {};
      const contactId = contact.ContactID || null;
      logger.logEvent(
        "debug",
        "[fetchContacts] Extracted contactId from payment",
        { contactId, clientId, reportId, iteration: i, source: "payment" }
      );
      if (contactId) {
        contactIdsSet.add(contactId);
      }
    }
    const contactIds = Array.from(contactIdsSet);

    // Prepare to collect actual contact objects
    const contactResults = [];

    logger.logEvent(
      "debug",
      "[fetchContacts] Final list of contactIds to fetch",
      { contactIds, clientId, reportId }
    );

    const limit = pLimit(5);

    // Use rate-limiting and error extraction utilities for each contact fetch
    const fetchAndSaveContact = async (contactId, idx) => {
      logger.logEvent("info", `Starting fetch for contact ${contactId}`, {
        action: "FetchContacts",
        clientId,
        reportId,
        contactId,
        iteration: idx,
      });
      try {
        let response;
        try {
          response = await retryWithExponentialBackoff(
            () => get(`/Contacts/${contactId}`, accessToken, tenantId),
            3,
            1000,
            global.sendWebSocketUpdate
          );
        } catch (apiError) {
          // Use extractErrorDetails utility
          logger.logEvent("error", `API error fetching contact ${contactId}`, {
            action: "FetchContacts",
            clientId,
            reportId,
            contactId,
            error: extractErrorDetails(apiError),
            status: apiError.response?.status,
            apiErrorBody: apiError.response?.data,
            iteration: idx,
          });
          if (
            apiError.response?.data?.Warnings ||
            apiError.response?.data?.Errors
          ) {
            logger.logEvent("warn", "API error contains warnings/errors", {
              clientId,
              reportId,
              contactId,
              warnings: apiError.response?.data?.Warnings,
              errors: apiError.response?.data?.Errors,
              iteration: idx,
            });
          }
          return;
        }
        logger.logEvent(
          "debug",
          `API call response status: ${response?.status ?? "n/a"}`,
          { contactId, clientId, reportId, iteration: idx }
        );
        logger.logEvent("debug", "API response body", {
          responseBody: response,
          contactId,
          clientId,
          reportId,
          iteration: idx,
        });

        // Xero SDKs may return the body as .Contact or .Contacts, depending on endpoint
        const contact =
          response?.Contact ||
          (response?.Contacts && Array.isArray(response.Contacts)
            ? response.Contacts[0]
            : null);
        if (
          !response ||
          (!contact && !response?.Contact && !response?.Contacts)
        ) {
          logger.logEvent(
            "warn",
            "API response is empty or missing expected fields for contact",
            { contactId, clientId, reportId, response, iteration: idx }
          );
        }
        if (response?.Warnings || response?.Errors) {
          logger.logEvent("warn", "API response contains warnings/errors", {
            clientId,
            reportId,
            contactId,
            warnings: response?.Warnings,
            errors: response?.Errors,
            iteration: idx,
          });
        }
        logger.logEvent("info", `Fetched data for contact ${contactId}`, {
          action: "FetchContacts",
          clientId,
          reportId,
          contactId,
          contactData: contact,
          iteration: idx,
        });
        if (contact) {
          // Collect the contact object
          contactResults.push(contact);
          try {
            await db.XeroContact.upsert({
              clientId,
              reportId,
              ContactID: contact.ContactID,
              Name: contact.Name || "",
              CompanyNumber: contact.CompanyNumber || null,
              TaxNumber: contact.TaxNumber || null,
              DAYSAFTERBILLDATE: contact.DaysaAfterBillDate || null,
              DAYSAFTERBILLMONTH: contact.DaysaAfterBillMonth || null,
              OFCURRENTMONTH: contact.OfCurrentMonth || null,
              OFFOLLOWINGMONTH: contact.OfFollowingMonth || null,
              createdAt: new Date(),
              updatedAt: new Date(),
            });
            logger.logEvent(
              "info",
              `Contact ${contactId} upserted successfully`,
              {
                action: "FetchContacts",
                clientId,
                reportId,
                contactId,
                iteration: idx,
              }
            );
          } catch (error) {
            logger.logEvent("error", "Error during contact upsert", {
              error: extractErrorDetails(error),
              contactId,
              clientId,
              reportId,
              iteration: idx,
            });
          }
        } else {
          logger.logEvent("warn", `No contact found for ${contactId}`, {
            action: "FetchContacts",
            clientId,
            reportId,
            contactId,
            iteration: idx,
          });
        }
      } catch (error) {
        logger.logEvent("error", `Unexpected error in fetchAndSaveContact`, {
          action: "FetchContacts",
          clientId,
          reportId,
          contactId,
          error: extractErrorDetails(error),
          iteration: idx,
        });
      }
    };

    await Promise.all(
      contactIds.map((id, idx) => limit(() => fetchAndSaveContact(id, idx)))
    );

    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        message: "Contacts fetched and saved.",
        type: "status",
      });
    }
    logger.logEvent("info", "Contacts fetched and saved", {
      action: "FetchContacts",
      clientId,
      reportId,
      count: contactResults.length,
    });
    return contactResults;
  } catch (error) {
    handleXeroApiError(error, global.sendWebSocketUpdate);
    logger.logEvent("error", "Error fetching contacts from Xero", {
      action: "FetchContacts",
      clientId,
      reportId,
      error: error.message,
    });
    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        status: "error",
        message: "Error fetching contacts from Xero: " + error.message,
        code: error.statusCode || error.response?.status || 500,
      });
    }
    throw error;
  }
}

/**
 * Extract data from Xero and store in DB for the given client.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId, payments }
 */
async function fetchInvoices(
  accessToken,
  tenantId,
  clientId,
  reportId,
  payments
) {
  logger.logEvent(
    "info",
    "Extracting invoice data from Xero via payment invoiceIds",
    {
      action: "ExtractXeroData",
      clientId,
      reportId,
    }
  );
  if (global.sendWebSocketUpdate) {
    global.sendWebSocketUpdate({
      message: "Fetching invoices...",
      type: "status",
    });
  }
  try {
    // Extract unique InvoiceIDs from payments
    const invoiceIdsSet = new Set();
    payments.forEach((payment) => {
      if (payment.Invoice?.InvoiceID) {
        invoiceIdsSet.add(payment.Invoice.InvoiceID);
      }
    });
    const invoiceIds = Array.from(invoiceIdsSet);

    // Prepare to collect actual invoice objects
    const invoiceResults = [];

    // Use rate-limiting and error extraction utilities for each invoice fetch
    const limit = pLimit(5);
    await Promise.all(
      invoiceIds.map((id) =>
        limit(async () => {
          try {
            const data = await retryWithExponentialBackoff(
              () => get(`/Invoices/${id}`, accessToken, tenantId),
              3,
              1000,
              global.sendWebSocketUpdate
            );
            // Xero returns {Invoices: [invoice]} or {Invoice: {...}} depending on API
            let invoice = null;
            if (data?.Invoice) {
              invoice = data.Invoice;
            } else if (
              Array.isArray(data?.Invoices) &&
              data.Invoices.length > 0
            ) {
              invoice = data.Invoices[0];
            }
            if (invoice) {
              invoiceResults.push(invoice);
              const invoiceRecord = {
                clientId,
                reportId,
                InvoiceID: invoice.InvoiceID || null,
                InvoiceNumber: invoice.InvoiceNumber || null,
                Reference: invoice.Reference || null,
                LineItems: invoice.LineItems || null,
                Type: invoice.Type || null,
                Contact: invoice.Contact || null,
                DateString: invoice.Date || null,
                DueDateString: invoice.DueDate || null,
                Payments: invoice.Payments || null,
                Status: invoice.Status || null,
                AmountPaid: invoice.AmountPaid || null,
                AmountDue: invoice.AmountDue || null,
                AmountCredited: invoice.AmountCredited || null,
                Total: invoice.Total || null,
                createdAt: new Date(),
                updatedAt: new Date(),
              };
              await db.XeroInvoice.upsert(invoiceRecord);
            }
          } catch (error) {
            handleXeroApiError(error, global.sendWebSocketUpdate);
            logger.logEvent("error", "Error extracting invoice from Xero", {
              action: "ExtractXeroData",
              clientId,
              reportId,
              error: extractErrorDetails(error),
              invoiceId: id,
            });
          }
        })
      )
    );

    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        message: "Invoices fetched and saved.",
        type: "status",
      });
    }
    logger.logEvent(
      "info",
      "Xero data extracted and saved via invoiceId loop",
      {
        action: "ExtractXeroData",
        clientId,
        reportId,
        count: invoiceResults.length,
      }
    );
    return invoiceResults;
  } catch (error) {
    handleXeroApiError(error, global.sendWebSocketUpdate);
    logger.logEvent("error", "Error extracting invoice data from Xero", {
      action: "ExtractXeroData",
      clientId,
      reportId,
      error: error.message,
    });
    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        status: "error",
        message: "Error extracting invoice data from Xero: " + error.message,
        code: error.statusCode || error.response?.status || 500,
      });
    }
    throw error;
  }
}

/**
 * Get transformed data for the current client.
 * @param {Object} options - { clientId, reportId }
 */
async function getTransformedData(clientId, reportId, db) {
  // async function getTransformedData(clientId, reportId) { // db is passed in for testing purposes
  logger.logEvent("info", "Retrieving transformed data for client", {
    action: "GetTransformedData",
    clientId,
    reportId,
  });
  if (global.sendWebSocketUpdate) {
    global.sendWebSocketUpdate({
      message: "Transforming data...",
      type: "status",
    });
  }
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
    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        message: "Transformation complete.",
        type: "status",
      });
    }
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
 * Includes all relevant fields as per the Xero API dump and transformation logic.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId }
 */
async function fetchPayments(
  accessToken,
  tenantId,
  clientId,
  reportId,
  startDate,
  endDate
) {
  logger.logEvent("info", "Extracting payments from Xero", {
    action: "ExtractXeroPayments",
    clientId,
    reportId,
    startDate,
    endDate,
  });
  if (global.sendWebSocketUpdate) {
    global.sendWebSocketUpdate({
      message: "Fetching payments...",
      type: "status",
    });
  }
  try {
    // Build Xero-compliant ISO 8601 DateTime filter
    const start = new Date(startDate);
    const end = new Date(endDate);
    const whereClause =
      `Date >= DateTime(${start.getFullYear()}, ${start.getMonth() + 1}, ${start.getDate()})` +
      ` && Date <= DateTime(${end.getFullYear()}, ${end.getMonth() + 1}, ${end.getDate()})`;
    // Optionally log the final URL/params for troubleshooting
    logger.logEvent("debug", "Xero Payments GET where clause", {
      whereClause,
      clientId,
      reportId,
      startDate,
      endDate,
    });
    const data = await retryWithExponentialBackoff(
      () =>
        get("/Payments", accessToken, tenantId, {
          params: {
            where: whereClause,
          },
        }),
      3,
      1000,
      global.sendWebSocketUpdate
    );
    const payments = data.Payments || [];
    console.log("Payments data from Xero:", payments?.length);
    for (const payment of payments) {
      const paymentRecord = {
        clientId,
        reportId,
        PaymentID: payment.PaymentID || null,
        Amount: payment.Amount || null,
        Date: payment.Date,
        Status: payment.Status || null,
        Invoice: payment.Invoice || null,
        createdAt: new Date(),
        updatedAt: new Date(),
      };
      try {
        await db.XeroPayment.upsert(paymentRecord);
      } catch (error) {
        logger.logEvent("error", "Error during payment upsert", {
          error: extractErrorDetails(error),
          paymentId: payment.PaymentID,
          clientId,
          reportId,
        });
      }
    }
    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        message: "SUCCESS: Payments fetched and saved",
        type: "status",
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
    handleXeroApiError(error, global.sendWebSocketUpdate);
    logger.logEvent("error", "Error extracting payments from Xero", {
      action: "ExtractXeroPayments",
      clientId,
      reportId,
      error: error.message,
    });
    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        status: "error",
        message: "Error extracting payments from Xero: " + error.message,
        code: error.statusCode || error.response?.status || 500,
      });
    }
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
  if (global.sendWebSocketUpdate) {
    global.sendWebSocketUpdate({
      message: "Fetching organisation details...",
      type: "status",
    });
  }

  try {
    // Use retryWithExponentialBackoff and extractErrorDetails utility
    const data = await retryWithExponentialBackoff(
      () => get("/Organisation", accessToken, tenantId),
      3,
      1000,
      global.sendWebSocketUpdate
    );
    const organisations = data.Organisations || [];
    console.log("Organisations data from Xero:", organisations?.length);
    for (const org of organisations) {
      const orgRecord = {
        clientId,
        reportId,
        organisationId: org.RegistrationNumber,
        organisationName: org.Name,
        organisationLegalName: org.LegalName,
        organisationAbn: org.TaxNumber || null,
        createdAt: new Date(),
        updatedAt: new Date(),
      };
      try {
        await db.XeroOrganisation.upsert(orgRecord);
      } catch (error) {
        logger.logEvent("error", "Error during organisation upsert", {
          error: extractErrorDetails(error),
          organisationId: org.OrganisationID,
          clientId,
          reportId,
        });
      }
    }
    await db.XeroOrganisation.findAll({
      where: { clientId, reportId },
    });
    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        message: "SUCCESS: Organisation details saved",
        type: "status",
      });
    }
    return organisations;
  } catch (error) {
    handleXeroApiError(error, global.sendWebSocketUpdate);
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
    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        status: "error",
        message:
          "Error extracting organisation details from Xero: " + error.message,
        code: error.statusCode || error.response?.status || 500,
      });
    }
    throw error;
  }
}

/**
 * Get connections (tenants) from Xero API.
 * @param {string} accessToken
 */
async function getConnections(accessToken) {
  logger.logEvent("info", "Fetching Xero connections (tenants)", {
    action: "GetConnections",
  });
  try {
    // Use retryWithExponentialBackoff and extractErrorDetails utility
    const data = await retryWithExponentialBackoff(
      () => get("/connections", accessToken.toString()),
      3,
      1000,
      global.sendWebSocketUpdate
    );
    logger.logEvent("info", "Xero connections retrieved", {
      action: "GetConnections",
      count: Array.isArray(data) ? data.length : 0,
      data,
    });
    return data;
  } catch (error) {
    logger.logEvent("error", "Error fetching Xero connections", {
      action: "GetConnections",
      error: error.message,
    });
    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        status: "error",
        message: "Error fetching Xero connections: " + error.message,
        code: error.statusCode || error.response?.status || 500,
      });
    }
    throw error;
  }
}

/**
 * Fetch all Xero data for a client and reportId.
 * @param {Object} options - { clientId, reportId }
 * @returns {Promise<Object>} - { organisations, invoices, payments, contacts }
 */
async function getAllXeroData(clientId, reportId, db) {
  // async function getAllXeroData(clientId, reportId) { // db is passed in for testing purposes
  logger.logEvent("info", "Fetching all Xero data for client and report", {
    action: "GetAllXeroData",
    clientId,
    reportId,
  });
  if (global.sendWebSocketUpdate) {
    global.sendWebSocketUpdate({
      message: "Fetching all Xero data...",
      type: "status",
    });
  }
  try {
    // Fetch all records from each table filtered by clientId and reportId
    const [organisations, invoices, payments, contacts] = await Promise.all([
      db.XeroOrganisation.findAll({ where: { clientId, reportId } }),
      db.XeroInvoice.findAll({ where: { clientId, reportId } }),
      db.XeroPayment.findAll({ where: { clientId, reportId } }),
      db.XeroContact.findAll({ where: { clientId, reportId } }),
    ]);
    logger.logEvent("info", "Fetched XeroOrganisation records", {
      action: "GetAllXeroData",
      clientId,
      reportId,
      count: organisations.length,
    });
    logger.logEvent("info", "Fetched XeroInvoice records", {
      action: "GetAllXeroData",
      clientId,
      reportId,
      count: invoices.length,
    });
    logger.logEvent("info", "Fetched XeroPayment records", {
      action: "GetAllXeroData",
      clientId,
      reportId,
      count: payments.length,
    });
    logger.logEvent("info", "Fetched XeroContact records", {
      action: "GetAllXeroData",
      clientId,
      reportId,
      count: contacts.length,
    });
    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        message: "SUCCESS: All Xero data fetched",
        type: "status",
      });
    }
    return {
      organisations,
      invoices,
      payments,
      contacts,
    };
  } catch (error) {
    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        status: "error",
        message: "Error fetching all Xero data: " + error.message,
        code: error.statusCode || error.response?.status || 500,
      });
    }
    throw error;
  }
}

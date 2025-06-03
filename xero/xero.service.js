const { logger } = require("../helpers/logger");
const { get, post } = require("./xeroApi");
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
 * Enhanced logging for API responses, errors, retries, and upserts.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId }
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

    logger.logEvent(
      "debug",
      "[fetchContacts] Final list of contactIds to fetch",
      { contactIds, clientId, reportId }
    );

    const limit = pLimit(5);

    // Wrap retryFetch to add retry logging
    async function retryFetchWithLogging(fetchFn, contactId) {
      let lastError;
      for (let i = 0; i < 3; i++) {
        try {
          return await fetchFn();
        } catch (error) {
          lastError = error;
          const isRetryable = i < 2 && error.response?.status === 504;
          logger.logEvent(
            isRetryable ? "warn" : "error",
            isRetryable
              ? `Retrying API call after 504 error (attempt ${i + 1})`
              : `API call failed and will not be retried`,
            {
              action: "FetchContacts",
              clientId,
              reportId,
              contactId,
              retryAttempt: i + 1,
              error: error.message,
              status: error.response?.status,
              apiErrorBody: error.response?.data,
            }
          );
          if (isRetryable) {
            await new Promise((r) => setTimeout(r, 1000));
          } else {
            throw error;
          }
        }
      }
      throw lastError;
    }

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
          response = await retryFetchWithLogging(
            () => get(`/Contacts/${contactId}`, accessToken, tenantId),
            contactId
          );
        } catch (apiError) {
          // Log error with as much info as possible
          logger.logEvent("error", `API error fetching contact ${contactId}`, {
            action: "FetchContacts",
            clientId,
            reportId,
            contactId,
            error: apiError.message,
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
        // Log the full API response status and body
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
              error: error.message,
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
          error: error.message,
          iteration: idx,
        });
      }
    };

    await Promise.all(
      contactIds.map((id, idx) => limit(() => fetchAndSaveContact(id, idx)))
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
  try {
    // Extract unique InvoiceIDs from payments
    const invoiceIdsSet = new Set();
    payments.forEach((payment) => {
      if (payment.Invoice?.InvoiceID) {
        invoiceIdsSet.add(payment.Invoice.InvoiceID);
      }
    });
    const invoiceIds = Array.from(invoiceIdsSet);

    const limit = pLimit(5);
    await Promise.all(
      invoiceIds.map((id) =>
        limit(async () => {
          const data = await retryFetch(() =>
            get(`/Invoices/${id}`, accessToken, tenantId)
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
        })
      )
    );

    logger.logEvent(
      "info",
      "Xero data extracted and saved via invoiceId loop",
      {
        action: "ExtractXeroData",
        clientId,
        reportId,
        count: invoiceIds.length,
      }
    );
    return invoiceIds;
  } catch (error) {
    logger.logEvent("error", "Error extracting invoice data from Xero", {
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
async function getTransformedData(clientId, reportId, db) {
  // async function getTransformedData(clientId, reportId) { // db is passed in for testing purposes
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
  try {
    const data = await retryFetch(() =>
      get("/Payments", accessToken, tenantId)
    );
    const payments = data.Payments || [];
    console.log("Payments data from Xero:", payments?.length);
    // Save payments to DB with clientId and reportId for RLS and auditing
    for (const payment of payments) {
      const paymentRecord = {
        // id: payment.id || null,
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
      await db.XeroPayment.upsert(paymentRecord);
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
    const data = await retryFetch(() =>
      get("/Organisation", accessToken, tenantId)
    );
    const organisations = data.Organisations || [];
    console.log("Organisations data from Xero:", organisations?.length);
    // Save organisation details to DB with clientId and reportId for RLS and auditing
    for (const org of organisations) {
      const orgRecord = {
        clientId,
        reportId,
        organisationId: org.OrganisationID,
        organisationName: org.Name,
        organisationLegalName: org.LegalName,
        organisationAbn: org.ABN || null,
        createdAt: new Date(),
        updatedAt: new Date(),
      };
      await db.XeroOrganisation.upsert(orgRecord);
    }
    await db.XeroOrganisation.findAll({
      where: { clientId, reportId },
    });
    // await db.sequelize.query("COMMIT;");
    // console.log("Transaction committed manually.");
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

/**
 * Retry helper for GET requests.
 * @param {Function} fetchFn - Function that returns a promise
 * @param {number} retries - Number of retries
 * @param {number} delay - Delay between retries (ms)
 */
async function retryFetch(fetchFn, retries = 3, delay = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      return await fetchFn();
    } catch (error) {
      if (i < retries - 1 && error.response?.status === 504) {
        logger.logEvent("warn", `Retrying after 504 error (attempt ${i + 1})`, {
          action: "RetryFetch",
        });
        await new Promise((r) => setTimeout(r, delay));
      } else {
        throw error;
      }
    }
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
    const data = await get("/connections", accessToken.toString());
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
  return {
    organisations,
    invoices,
    payments,
    contacts,
  };
}

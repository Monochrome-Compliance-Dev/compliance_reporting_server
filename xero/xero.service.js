const { logger } = require("../helpers/logger");
const { parseXeroDate } = require("../utils/date-parser");
const { get, post, del } = require("./xeroApi");
const {
  retryWithExponentialBackoff,
  extractErrorDetails,
  handleXeroApiError,
} = require("./xeroApiUtils");
const defineXeroTokenModel = require("./xero_tokens.model");
const db = require("../db/database");
const pLimit = require("p-limit");
const querystring = require("querystring");

// Ensure XeroToken model is initialized with the sequelize instance from db
const XeroToken = defineXeroTokenModel(db.sequelize);

// --- Helper Functions ---
function sendWsStatus(message, type = "status", extra = {}) {
  if (global.sendWebSocketUpdate) {
    global.sendWebSocketUpdate({ message, type, ...extra });
  }
}
function sendWsError(message, error, code) {
  if (global.sendWebSocketUpdate) {
    global.sendWebSocketUpdate({
      status: "error",
      message: message + (error?.message ? ": " + error.message : ""),
      code: code || error?.statusCode || error?.response?.status || 500,
    });
  }
}
function buildWhereClauseDateRange(startDate, endDate, extra = "") {
  const start = new Date(startDate);
  const end = new Date(endDate);
  let clause =
    `Date >= DateTime(${start.getFullYear()}, ${start.getMonth() + 1}, ${start.getDate()})` +
    ` && Date <= DateTime(${end.getFullYear()}, ${end.getMonth() + 1}, ${end.getDate()})`;
  if (extra) clause += " && " + extra;
  return clause;
}
function nowTimestamps(createdBy) {
  return {
    createdBy,
    createdAt: new Date(),
    updatedAt: new Date(),
    source: "Xero",
  };
}

// --- Exported Functions (grouped by logical feature) ---
module.exports = {
  // Auth/Token
  exchangeAuthCodeForTokens,
  refreshToken,
  saveToken,
  getLatestToken,
  removeTenant,
  getConnections,

  // Fetch
  fetchPayments,
  fetchBankTransactions,
  fetchContacts,
  fetchOrganisationDetails,
  fetchInvoices,

  // Save/Transform
  getTransformedData,
  getAllXeroData,

  // Orchestration
  startXeroExtraction,
  syncAllTenants,
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
    const { data: tokenData } = await retryWithExponentialBackoff(
      () =>
        post("https://identity.xero.com/connect/token", params, {
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
        }),
      3,
      1000,
      sendWsStatus
    );
    logger.logEvent("info", "Auth code exchanged for tokens successfully", {
      action: "ExchangeAuthCodeForTokens",
      state,
    });
    // Extract clientId and reportId securely from state if encoded, else fallback to req.auth.clientId
    let clientId = null;
    let reportId = null;
    let tenantId = null;
    try {
      const parsedState = JSON.parse(state);
      clientId = parsedState.clientId || req.auth.clientId;
      reportId = parsedState.reportId || req.query?.reportId || null;
      tenantId = parsedState.tenantId || null;
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
        tenantId,
      }
    );
    return { status: "success", data: tokenData };
  } catch (error) {
    logger.logEvent("error", "Error exchanging auth code for tokens", {
      action: "ExchangeAuthCodeForTokens",
      error: error.message,
      state,
    });
    sendWsError("Error exchanging auth code for tokens", error);
    throw error;
  }
}

async function refreshToken(options = {}) {
  logger.logEvent("info", "Starting token refresh process...", {
    action: "RefreshToken",
  });
  try {
    logger.logEvent(
      "debug",
      "[refreshToken] Initiating Xero token refresh request",
      {
        action: "RefreshToken",
        url: "https://identity.xero.com/connect/token",
      }
    );
    // options: { refreshToken, clientId, clientSecret }
    const params = querystring.stringify({
      grant_type: "refresh_token",
      refresh_token: options.refreshToken || "your_refresh_token_here",
      client_id: options.clientId || "your_client_id_here",
      client_secret: options.clientSecret || "your_client_secret_here",
    });
    const { data: tokenData } = await retryWithExponentialBackoff(
      () =>
        post("https://identity.xero.com/connect/token", params, {
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
        }),
      3,
      1000,
      sendWsStatus
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
      createdByIp: "",
      revoked: null,
      revokedByIp: null,
      replacedByToken: null,
      clientId: options.clientId || null,
      reportId: null,
    };
    logger.logEvent("debug", "[refreshToken] Writing token values to DB", {
      dbTokenRecord,
    });
    await XeroToken.upsert(dbTokenRecord);
    logger.logEvent("info", "Token data saved to database", {
      action: "RefreshToken",
      savedFields: Object.keys(dbTokenRecord),
    });
    return { status: "success", data: tokenData };
  } catch (error) {
    logger.logEvent("error", "Error refreshing token", {
      action: "RefreshToken",
      error: error.message,
      statusCode: error.statusCode || error.response?.status || "N/A",
    });
    sendWsError("Error refreshing token", error);
    throw error;
  }
}

/**
 * Fetch contacts from Xero API and save them to the database.
 * Enhanced logging for API responses, errors, retries, and upserts.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId, transactions }
 */
// TODO: Request a list of contacts by id only
// https://developer.xero.com/documentation/api/accounting/contacts#optimised-use-of-the-where-filter
async function fetchContacts(options) {
  const { accessToken, tenantId, clientId, reportId, transactions, createdBy } =
    options;
  logger.logEvent("info", "Fetching contacts from Xero", {
    action: "FetchContacts",
    clientId,
    reportId,
  });
  sendWsStatus(`Fetching ${tenantId} contacts...`);
  try {
    logger.logEvent(
      "debug",
      "Number of transactions (payments + bankTxns) before processing",
      {
        action: "FetchContacts",
        clientId,
        reportId,
        transactionCount: transactions.length,
      }
    );
    const contactIdsSet = new Set();
    for (let i = 0; i < transactions.length; i++) {
      const record = transactions[i];
      const contactId =
        record.Contact?.ContactID || record.Invoice?.Contact?.ContactID || null;
      logger.logEvent(
        "debug",
        "[fetchContacts] Extracted contactId from record",
        { contactId, clientId, reportId, iteration: i, source: "record" }
      );
      if (contactId) {
        contactIdsSet.add(contactId);
      }
    }
    const contactIds = Array.from(contactIdsSet);
    const contactResults = [];
    logger.logEvent(
      "debug",
      "[fetchContacts] Final list of contactIds to fetch",
      { contactIds, clientId, reportId }
    );
    const limit = pLimit(5);
    const fetchAndSaveContact = async (contactId, idx) => {
      logger.logEvent("info", `Starting fetch for contact ${contactId}`, {
        action: "FetchContacts",
        clientId,
        reportId,
        contactId,
        iteration: idx,
      });
      try {
        let data, headers, status;
        try {
          ({ data, headers, status } = await retryWithExponentialBackoff(
            () => get(`/Contacts/${contactId}`, accessToken, tenantId),
            3,
            1000,
            sendWsStatus
          ));
        } catch (apiError) {
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
          `API call response status: ${status ?? "n/a"}`,
          { contactId, clientId, reportId, iteration: idx }
        );
        logger.logEvent("debug", "API response body", {
          responseBody: data,
          contactId,
          clientId,
          reportId,
          iteration: idx,
        });
        logger.logEvent("debug", "Xero API response headers", { headers });
        const contact =
          data?.Contact ||
          (data?.Contacts && Array.isArray(data.Contacts)
            ? data.Contacts[0]
            : null);
        if (!data || (!contact && !data?.Contact && !data?.Contacts)) {
          logger.logEvent(
            "warn",
            "API response is empty or missing expected fields for contact",
            { contactId, clientId, reportId, data, iteration: idx }
          );
        }
        if (data?.Warnings || data?.Errors) {
          logger.logEvent("warn", "API response contains warnings/errors", {
            clientId,
            reportId,
            contactId,
            warnings: data?.Warnings,
            errors: data?.Errors,
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
          contactResults.push(contact);
          try {
            await db.XeroContact.upsert({
              clientId,
              reportId,
              ContactID: contact.ContactID,
              Name: contact.Name,
              CompanyNumber: contact.CompanyNumber,
              TaxNumber: contact.TaxNumber,
              PaymentTerms: contact.PaymentTerms,
              ...nowTimestamps(createdBy),
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
    sendWsStatus("Contacts fetched and saved.");
    logger.logEvent("info", "Contacts fetched and saved", {
      action: "FetchContacts",
      clientId,
      reportId,
      count: contactResults.length,
    });
    return { status: "success", data: contactResults };
  } catch (error) {
    handleXeroApiError(error, sendWsStatus);
    logger.logEvent("error", "Error fetching contacts from Xero", {
      action: "FetchContacts",
      clientId,
      reportId,
      error: error.message,
    });
    sendWsError("Error fetching contacts from Xero", error);
    throw error;
  }
}

/**
 * Extract data from Xero and store in DB for the given client.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId, payments }
 */
async function fetchInvoices(options) {
  const { accessToken, tenantId, clientId, reportId, payments, createdBy } =
    options;
  logger.logEvent(
    "info",
    "Extracting invoice data from Xero via payment invoiceIds",
    {
      action: "ExtractXeroData",
      clientId,
      reportId,
    }
  );
  sendWsStatus(`Fetching ${tenantId} invoices...`);
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

    // Prepare to collect not found invoices
    const notFoundInvoices = [];

    // Use rate-limiting and error extraction utilities for each invoice fetch
    const limit = pLimit(5);
    await Promise.all(
      invoiceIds.map((id) =>
        limit(async () => {
          try {
            const { data, headers, status } = await retryWithExponentialBackoff(
              () => get(`/Invoices/${id}`, accessToken, tenantId),
              3,
              1000,
              sendWsStatus
            );
            logger.logEvent("debug", "Xero API response headers", { headers });
            let invoice = null;
            if (data?.Invoice) {
              invoice = data.Invoice;
            } else if (
              Array.isArray(data?.Invoices) &&
              data.Invoices.length > 0
            ) {
              invoice = data.Invoices[0];
            }
            if (!invoice) {
              notFoundInvoices.push({
                invoiceId: id,
                reason: "Not returned by Xero API",
              });
            }
            if (invoice) {
              invoiceResults.push(invoice);
              const invoiceRecord = {
                clientId,
                reportId,
                InvoiceID: invoice.InvoiceID,
                InvoiceNumber: invoice.InvoiceNumber,
                Reference: invoice.Reference,
                LineItems: invoice.LineItems,
                Type: invoice.Type,
                Contact: invoice.Contact,
                DateString: invoice.DateString,
                DueDateString: invoice.DueDateString,
                Payments: invoice.Payments,
                Status: invoice.Status,
                AmountDue: invoice.AmountDue,
                AmountPaid: invoice.AmountPaid,
                AmountCredited: invoice.AmountCredited,
                Total: invoice.Total,
                invoicePaymentTermsBillsDay:
                  invoice.invoicePaymentTermsBillsDay,
                invoicePaymentTermsBillsType:
                  invoice.invoicePaymentTermsBillsType,
                invoicePaymentTermsSalesDay:
                  invoice.invoicePaymentTermsSalesDay,
                invoicePaymentTermsSalesType:
                  invoice.invoicePaymentTermsSalesType,
                ...nowTimestamps(createdBy),
              };
              await db.XeroInvoice.upsert(invoiceRecord);
            }
          } catch (error) {
            handleXeroApiError(error, sendWsStatus);
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
    if (notFoundInvoices.length > 0) {
      sendWsStatus("Some invoices could not be retrieved", "warning", {
        missingInvoices: notFoundInvoices,
      });
    }
    sendWsStatus("Invoices fetched and saved.");
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
    return { status: "success", data: invoiceResults };
  } catch (error) {
    handleXeroApiError(error, sendWsStatus);
    logger.logEvent("error", "Error extracting invoice data from Xero", {
      action: "ExtractXeroData",
      clientId,
      reportId,
      error: error.message,
    });
    sendWsError("Error extracting invoice data from Xero", error);
    throw error;
  }
}

/**
 * Get transformed data for the current client.
 * @param {Object} options - { clientId, reportId }
 */
// async function getTransformedData(clientId, reportId, db) {
async function getTransformedData(options) {
  const { clientId, reportId } = options;
  logger.logEvent("info", "Retrieving transformed data for client", {
    action: "GetTransformedData",
    clientId,
    reportId,
  });
  sendWsStatus("Transforming data...");
  try {
    const transformed = await db.TransformedXeroData.findAll({
      where: { clientId, reportId },
    });
    logger.logEvent("info", "Transformed data retrieved", {
      action: "GetTransformedData",
      clientId,
      reportId,
      count: transformed.length,
    });
    sendWsStatus("Transformation complete.");
    return { status: "success", data: transformed };
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
async function fetchPayments(options) {
  const {
    accessToken,
    tenantId,
    clientId,
    reportId,
    startDate,
    endDate,
    createdBy,
  } = options;
  logger.logEvent("info", "Extracting payments from Xero", {
    action: "ExtractXeroPayments",
    clientId,
    reportId,
    startDate,
    endDate,
  });
  sendWsStatus(`Fetching ${tenantId} payments...`);
  try {
    let fetchedAll = false;
    let page = 1;
    const allPayments = [];

    while (!fetchedAll) {
      const whereClause = buildWhereClauseDateRange(
        startDate,
        endDate,
        `Status != "DELETED" && Invoice.Type != "ACCREC" && Invoice.Type != "ACCRECCREDIT"`
      );
      logger.logEvent("debug", "Xero Payments GET where clause", {
        whereClause,
        clientId,
        reportId,
        startDate,
        endDate,
      });
      const { data, headers } = await retryWithExponentialBackoff(
        () =>
          get("/Payments", accessToken, tenantId, {
            params: {
              where: whereClause,
              page,
            },
          }),
        3,
        1000,
        sendWsStatus
      );
      const pageItems = data?.Payments || [];
      for (const payment of pageItems) {
        allPayments.push(payment);
        if (
          payment.Status === "DELETED" ||
          payment.Invoice?.Type === "ACCREC" ||
          payment.Invoice?.Type === "ACCRECCREDIT"
        ) {
          continue;
        }
        const paymentRecord = {
          clientId,
          reportId,
          Amount: payment.Amount,
          PaymentID: payment.PaymentID,
          Date: payment.Date,
          Reference: payment.Reference,
          IsReconciled: payment.IsReconciled,
          Status: payment.Status,
          Invoice: payment.Invoice,
          PaymentType: payment.PaymentType,
          ...nowTimestamps(createdBy),
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
      if (pageItems.length < 100) {
        fetchedAll = true;
      } else {
        page++;
      }
    }
    sendWsStatus("SUCCESS: Payments fetched and saved");
    logger.logEvent("info", "Xero payments extracted and saved", {
      action: "ExtractXeroPayments",
      clientId,
      reportId,
      count: allPayments.length,
    });
    return { status: "success", data: allPayments };
  } catch (error) {
    handleXeroApiError(error, sendWsStatus);
    logger.logEvent("error", "Error extracting payments from Xero", {
      action: "ExtractXeroPayments",
      clientId,
      reportId,
      error: error.message,
    });
    sendWsError("Error extracting payments from Xero", error);
    throw error;
  }
}

/**
 * Fetch organisation details from Xero API and save them to the database.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId }
 */
async function fetchOrganisationDetails(options) {
  const { accessToken, tenantId, clientId, reportId, createdBy } = options;
  logger.logEvent("info", "Extracting organisation details from Xero", {
    action: "ExtractXeroOrganisationDetails",
    clientId,
    reportId,
  });
  sendWsStatus(`Fetching ${tenantId} organisation details...`);
  try {
    const { data, headers, status } = await retryWithExponentialBackoff(
      () => get("/Organisation", accessToken, tenantId),
      3,
      1000,
      sendWsStatus
    );
    logger.logEvent("debug", "Xero API response headers", { headers });
    const organisation =
      Array.isArray(data?.Organisations) && data.Organisations.length > 0
        ? data.Organisations[0]
        : null;
    try {
      if (organisation) {
        const orgRecord = {
          OrganisationID: organisation.OrganisationID,
          Name: organisation.Name,
          LegalName: organisation.LegalName,
          RegistrationNumber: organisation.RegistrationNumber,
          TaxNumber: organisation.TaxNumber,
          PaymentTerms: organisation.PaymentTerms,
          clientId,
          reportId,
          ...nowTimestamps(createdBy),
        };
        await db.XeroOrganisation.upsert(orgRecord);
      }
    } catch (error) {
      logger.logEvent("error", "Error during organisation upsert", {
        error: extractErrorDetails(error),
        organisationId: organisation?.APIKey || organisation?.OrganisationID,
        clientId,
        reportId,
      });
    }
    sendWsStatus("SUCCESS: Organisation details saved");
    return { status: "success", data: organisation };
  } catch (error) {
    handleXeroApiError(error, sendWsStatus);
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
    sendWsError("Error extracting organisation details from Xero", error);
    throw error;
  }
}

/**
 * Get connections (tenants) from Xero API.
 * @param {string} accessToken
 */
async function getConnections(options) {
  const accessToken =
    typeof options === "string" ? options : options.accessToken;
  logger.logEvent("info", "Fetching Xero connections (tenants)", {
    action: "GetConnections",
  });
  try {
    const { data, headers, status } = await retryWithExponentialBackoff(
      () => get("/connections", accessToken.toString()),
      3,
      1000,
      sendWsStatus
    );
    logger.logEvent("info", "Xero connections retrieved", {
      action: "GetConnections",
      count: Array.isArray(data) ? data.length : 0,
      data,
    });
    return { status: "success", data };
  } catch (error) {
    logger.logEvent("error", "Error fetching Xero connections", {
      action: "GetConnections",
      error: error.message,
    });
    sendWsError("Error fetching Xero connections", error);
    throw error;
  }
}

/**
 * Fetch all Xero data for a client and reportId.
 * @param {Object} options - { clientId, reportId }
 * @returns {Promise<Object>} - { organisations, invoices, payments, contacts }
 */
async function getAllXeroData(options) {
  const { clientId, reportId } = options;
  logger.logEvent("info", "Fetching all Xero data for client and report", {
    action: "GetAllXeroData",
    clientId,
    reportId,
  });
  sendWsStatus("Fetching all Xero data...");
  try {
    // Fetch all records from each table filtered by clientId and reportId
    const [organisations, invoices, payments, contacts, bankTransactions] =
      await Promise.all([
        db.XeroOrganisation.findAll({ where: { clientId, reportId } }),
        db.XeroInvoice.findAll({ where: { clientId, reportId } }),
        db.XeroPayment.findAll({ where: { clientId, reportId } }),
        db.XeroContact.findAll({ where: { clientId, reportId } }),
        db.XeroBankTxn.findAll({ where: { clientId, reportId } }),
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
    logger.logEvent("info", "Fetched XeroBankTxn records", {
      action: "GetAllXeroData",
      clientId,
      reportId,
      count: bankTransactions.length,
    });
    sendWsStatus("SUCCESS: All Xero data fetched");
    return {
      status: "success",
      data: { organisations, invoices, payments, contacts, bankTransactions },
    };
  } catch (error) {
    sendWsError("Error fetching all Xero data", error);
    throw error;
  }
}

/**
 * Sync all tenants with staggered delays to avoid Xero rate limits.
 * This version is a placeholder: the actual tenant-level delay, error handling, and WebSocket updates
 * should be implemented directly in the controller loop for better progress reporting.
 */
async function syncAllTenants(tenants, fetchXeroDataForTenant, logger) {
  // See controller for actual implementation with delay, error handling, and WebSocket updates.
  for (const tenant of tenants) {
    await fetchXeroDataForTenant(tenant);
  }
}

/**
 * Remove a Xero tenant connection.
 * @param {string} tenantId
 */
async function removeTenant(tenantId) {
  logger.logEvent("info", "Removing Xero tenant connection", {
    action: "RemoveTenant",
    tenantId,
  });
  logger.logEvent("debug", "Looking for most recent unrevoked access token", {
    action: "RemoveTenant",
    tenantId,
  });
  try {
    const accessTokenRecord = await XeroToken.findOne({
      where: { tenantId, revoked: null },
      order: [["created", "DESC"]],
    });
    if (!accessTokenRecord) {
      logger.logEvent("warn", "No unrevoked access token found", {
        action: "RemoveTenant",
        tenantId,
      });
      throw new Error("No active access token found for removing tenant");
    }
    logger.logEvent("debug", "Access token found for removal", {
      action: "RemoveTenant",
      tenantId,
      tokenCreated: accessTokenRecord.created,
    });
    const accessToken = accessTokenRecord.access_token;
    logger.logEvent("debug", "Sending request to Xero to remove connection", {
      action: "RemoveTenant",
      tenantId,
      tokenSnippet: accessToken?.slice?.(0, 10) + "...",
    });
    try {
      await retryWithExponentialBackoff(
        () => del("/connections/" + tenantId, accessToken),
        3,
        1000,
        sendWsStatus
      );
      logger.logEvent("info", "Tenant removed from Xero successfully", {
        action: "RemoveTenant",
        tenantId,
      });
      return { status: "success", data: null };
    } catch (error) {
      const details = extractErrorDetails(error);
      if (
        details?.Status === 403 &&
        details?.Detail === "AuthenticationUnsuccessful"
      ) {
        logger.logEvent(
          "warn",
          "Token no longer valid for tenant — assuming removal",
          {
            action: "RemoveTenant",
            tenantId,
            fallback: true,
            details,
          }
        );
        logger.logEvent(
          "info",
          "Tenant assumed removed from Xero (token invalid or already removed)",
          {
            action: "RemoveTenant",
            tenantId,
            fallback: true,
          }
        );
        return { status: "success", data: null };
      }
      logger.logEvent("error", "Failed to remove tenant", {
        action: "RemoveTenant",
        tenantId,
        error: details,
      });
      sendWsError("Failed to remove tenant", error);
      throw error;
    }
  } catch (error) {
    logger.logEvent("error", "Failed to remove tenant", {
      action: "RemoveTenant",
      tenantId,
      error: extractErrorDetails(error),
    });
    sendWsError("Failed to remove tenant", error);
    throw error;
  }
}

// Save a Xero token for a tenant/client

/**
 * Save or update a Xero token for a tenant.
 * @param {Object} param0
 */
async function saveToken(options) {
  const {
    accessToken,
    refreshToken,
    expiresIn,
    clientId,
    tenantId,
    createdByIp = "",
  } = options;
  const updateData = {
    access_token: accessToken,
    refresh_token: refreshToken,
    expires: new Date(Date.now() + (expiresIn || 0) * 1000),
    created: new Date(),
    createdByIp,
    revoked: null,
    revokedByIp: null,
    replacedByToken: null,
    clientId,
    tenantId,
  };
  await XeroToken.upsert(updateData);
  return { status: "success", data: null };
}

/**
 * Retrieve the most recent (unrevoked) token for a given clientId.
 * @param {string} clientId
 * @returns {Promise<Object|null>}
 */
async function getLatestToken(options) {
  const { clientId, tenantId } = options;
  const token = await XeroToken.findOne({
    where: { clientId, tenantId, revoked: null },
    order: [["created", "DESC"]],
  });
  return { status: "success", data: token };
}

// Possible fuzzy matching for bank transactions and payments
// import dayjs from "dayjs";

// function matchPaymentsToBankTransactions(
//   payments,
//   bankTransactions,
//   options = {}
// ) {
//   const {
//     dateToleranceDays = 2,
//     amountTolerance = 0.01,
//     useContactMatch = true,
//   } = options;

//   const matched = [];
//   const unmatchedPayments = [];
//   const matchedTransactionIds = new Set();

//   payments.forEach((payment) => {
//     const paymentDate = dayjs(payment.date);
//     const paymentAmount = parseFloat(payment.amount);

//     const match = bankTransactions.find((tx) => {
//       const txDate = dayjs(tx.date);
//       const txAmount = parseFloat(tx.amount);
//       const dateDiff = Math.abs(paymentDate.diff(txDate, "day"));

//       const contactMatch =
//         !useContactMatch ||
//         (payment.contact?.name &&
//           tx.contact?.name &&
//           payment.contact.name.toLowerCase() === tx.contact.name.toLowerCase());

//       const isAmountMatch =
//         Math.abs(paymentAmount - txAmount) <= amountTolerance;

//       const isMatch =
//         isAmountMatch && dateDiff <= dateToleranceDays && contactMatch;

//       return isMatch && !matchedTransactionIds.has(tx.id);
//     });

//     if (match) {
//       matched.push({ payment, bankTransaction: match });
//       matchedTransactionIds.add(match.id);
//     } else {
//       unmatchedPayments.push(payment);
//     }
//   });

//   return {
//     matched,
//     unmatchedPayments,
//     unmatchedBankTransactions: bankTransactions.filter(
//       (tx) => !matchedTransactionIds.has(tx.id)
//     ),
//   };
// }

/**
 * Fetch bank transactions from Xero API and save them to the database.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId, startDate, endDate, createdBy }
 */
async function fetchBankTransactions(options) {
  const {
    accessToken,
    tenantId,
    clientId,
    reportId,
    startDate,
    endDate,
    createdBy,
  } = options;
  logger.logEvent("info", "Extracting bank transactions from Xero", {
    action: "ExtractXeroBankTransactions",
    clientId,
    reportId,
    startDate,
    endDate,
  });
  sendWsStatus(`Fetching ${tenantId} bank transactions...`);
  try {
    let fetchedAll = false;
    let page = 1;
    const allBankTxns = [];
    while (!fetchedAll) {
      const whereClause = buildWhereClauseDateRange(
        startDate,
        endDate,
        `Type == "SPEND"`
      );
      logger.logEvent("debug", "Xero Bank Transactions GET where clause", {
        whereClause,
        clientId,
        reportId,
        startDate,
        endDate,
      });
      const { data, headers } = await retryWithExponentialBackoff(
        () =>
          get("/BankTransactions", accessToken, tenantId, {
            params: {
              where: whereClause,
              page,
            },
          }),
        3,
        1000,
        sendWsStatus
      );
      const pageItems = data?.BankTransactions || [];
      if (!Array.isArray(pageItems)) {
        logger.logEvent("warn", "No bank transactions returned", {
          tenantId,
          clientId,
          reportId,
          page,
        });
        break;
      }
      for (const txn of pageItems) {
        allBankTxns.push(txn);
        try {
          await db.XeroBankTxn.upsert({
            ...txn,
            Date: parseXeroDate(txn.Date, txn.DateString),
            clientId,
            reportId,
            tenantId,
            Url: txn.Url || null,
            Reference: txn.Reference || null,
            ...nowTimestamps(createdBy),
          });
        } catch (err) {
          logger.logEvent("error", "Error during bank transaction upsert", {
            error: extractErrorDetails(err),
            bankTransactionId: txn.BankTransactionID,
            clientId,
            reportId,
          });
        }
      }
      if (pageItems.length < 100) {
        fetchedAll = true;
      } else {
        page++;
      }
    }
    sendWsStatus("SUCCESS: Bank transactions fetched and saved");
    logger.logEvent("info", "Bank transactions extracted and saved", {
      action: "ExtractXeroBankTransactions",
      clientId,
      reportId,
      count: allBankTxns.length,
    });
    return { status: "success", data: allBankTxns };
  } catch (error) {
    handleXeroApiError(error, sendWsStatus);
    logger.logEvent("error", "Error extracting bank transactions from Xero", {
      action: "ExtractXeroBankTransactions",
      clientId,
      reportId,
      error: error.message,
    });
    sendWsError("Error extracting bank transactions from Xero", error);
    throw error;
  }
}

/**
 * Starts the full extraction process from Xero, orchestrating all sub-fetches and reporting progress.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId, startDate, endDate, createdBy }
 */
async function startXeroExtraction(options) {
  const {
    accessToken,
    tenantId,
    clientId,
    reportId,
    startDate,
    endDate,
    createdBy,
  } = options;
  logger.logEvent("info", "Starting full Xero extraction", {
    action: "StartXeroExtraction",
    clientId,
    reportId,
    startDate,
    endDate,
  });
  sendWsStatus(`Starting Xero data extraction for tenant ${tenantId}...`);
  try {
    const [paymentsResult, bankTransactionsResult] = await Promise.all([
      fetchPayments({
        accessToken,
        tenantId,
        clientId,
        reportId,
        startDate,
        endDate,
        createdBy,
      }),
      fetchBankTransactions({
        accessToken,
        tenantId,
        clientId,
        reportId,
        startDate,
        endDate,
        createdBy,
      }),
    ]);
    const payments = paymentsResult.data;
    const bankTransactions = bankTransactionsResult.data;
    await fetchContacts({
      accessToken,
      tenantId,
      clientId,
      reportId,
      transactions: [...payments, ...bankTransactions],
      createdBy,
    });
    await fetchOrganisationDetails({
      accessToken,
      tenantId,
      clientId,
      reportId,
      createdBy,
    });
    await fetchInvoices({
      accessToken,
      tenantId,
      clientId,
      reportId,
      payments,
      createdBy,
    });
    logger.logEvent("info", "Xero extraction completed successfully", {
      action: "StartXeroExtraction",
      clientId,
      reportId,
    });
    sendWsStatus("SUCCESS: Xero extraction completed.");
    return { status: "success" };
  } catch (error) {
    logger.logEvent("error", "Xero extraction failed", {
      action: "StartXeroExtraction",
      clientId,
      reportId,
      error: extractErrorDetails(error),
    });
    sendWsError("Xero extraction failed", error);
    throw error;
  }
}

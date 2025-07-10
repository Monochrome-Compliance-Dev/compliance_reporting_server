// Removed logger import; logging will be removed from service layer per gold standard.
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
// Removed sendWsStatus and sendWsError per gold standard: no side effects in service layer.

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
      1000
    );
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
      clientId = req.auth.clientId;
    }
    if (!clientId) {
      throw new Error(
        "Missing clientId in request or state. Cannot proceed with token exchange."
      );
    }
    return { status: "success", data: tokenData };
  } catch (error) {
    throw error;
  }
}

async function refreshToken(options = {}) {
  try {
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
      1000
    );
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
    await XeroToken.upsert(dbTokenRecord);
    return { status: "success", data: tokenData };
  } catch (error) {
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
  const {
    accessToken,
    tenantId,
    clientId,
    reportId,
    transactions,
    createdBy,
    onProgress = () => {},
  } = options;
  const contactIds = Array.from(
    new Set(
      transactions.flatMap((txn) =>
        [txn.Contact?.ContactID, txn.Invoice?.Contact?.ContactID].filter(
          Boolean
        )
      )
    )
  );
  const results = [];
  const limit = pLimit(5);
  await Promise.all(
    contactIds.map((id, idx) =>
      limit(async () => {
        onProgress({ stage: "fetchContacts", contactId: id, index: idx });
        const { data } = await retryWithExponentialBackoff(
          () => get(`/Contacts/${id}`, accessToken, tenantId),
          3,
          1000
        ).catch(() => ({}));
        const contact = data?.Contact || data?.Contacts?.[0];
        if (contact) {
          results.push(contact);
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
        }
      })
    )
  );
  return { status: "success", data: results };
}

/**
 * Extract data from Xero and store in DB for the given client.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId, payments }
 */
async function fetchInvoices(options) {
  const {
    accessToken,
    tenantId,
    clientId,
    reportId,
    payments,
    createdBy,
    onProgress = () => {},
  } = options;
  const invoiceIds = Array.from(
    new Set(payments.map((p) => p.Invoice?.InvoiceID).filter(Boolean))
  );
  const results = [];
  const limit = pLimit(5);
  await Promise.all(
    invoiceIds.map((id, idx) =>
      limit(async () => {
        onProgress({ stage: "fetchInvoices", invoiceId: id, index: idx });
        const { data } = await retryWithExponentialBackoff(
          () => get(`/Invoices/${id}`, accessToken, tenantId),
          3,
          1000
        ).catch(() => ({}));
        const invoice = data?.Invoice || data?.Invoices?.[0];
        if (invoice) {
          results.push(invoice);
          await db.XeroInvoice.upsert({
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
            invoicePaymentTermsBillsDay: invoice.invoicePaymentTermsBillsDay,
            invoicePaymentTermsBillsType: invoice.invoicePaymentTermsBillsType,
            invoicePaymentTermsSalesDay: invoice.invoicePaymentTermsSalesDay,
            invoicePaymentTermsSalesType: invoice.invoicePaymentTermsSalesType,
            ...nowTimestamps(createdBy),
          });
        }
      })
    )
  );
  return { status: "success", data: results };
}

/**
 * Get transformed data for the current client.
 * @param {Object} options - { clientId, reportId }
 */
// async function getTransformedData(clientId, reportId, db) {
async function getTransformedData(options) {
  const { clientId, reportId } = options;
  try {
    const transformed = await db.TransformedXeroData.findAll({
      where: { clientId, reportId },
    });
    return { status: "success", data: transformed };
  } catch (error) {
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
    onProgress = () => {},
  } = options;
  try {
    let fetchedAll = false;
    let page = 1;
    const allPayments = [];

    while (!fetchedAll) {
      onProgress({ stage: "fetchPayments", page });
      const whereClause = buildWhereClauseDateRange(
        startDate,
        endDate,
        `Status != "DELETED" && Invoice.Type != "ACCREC" && Invoice.Type != "ACCRECCREDIT"`
      );
      const { data } = await retryWithExponentialBackoff(
        () =>
          get("/Payments", accessToken, tenantId, {
            params: { where: whereClause, page },
          }),
        3,
        1000
      );
      const pageItems = data?.Payments || [];
      for (const payment of pageItems) {
        allPayments.push(payment);
        if (
          payment.Status === "DELETED" ||
          payment.Invoice?.Type === "ACCREC" ||
          payment.Invoice?.Type === "ACCRECCREDIT"
        )
          continue;
        await db.XeroPayment.upsert({
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
        });
      }
      fetchedAll = pageItems.length < 100;
      page++;
    }
    return { status: "success", data: allPayments };
  } catch (error) {
    throw error;
  }
}

/**
 * Fetch organisation details from Xero API and save them to the database.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId }
 */
async function fetchOrganisationDetails(options) {
  const {
    accessToken,
    tenantId,
    clientId,
    reportId,
    createdBy,
    onProgress = () => {},
  } = options;
  onProgress({ stage: "fetchOrganisationDetails" });
  const { data } = await retryWithExponentialBackoff(
    () => get("/Organisation", accessToken, tenantId),
    3,
    1000
  );
  const org = data?.Organisations?.[0];
  if (org) {
    await db.XeroOrganisation.upsert({
      OrganisationID: org.OrganisationID,
      Name: org.Name,
      LegalName: org.LegalName,
      RegistrationNumber: org.RegistrationNumber,
      TaxNumber: org.TaxNumber,
      PaymentTerms: org.PaymentTerms,
      clientId,
      reportId,
      ...nowTimestamps(createdBy),
    });
  }
  return { status: "success", data: org };
}

/**
 * Get connections (tenants) from Xero API.
 * @param {string} accessToken
 */
async function getConnections(options) {
  const accessToken =
    typeof options === "string" ? options : options.accessToken;
  try {
    const { data, headers, status } = await retryWithExponentialBackoff(
      () => get("/connections", accessToken.toString()),
      3,
      1000
    );
    return { status: "success", data };
  } catch (error) {
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
    return {
      status: "success",
      data: { organisations, invoices, payments, contacts, bankTransactions },
    };
  } catch (error) {
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
  try {
    const accessTokenRecord = await XeroToken.findOne({
      where: { tenantId, revoked: null },
      order: [["created", "DESC"]],
    });
    if (!accessTokenRecord) {
      throw new Error("No active access token found for removing tenant");
    }
    const accessToken = accessTokenRecord.access_token;
    try {
      await retryWithExponentialBackoff(
        () => del("/connections/" + tenantId, accessToken),
        3,
        1000
      );
      return { status: "success", data: null };
    } catch (error) {
      const details = extractErrorDetails(error);
      if (
        details?.Status === 403 &&
        details?.Detail === "AuthenticationUnsuccessful"
      ) {
        return { status: "success", data: null };
      }
      throw error;
    }
  } catch (error) {
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
    onProgress = () => {},
  } = options;
  try {
    let fetchedAll = false;
    let page = 1;
    const allBankTxns = [];
    while (!fetchedAll) {
      onProgress({ stage: "fetchBankTransactions", page });
      const whereClause = buildWhereClauseDateRange(
        startDate,
        endDate,
        `Type == "SPEND"`
      );
      const { data } = await retryWithExponentialBackoff(
        () =>
          get("/BankTransactions", accessToken, tenantId, {
            params: { where: whereClause, page },
          }),
        3,
        1000
      );
      const pageItems = data?.BankTransactions || [];
      for (const txn of pageItems) {
        allBankTxns.push(txn);
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
      }
      fetchedAll = pageItems.length < 100;
      page++;
    }
    return { status: "success", data: allBankTxns };
  } catch (error) {
    throw error;
  }
}

/**
 * Starts the full extraction process from Xero, orchestrating all sub-fetches and reporting progress.
 * @param {Object} options - { accessToken, tenantId, clientId, reportId, startDate, endDate, createdBy, onProgress }
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
    onProgress = () => {},
  } = options;

  // Fetch organisation details
  await fetchOrganisationDetails({
    accessToken,
    tenantId,
    clientId,
    reportId,
    createdBy,
    onProgress,
  });

  // Fetch payments
  const { data: payments } = await fetchPayments({
    accessToken,
    tenantId,
    clientId,
    reportId,
    startDate,
    endDate,
    createdBy,
    onProgress,
  });

  // Fetch invoices
  await fetchInvoices({
    accessToken,
    tenantId,
    clientId,
    reportId,
    payments,
    createdBy,
    onProgress,
  });

  // Fetch bank transactions
  await fetchBankTransactions({
    accessToken,
    tenantId,
    clientId,
    reportId,
    startDate,
    endDate,
    createdBy,
    onProgress,
  });

  // Fetch contacts
  await fetchContacts({
    accessToken,
    tenantId,
    clientId,
    reportId,
    transactions: payments,
    createdBy,
    onProgress,
  });

  return { status: "success", message: "Xero extraction completed." };
}

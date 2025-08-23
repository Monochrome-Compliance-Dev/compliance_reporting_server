// Helper: Truncate string fields >255 chars, unless valid JSON
function trimStringIfTooLong(value) {
  if (typeof value !== "string") return value;
  try {
    JSON.parse(value);
    return value; // don't trim if valid JSON
  } catch {
    return value.length > 255 ? value.substring(0, 255) : value;
  }
}
// Removed logger import; logging will be removed from service layer per gold standard.
const { parseXeroDate } = require("../utils/date-parser");
const { get, post, del, resetXeroProgress } = require("./xeroApi");
const {
  retryWithExponentialBackoff,
  extractErrorDetails,
  handleXeroApiError,
  callXeroApiWithAutoRefresh,
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
      customer_id: process.env.XERO_CLIENT_ID,
      customer_secret: process.env.XERO_CLIENT_SECRET,
    });
    const { data: tokenData } = await retryWithExponentialBackoff(
      () =>
        post("https://identity.xero.com/connect/token", params, null, null, {
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
        }),
      3,
      1000
    );
    // Extract customerId and ptrsId securely from state if encoded, else fallback to req.auth.customerId
    let customerId = null;
    let ptrsId = null;
    let tenantId = null;
    try {
      const parsedState = JSON.parse(state);
      customerId = parsedState.customerId || req.auth.customerId;
      ptrsId = parsedState.ptrsId || req.query?.ptrsId || null;
      tenantId = parsedState.tenantId || null;
    } catch (e) {
      customerId = req.auth.customerId;
    }
    if (!customerId) {
      throw new Error(
        "Missing customerId in request or state. Cannot proceed with token exchange."
      );
    }
    return tokenData;
  } catch (error) {
    throw error;
  }
}

async function refreshToken(options = {}) {
  try {
    const rt = options.refreshToken;
    const cid = options.customerId || process.env.XERO_CLIENT_ID;
    const csec = options.customerSecret || process.env.XERO_CLIENT_SECRET;
    if (!rt || !cid || !csec) {
      throw new Error(
        "refreshToken: missing refreshToken/customerId/customerSecret"
      );
    }
    const params = querystring.stringify({
      grant_type: "refresh_token",
      refresh_token: rt,
      customer_id: cid,
      customer_secret: csec,
    });
    const { data: tokenData } = await retryWithExponentialBackoff(
      () =>
        post("https://identity.xero.com/connect/token", params, null, null, {
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
        }),
      3,
      1000
    );

    await XeroToken.upsert({
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token,
      expires: new Date(Date.now() + (tokenData.expires_in || 0) * 1000),
      created: new Date(),
      createdByIp: "",
      revoked: null,
      revokedByIp: null,
      replacedByToken: null,
      customerId: options.customerId || null,
      tenantId: options.tenantId || null,
      ptrsId: null,
    });

    return tokenData;
  } catch (error) {
    console.log(`[xero.refreshToken] Failed: ${error?.message}`);
    throw error;
  }
}

async function refreshAccessTokenFor(customerId, tenantId) {
  let tokenRecord;
  if (tenantId) {
    tokenRecord = await getLatestToken({ customerId, tenantId });
  } else {
    tokenRecord = await XeroToken.findOne({
      where: { customerId, revoked: null },
      order: [["created", "DESC"]],
    });
  }
  const refreshTok = tokenRecord?.refresh_token || tokenRecord?.refreshToken;
  if (!refreshTok) {
    throw new Error(
      `No refresh token available for ${tenantId ? `tenant ${tenantId}` : `customer ${customerId}`}`
    );
  }
  return refreshToken({
    refreshToken: refreshTok,
    customerId: process.env.XERO_CLIENT_ID,
    customerSecret: process.env.XERO_CLIENT_SECRET,
    tenantId,
  });
}

/**
 * Fetch contacts from Xero API and save them to the database.
 * Accepts payments array, extracts unique contact IDs, fetches missing, upserts, and returns all.
 * @param {Object} options - { accessToken, tenantId, customerId, ptrsId, payments }
 */
// TODO: Request a list of contacts by id only
// https://developer.xero.com/documentation/api/accounting/contacts#optimised-use-of-the-where-filter
async function fetchContacts(options) {
  const {
    accessToken,
    tenantId,
    customerId,
    ptrsId,
    payments,
    createdBy,
    onProgress = () => {},
  } = options;
  console.log("[fetchContacts] Starting contacts fetch...");
  // Extract unique contact IDs from payments
  const contactIds = Array.from(
    new Set(
      payments
        .flatMap((payment) => [
          payment.Contact?.ContactID,
          payment.Invoice?.Contact?.ContactID,
        ])
        .filter(Boolean)
    )
  );
  console.log(
    `[fetchContacts] Extracted ${contactIds.length} unique contact IDs`
  );
  if (!contactIds.length) {
    console.log(
      "[fetchContacts] No contacts to fetch, returning existing from DB."
    );
    return db.XeroContact.findAll({ where: { customerId, ptrsId } });
  }
  // Check DB for existing contacts for this customer/period and only fetch missing ones
  const existing = await db.XeroContact.findAll({
    where: { customerId, ptrsId, ContactID: contactIds },
    attributes: ["ContactID"],
  });
  const existingIds = new Set(existing.map((r) => r.ContactID));
  console.log(
    `[fetchContacts] Found ${existingIds.size} contact IDs already in DB`
  );

  const idsToFetch = contactIds.filter((id) => !existingIds.has(id));
  console.log(
    `[fetchContacts] Will fetch ${idsToFetch.length} contacts from Xero`
  );

  const limit = pLimit(5);
  await Promise.all(
    idsToFetch.map((id, idx) =>
      limit(async () => {
        console.log(
          `[fetchContacts] Fetching contact ID: ${id} (index ${idx})`
        );
        onProgress({ stage: "fetchContacts", contactId: id, index: idx });
        const { data } = await callXeroApiWithAutoRefresh(
          () =>
            withLatestAccessToken(customerId, tenantId, (at) =>
              retryWithExponentialBackoff(
                () => get(`/Contacts/${encodeURIComponent(id)}`, at, tenantId),
                3,
                1000
              )
            ),
          customerId,
          () => refreshAccessTokenFor(customerId, tenantId)
        ).catch((err) => {
          console.log(
            `[fetchContacts] Fetch failed for ContactID=${id} :: ${err?.response?.status || err?.status || "n/a"} ${err?.message || ""}`
          );
          return {};
        });

        const contact = data?.Contact || data?.Contacts?.[0];
        if (contact?.ContactID) {
          await db.XeroContact.upsert({
            customerId,
            ptrsId,
            ContactID: contact.ContactID,
            Name: trimStringIfTooLong(contact.Name),
            CompanyNumber: trimStringIfTooLong(contact.CompanyNumber),
            TaxNumber: trimStringIfTooLong(contact.TaxNumber),
            PaymentTerms: trimStringIfTooLong(contact.PaymentTerms),
            ...nowTimestamps(createdBy),
          });
          onProgress({
            stage: "fetchContacts",
            contactId: id,
            action: "upserted",
          });
        } else {
          console.log(
            `[fetchContacts] No contact payload returned for ContactID=${id}; skipping upsert`
          );
        }
      })
    )
  );
  // Return all contacts for this ptrsId (including those just fetched)
  const allContacts = await db.XeroContact.findAll({
    where: { customerId, ptrsId },
  });
  console.log(
    `[fetchContacts] Finished. Total contacts now in DB for customerId=${customerId}, ptrsId=${ptrsId}: ${allContacts.length}`
  );
  return allContacts;
}

/**
 * Extract data from Xero and store in DB for the given customer.
 * @param {Object} options - { accessToken, tenantId, customerId, ptrsId, payments }
 */
async function fetchInvoices(options) {
  const {
    accessToken,
    tenantId,
    customerId,
    ptrsId,
    payments,
    createdBy,
    onProgress = () => {},
  } = options;
  console.log("[fetchInvoices] Starting invoice fetch...");
  const invoiceIds = Array.from(
    new Set(payments.map((p) => p.Invoice?.InvoiceID).filter(Boolean))
  );
  console.log(`[fetchInvoices] Found ${invoiceIds.length} unique invoice IDs`);
  const results = [];
  const limit = pLimit(5);
  await Promise.all(
    invoiceIds.map((id, idx) =>
      limit(async () => {
        console.log(
          `[fetchInvoices] Fetching invoice ID: ${id} (index ${idx})`
        );
        onProgress({ stage: "fetchInvoices", invoiceId: id, index: idx });
        const { data } = await callXeroApiWithAutoRefresh(
          () =>
            withLatestAccessToken(customerId, tenantId, (at) =>
              retryWithExponentialBackoff(
                () => get(`/Invoices/${id}`, at, tenantId),
                3,
                1000
              )
            ),
          customerId,
          () => refreshAccessTokenFor(customerId, tenantId)
        ).catch(() => ({}));
        const invoice = data?.Invoice || data?.Invoices?.[0];
        if (invoice) {
          results.push(invoice);
          await db.XeroInvoice.upsert({
            customerId,
            ptrsId,
            tenantId,
            InvoiceID: invoice.InvoiceID,
            InvoiceNumber: trimStringIfTooLong(invoice.InvoiceNumber),
            Reference: trimStringIfTooLong(invoice.Reference),
            LineItems: invoice.LineItems,
            Type: trimStringIfTooLong(invoice.Type),
            Contact: invoice.Contact,
            DateString: invoice.DateString,
            DueDateString: invoice.DueDateString,
            Payments: invoice.Payments,
            Status: trimStringIfTooLong(invoice.Status),
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
        } else {
          console.log(`[fetchInvoices] No invoice found for ID: ${id}`);
        }
      })
    )
  );
  console.log(
    `[fetchInvoices] Finished. Total invoices fetched: ${results.length}`
  );
  return results;
}

/**
 * Get transformed data for the current customer.
 * @param {Object} options - { customerId, ptrsId }
 */
// async function getTransformedData(customerId, ptrsId, db) {
async function getTransformedData(options) {
  const { customerId, ptrsId } = options;
  try {
    const transformed = await db.TransformedXeroData.findAll({
      where: { customerId, ptrsId },
    });
    return transformed;
  } catch (error) {
    throw error;
  }
}

/**
 * Fetch payments from Xero API and save them to the database.
 * Includes all relevant fields as per the Xero API dump and transformation logic.
 * @param {Object} options - { accessToken, tenantId, customerId, ptrsId }
 */
async function fetchPayments(options) {
  const {
    accessToken,
    tenantId,
    customerId,
    ptrsId,
    startDate,
    endDate,
    createdBy,
    onProgress = () => {},
  } = options;
  try {
    console.log("[fetchPayments] Starting payments fetch...");
    let fetchedAll = false;
    let page = 1;
    const allPayments = [];
    while (!fetchedAll) {
      console.log(`[fetchPayments] Fetching page ${page}`);
      onProgress({ stage: "fetchPayments", page });
      const whereClause = buildWhereClauseDateRange(
        startDate,
        endDate,
        `Status != "DELETED" && Invoice.Type != "ACCREC" && Invoice.Type != "ACCRECCREDIT"`
      );
      const { data } = await callXeroApiWithAutoRefresh(
        () =>
          withLatestAccessToken(customerId, tenantId, (at) =>
            retryWithExponentialBackoff(
              () =>
                get("/Payments", at, tenantId, {
                  params: { where: whereClause, page },
                }),
              3,
              1000
            )
          ),
        customerId,
        () => refreshAccessTokenFor(customerId, tenantId)
      );
      const pageItems = data?.Payments || [];
      console.log(
        `[fetchPayments] Page ${page} received ${pageItems.length} payments`
      );
      for (const payment of pageItems) {
        allPayments.push(payment);
        if (
          payment.Status === "DELETED" ||
          payment.Invoice?.Type === "ACCREC" ||
          payment.Invoice?.Type === "ACCRECCREDIT"
        )
          continue;
        await db.XeroPayment.upsert({
          customerId,
          ptrsId,
          tenantId,
          Amount: payment.Amount,
          PaymentID: payment.PaymentID,
          Date: payment.Date,
          Reference: trimStringIfTooLong(payment.Reference),
          IsReconciled: payment.IsReconciled,
          Status: trimStringIfTooLong(payment.Status),
          Invoice: payment.Invoice,
          PaymentType: trimStringIfTooLong(payment.PaymentType),
          ...nowTimestamps(createdBy),
        });
      }
      fetchedAll = pageItems.length < 100;
      page++;
      console.log(
        `[fetchPayments] Total payments accumulated so far: ${allPayments.length}`
      );
    }
    console.log(
      `[fetchPayments] Finished. Total payments fetched: ${allPayments.length}`
    );
    return allPayments;
  } catch (error) {
    console.error("[fetchPayments] Error:", error);
    throw error;
  }
}

/**
 * Fetch organisation details from Xero API and save them to the database.
 * @param {Object} options - { accessToken, tenantId, customerId, ptrsId }
 */
async function fetchOrganisationDetails(options) {
  const {
    accessToken,
    tenantId,
    customerId,
    ptrsId,
    createdBy,
    onProgress = () => {},
  } = options;
  onProgress({ stage: "fetchOrganisationDetails" });
  const { data } = await callXeroApiWithAutoRefresh(
    () =>
      withLatestAccessToken(customerId, tenantId, (at) =>
        retryWithExponentialBackoff(
          () => get("/Organisation", at, tenantId),
          3,
          1000
        )
      ),
    customerId,
    () => refreshAccessTokenFor(customerId, tenantId)
  );
  const org = data?.Organisations?.[0];
  if (org) {
    await db.XeroOrganisation.upsert({
      OrganisationID: org.OrganisationID,
      Name: trimStringIfTooLong(org.Name),
      LegalName: trimStringIfTooLong(org.LegalName),
      RegistrationNumber: trimStringIfTooLong(org.RegistrationNumber),
      TaxNumber: trimStringIfTooLong(org.TaxNumber),
      PaymentTerms: trimStringIfTooLong(org.PaymentTerms),
      customerId,
      ptrsId,
      ...nowTimestamps(createdBy),
    });
  }
  return org;
}

/**
 * Get connections (tenants) from Xero API.
 * @param {string} accessToken
 */
async function getConnections(options) {
  // Accept either a raw token string or { accessToken, customerId }
  const providedAccessToken =
    typeof options === "string" ? options : options?.accessToken || null;
  const customerId =
    typeof options === "object" ? options?.customerId || null : null;

  try {
    // If we already have a fresh token (e.g., right after OAuth exchange), use it directly.
    if (providedAccessToken) {
      const { data } = await retryWithExponentialBackoff(
        () => get("/connections", providedAccessToken),
        3,
        1000
      );
      return data;
    }

    // Otherwise, fall back to the latest unrevoked token for this customer (tenant-agnostic endpoint).
    if (!customerId) {
      throw new Error(
        "getConnections: customerId is required when no accessToken is provided"
      );
    }

    const { data } = await callXeroApiWithAutoRefresh(
      () =>
        withLatestAccessToken(customerId, null, (at) =>
          retryWithExponentialBackoff(() => get("/connections", at), 3, 1000)
        ),
      customerId,
      () => refreshAccessTokenFor(customerId, null)
    );
    return data;
  } catch (error) {
    throw error;
  }
}

/**
 * Fetch all Xero data for a customer and ptrsId.
 * @param {Object} options - { customerId, ptrsId }
 * @returns {Promise<Object>} - { organisations, invoices, payments, contacts }
 */
async function getAllXeroData(options) {
  const { customerId, ptrsId } = options;
  try {
    // Fetch all records from each table filtered by customerId and ptrsId
    const [organisations, invoices, payments, contacts, bankTransactions] =
      await Promise.all([
        db.XeroOrganisation.findAll({ where: { customerId, ptrsId } }),
        db.XeroInvoice.findAll({ where: { customerId, ptrsId } }),
        db.XeroPayment.findAll({ where: { customerId, ptrsId } }),
        db.XeroContact.findAll({ where: { customerId, ptrsId } }),
        db.XeroBankTxn.findAll({ where: { customerId, ptrsId } }),
      ]);
    return { organisations, invoices, payments, contacts, bankTransactions };
  } catch (error) {
    throw error;
  }
}

/**
 * Sync all tenants with staggered delays to avoid Xero rate limits.
 * This version is a placeholder: the actual tenant-level delay, error handling, and WebSocket updates
 * should be implemented directly in the controller loop for better progress ptrsing.
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
      return null;
    } catch (error) {
      const details = extractErrorDetails(error);
      if (
        details?.Status === 403 &&
        details?.Detail === "AuthenticationUnsuccessful"
      ) {
        return null;
      }
      throw error;
    }
  } catch (error) {
    throw error;
  }
}

// Save a Xero token for a tenant/customer

/**
 * Save or update a Xero token for a tenant.
 * @param {Object} param0
 */
async function saveToken(options) {
  const {
    accessToken,
    refreshToken,
    expiresIn,
    customerId,
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
    customerId,
    tenantId,
  };
  await XeroToken.upsert(updateData);
  return null;
}

/**
 * Retrieve the most recent (unrevoked) token for a given customerId.
 * @param {string} customerId
 * @returns {Promise<Object|null>}
 */
async function getLatestToken(options) {
  const { customerId, tenantId } = options;
  const token = await XeroToken.findOne({
    where: { customerId, tenantId, revoked: null },
    order: [["created", "DESC"]],
  });
  return token;
}

async function withLatestAccessToken(customerId, tenantId, fn) {
  let tokenRecord;
  if (tenantId) {
    tokenRecord = await getLatestToken({ customerId, tenantId });
  } else {
    tokenRecord = await XeroToken.findOne({
      where: { customerId, revoked: null },
      order: [["created", "DESC"]],
    });
  }
  if (!tokenRecord) {
    throw new Error(
      `No access token found for ${tenantId ? `tenant ${tenantId}` : `customer ${customerId}`}`
    );
  }
  const at = tokenRecord.access_token || tokenRecord.accessToken || "";
  return fn(at);
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
 * @param {Object} options - { accessToken, tenantId, customerId, ptrsId, startDate, endDate, createdBy }
 */
async function fetchBankTransactions(options) {
  const {
    accessToken,
    tenantId,
    customerId,
    ptrsId,
    startDate,
    endDate,
    createdBy,
    onProgress = () => {},
  } = options;
  try {
    console.log("[fetchBankTransactions] Starting bank transactions fetch...");
    let fetchedAll = false;
    let page = 1;
    const allBankTxns = [];
    while (!fetchedAll) {
      console.log(`[fetchBankTransactions] Fetching page ${page}`);
      onProgress({ stage: "fetchBankTransactions", page });
      const whereClause = buildWhereClauseDateRange(
        startDate,
        endDate,
        `Type == "SPEND"`
      );
      const { data } = await callXeroApiWithAutoRefresh(
        () =>
          withLatestAccessToken(customerId, tenantId, (at) =>
            retryWithExponentialBackoff(
              () =>
                get("/BankTransactions", at, tenantId, {
                  params: { where: whereClause, page },
                }),
              3,
              1000
            )
          ),
        customerId,
        () => refreshAccessTokenFor(customerId, tenantId)
      );
      const pageItems = data?.BankTransactions || [];
      console.log(
        `[fetchBankTransactions] Page ${page} received ${pageItems.length} transactions`
      );
      for (const txn of pageItems) {
        allBankTxns.push(txn);
        await db.XeroBankTxn.upsert({
          ...txn,
          Date: parseXeroDate(txn.Date, txn.DateString),
          customerId,
          ptrsId,
          tenantId,
          Url: trimStringIfTooLong(txn.Url || null),
          Reference: trimStringIfTooLong(txn.Reference || null),
          ...nowTimestamps(createdBy),
        });
      }
      fetchedAll = pageItems.length < 100;
      page++;
      console.log(
        `[fetchBankTransactions] Total bank transactions accumulated so far: ${allBankTxns.length}`
      );
    }
    console.log(
      `[fetchBankTransactions] Finished. Total bank transactions fetched: ${allBankTxns.length}`
    );
    return allBankTxns;
  } catch (error) {
    console.error("[fetchBankTransactions] Error:", error);
    throw error;
  }
}

/**
 * Starts the full extraction process from Xero, orchestrating all sub-fetches and ptrsing progress.
 * @param {Object} options - { accessToken, tenantId, customerId, ptrsId, startDate, endDate, createdBy, onProgress }
 */
async function startXeroExtraction(options) {
  const {
    accessToken,
    tenantId,
    customerId,
    ptrsId,
    startDate,
    endDate,
    createdBy,
    onProgress = () => {},
  } = options;

  console.log("[startXeroExtraction] Starting full Xero extraction process...");
  resetXeroProgress();

  // Fetch organisation details
  console.log("[startXeroExtraction] Fetching organisation details...");
  await fetchOrganisationDetails({
    accessToken,
    tenantId,
    customerId,
    ptrsId,
    createdBy,
    onProgress,
  });
  console.log("[startXeroExtraction] Finished fetching organisation details.");

  // Fetch payments
  console.log("[startXeroExtraction] Fetching payments...");
  const payments = await fetchPayments({
    accessToken,
    tenantId,
    customerId,
    ptrsId,
    startDate,
    endDate,
    createdBy,
    onProgress,
  });
  console.log(`[startXeroExtraction] Fetched ${payments.length} payments.`);

  // Fetch invoices
  console.log("[startXeroExtraction] Fetching invoices...");
  await fetchInvoices({
    accessToken,
    tenantId,
    customerId,
    ptrsId,
    payments,
    createdBy,
    onProgress,
  });
  console.log("[startXeroExtraction] Finished fetching invoices.");

  // Fetch bank transactions
  console.log("[startXeroExtraction] Fetching bank transactions...");
  await fetchBankTransactions({
    accessToken,
    tenantId,
    customerId,
    ptrsId,
    startDate,
    endDate,
    createdBy,
    onProgress,
  });
  console.log("[startXeroExtraction] Finished fetching bank transactions.");

  // Fetch contacts
  console.log("[startXeroExtraction] Fetching contacts...");
  await fetchContacts({
    accessToken,
    tenantId,
    customerId,
    ptrsId,
    payments,
    createdBy,
    onProgress,
  });
  console.log("[startXeroExtraction] Finished fetching contacts.");

  console.log("[startXeroExtraction] Xero extraction completed.");
  return { message: "Xero extraction completed." };
}

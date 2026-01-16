const axios = require("axios");
const { Op } = require("sequelize");

const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const xeroApi = require("./xeroApi");
const {
  paginateXeroApi,
  callXeroApiWithAutoRefresh,
  extractErrorDetails,
} = require("./xeroApiUtils");

const XERO_TOKEN_URL =
  process.env.XERO_TOKEN_URL || "https://identity.xero.com/connect/token";
const XERO_AUTHORIZE_URL =
  process.env.XERO_AUTHORIZE_URL ||
  "https://login.xero.com/identity/connect/authorize";

module.exports = {
  buildAuthUrl,
  exchangeAuthCodeForToken,
  listConnections,
  computeExpiresFromToken,
  getDefaultTenantForCustomer,
  getReportingPeriodForPtrs,
  getValidAccessTokenForTenant,
  listContactsForTenant,
  listPaymentsForTenantAndPeriod,
  listApInvoicesForTenantAndPeriod,
};

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} is required`);
  return v;
}

/**
 * Build the Xero OAuth2 authorisation URL for redirecting the user.
 */
function buildAuthUrl({ state, redirectUri }) {
  const clientId =
    process.env.XERO_CLIENT_ID ||
    process.env.XERO_OAUTH_CLIENT_ID ||
    process.env.XERO_CLIENTID ||
    null;

  if (!clientId) {
    throw new Error("Xero client ID missing (XERO_CLIENT_ID)");
  }
  if (!redirectUri) {
    throw new Error("redirectUri is required");
  }

  const scope =
    process.env.XERO_SCOPES ||
    "offline_access accounting.contacts accounting.transactions accounting.settings";

  const params = new URLSearchParams();
  params.set("response_type", "code");
  params.set("client_id", clientId);
  params.set("redirect_uri", redirectUri);
  params.set("scope", scope);
  if (state) params.set("state", state);

  // Force consent so we reliably get refresh_token during dev/test.
  params.set("prompt", "consent");

  return `${XERO_AUTHORIZE_URL}?${params.toString()}`;
}

/**
 * Exchange an auth code for tokens.
 */
async function exchangeAuthCodeForToken({ code, redirectUri }) {
  if (!code) throw new Error("code is required");

  const clientId =
    process.env.XERO_CLIENT_ID ||
    process.env.XERO_OAUTH_CLIENT_ID ||
    process.env.XERO_CLIENTID ||
    null;

  const clientSecret =
    process.env.XERO_CLIENT_SECRET ||
    process.env.XERO_OAUTH_CLIENT_SECRET ||
    process.env.XERO_CLIENTSECRET ||
    null;

  if (!clientId || !clientSecret) {
    throw new Error(
      "Xero client credentials missing (XERO_CLIENT_ID / XERO_CLIENT_SECRET)"
    );
  }

  const ru = redirectUri || requireEnv("XERO_REDIRECT_URI");

  const params = new URLSearchParams();
  params.set("grant_type", "authorization_code");
  params.set("code", code);
  params.set("redirect_uri", ru);

  const res = await axios.post(XERO_TOKEN_URL, params.toString(), {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    auth: { username: clientId, password: clientSecret },
    timeout: 30000,
  });

  return res.data || {};
}

/**
 * List Xero connections (tenants) available for the authorised user.
 * Uses the special Connections endpoint on api.xero.com.
 */
async function listConnections({ accessToken }) {
  if (!accessToken) throw new Error("accessToken is required");

  const res = await axios.get("https://api.xero.com/connections", {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/json",
    },
    timeout: 30000,
  });

  const data = Array.isArray(res.data) ? res.data : [];
  return data.map((c) => ({
    tenantId: c.tenantId,
    tenantName: c.tenantName || c.tenantId,
  }));
}

/**
 * Compute token expiry timestamp from an OAuth token response.
 */
function computeExpiresFromToken(token) {
  const expiresIn = Number(token?.expires_in || 0);
  return new Date(Date.now() + Math.max(expiresIn, 0) * 1000);
}

async function getDefaultTenantForCustomer(customerId) {
  const XeroToken = getXeroTokenModel();

  const token = await XeroToken.findOne({
    where: {
      customerId,
      revoked: null,
    },
    order: [["created", "DESC"]],
  });

  if (!token?.tenantId) {
    throw new Error("No active Xero tenant linked to this customer");
  }

  return token.tenantId;
}

async function getReportingPeriodForPtrs({ customerId, ptrsId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!ptrsId) throw new Error("ptrsId is required");

  const Ptrs = db.Ptrs;
  if (!Ptrs) throw new Error("Ptrs model not loaded (db.Ptrs missing)");

  const run = await Ptrs.findOne({
    where: {
      id: ptrsId,
      customerId,
      deletedAt: { [Op.is]: null },
    },
  });

  if (!run) throw new Error("PTRS run not found");
  if (!run.periodStart || !run.periodEnd) {
    throw new Error(
      "PTRS reporting period is missing (periodStart/periodEnd). Set the reporting period before importing from Xero."
    );
  }

  return {
    periodStart: run.periodStart,
    periodEnd: run.periodEnd,
  };
}

/**
 * Returns a valid access token for the given customer + tenant.
 * Refreshes and persists a new token row if expired (revokes old row).
 */
async function getValidAccessTokenForTenant({ customerId, tenantId }) {
  const XeroToken = getXeroTokenModel();

  const token = await XeroToken.findOne({
    where: {
      customerId,
      tenantId,
      revoked: null,
    },
    order: [["created", "DESC"]],
  });

  if (!token) {
    throw new Error("No active Xero token found for this customer + tenant");
  }

  const isExpired = Date.now() >= new Date(token.expires).getTime();
  if (!isExpired) return token.access_token;

  const refreshed = await refreshAccessToken({
    refreshToken: token.refresh_token,
  });

  // Revoke old token row
  await XeroToken.update(
    {
      revoked: new Date(),
      revokedByIp: "system",
      replacedByToken: refreshed?.access_token || null,
    },
    { where: { id: token.id } }
  );

  // Persist new token row (keep tenant/customer binding)
  await XeroToken.create({
    access_token: refreshed.access_token,
    refresh_token: refreshed.refresh_token,
    scope: refreshed.scope || token.scope,
    expires: refreshed.expires,
    created: new Date(),
    createdByIp: "system",
    revoked: null,
    revokedByIp: null,
    replacedByToken: null,
    customerId,
    tenantId,
  });

  return refreshed.access_token;
}

async function listContactsForTenant({ customerId, tenantId }) {
  const out = [];
  await paginateXeroCollection({
    label: "Contacts",
    fetchPage: async (page) => {
      const accessToken = await getValidAccessTokenForTenant({
        customerId,
        tenantId,
      });
      return callXeroApiWithAutoRefresh(
        () =>
          xeroApi.get(
            `/api.xro/2.0/Contacts?page=${page}`,
            accessToken,
            tenantId
          ),
        customerId,
        () => refreshAndSwapAccessToken({ customerId, tenantId })
      );
    },
    extractItems: (payload) => payload?.Contacts || [],
    onItems: (items) => out.push(...items),
    pageSize: 100,
  });

  return out;
}

async function listPaymentsForTenantAndPeriod({
  customerId,
  tenantId,
  periodStart,
  periodEnd,
}) {
  const where = buildPaymentsWhere(periodStart, periodEnd);

  const out = [];
  await paginateXeroCollection({
    label: "Payments",
    fetchPage: async (page) => {
      const accessToken = await getValidAccessTokenForTenant({
        customerId,
        tenantId,
      });
      return callXeroApiWithAutoRefresh(
        () =>
          xeroApi.get(
            `/api.xro/2.0/Payments?page=${page}&where=${encodeURIComponent(where)}`,
            accessToken,
            tenantId
          ),
        customerId,
        () => refreshAndSwapAccessToken({ customerId, tenantId })
      );
    },
    extractItems: (payload) => payload?.Payments || [],
    onItems: (items) => out.push(...items),
    pageSize: 100,
  });

  return out;
}

/**
 * Optional: AP invoices/bills. Not required for payment-driven PTRS import MVP,
 * but useful later to enrich payment rows.
 */
async function listApInvoicesForTenantAndPeriod({
  customerId,
  tenantId,
  periodStart,
  periodEnd,
}) {
  const where = buildApInvoicesWhere(periodStart, periodEnd);

  const out = [];
  await paginateXeroCollection({
    label: "Invoices",
    fetchPage: async (page) => {
      const accessToken = await getValidAccessTokenForTenant({
        customerId,
        tenantId,
      });
      return callXeroApiWithAutoRefresh(
        () =>
          xeroApi.get(
            `/api.xro/2.0/Invoices?page=${page}&where=${encodeURIComponent(where)}`,
            accessToken,
            tenantId
          ),
        customerId,
        () => refreshAndSwapAccessToken({ customerId, tenantId })
      );
    },
    extractItems: (payload) => payload?.Invoices || [],
    onItems: (items) => out.push(...items),
    pageSize: 100,
  });

  return out;
}

// -----------------------------------------------------------------------------
// Internals
// -----------------------------------------------------------------------------

function getXeroTokenModel() {
  const model = db.XeroToken || db.models?.XeroToken;
  if (!model)
    throw new Error("XeroToken model not loaded (db.XeroToken missing)");
  return model;
}

async function refreshAndSwapAccessToken({ customerId, tenantId }) {
  try {
    await getValidAccessTokenForTenant({ customerId, tenantId });
  } catch (e) {
    logger?.error?.("Failed to refresh Xero token", {
      action: "XeroTokenRefresh",
      customerId,
      tenantId,
      error: e?.message,
    });
    throw e;
  }
}

async function paginateXeroCollection({
  label,
  fetchPage,
  extractItems,
  onItems,
  pageSize = 100,
}) {
  await paginateXeroApi(
    async (page) => fetchPage(page),
    async (response, page) => {
      const items = extractItems(response?.data) || [];
      if (items.length && typeof onItems === "function") onItems(items, page);
      if (global.sendWebSocketUpdate) {
        global.sendWebSocketUpdate({
          status: "info",
          message: `[Xero API] ${label}: page ${page} (${items.length} items)`,
          timestamp: new Date().toISOString(),
        });
      }
    },
    {
      pageSize,
      hasMoreFn: (res) => {
        const items = extractItems(res?.data) || [];
        return Array.isArray(items) && items.length === pageSize;
      },
    }
  );
}

async function refreshAccessToken({ refreshToken }) {
  const clientId =
    process.env.XERO_CLIENT_ID ||
    process.env.XERO_OAUTH_CLIENT_ID ||
    process.env.XERO_CLIENTID ||
    null;
  const clientSecret =
    process.env.XERO_CLIENT_SECRET ||
    process.env.XERO_OAUTH_CLIENT_SECRET ||
    process.env.XERO_CLIENTSECRET ||
    null;

  if (!clientId || !clientSecret) {
    throw new Error(
      "Xero client credentials missing (XERO_CLIENT_ID / XERO_CLIENT_SECRET)"
    );
  }

  const params = new URLSearchParams();
  params.set("grant_type", "refresh_token");
  params.set("refresh_token", refreshToken);

  try {
    const res = await axios.post(XERO_TOKEN_URL, params.toString(), {
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      auth: { username: clientId, password: clientSecret },
      timeout: 30000,
    });

    const data = res.data || {};
    const expiresIn = Number(data.expires_in || 0);
    const expires = new Date(Date.now() + Math.max(expiresIn, 0) * 1000);

    return {
      access_token: data.access_token,
      refresh_token: data.refresh_token,
      scope: data.scope || "",
      expires,
    };
  } catch (err) {
    const details = extractErrorDetails(err);
    throw new Error(`Failed to refresh Xero token: ${details}`);
  }
}

function buildDateTimeExpr(dateOnly) {
  const d = new Date(dateOnly);
  if (Number.isNaN(d.getTime())) throw new Error("Invalid date");
  const y = d.getUTCFullYear();
  const m = d.getUTCMonth() + 1;
  const day = d.getUTCDate();
  return `DateTime(${y},${m},${day})`;
}

function buildPaymentsWhere(periodStart, periodEnd) {
  const start = buildDateTimeExpr(periodStart);
  const end = buildDateTimeExpr(periodEnd);
  return `Date>=${start}&&Date<=${end}`;
}

function buildApInvoicesWhere(periodStart, periodEnd) {
  const start = buildDateTimeExpr(periodStart);
  const end = buildDateTimeExpr(periodEnd);
  return `Type=="ACCPAY"&&Date>=${start}&&Date<=${end}`;
}

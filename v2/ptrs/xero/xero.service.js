const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const fs = require("fs");
const path = require("path");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

const xeroClient = require("@/v2/core/xero/xeroClient.service");
const xeroApi = require("@/v2/core/xero/xeroApi");
const {
  paginateXeroApi,
  callXeroApiWithAutoRefresh,
} = require("@/v2/core/xero/xeroApiUtils");

const statusStore = new Map();
const selectionStore = new Map();

function getXeroLogSlowMs() {
  const raw = process.env.XERO_LOG_SLOW_MS;
  if (raw === undefined || raw === null || raw === "") return 2000;
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 ? Math.floor(n) : 2000;
}

function shouldWriteSlowCallsTextLog() {
  const raw = process.env.XERO_SLOW_CALLS_TEXT_LOG;
  if (raw === undefined || raw === null || raw === "") {
    // Default ON in non-production to make diagnosing slow Xero calls easier.
    return process.env.NODE_ENV !== "production";
  }
  return ["1", "true", "yes", "y", "on"].includes(String(raw).toLowerCase());
}

function ensureLogsDir() {
  try {
    const dir = path.join(process.cwd(), "logs");
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    return dir;
  } catch (_) {
    return null;
  }
}

function getSlowCallsTextLogPath() {
  const dir = ensureLogsDir();
  if (!dir) return null;
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return path.join(
    dir,
    `xero-slow-calls/xero-slow-calls-${yyyy}-${mm}-${dd}.log`,
  );
}

async function appendSlowCallTextLine(line) {
  try {
    const filePath = getSlowCallsTextLogPath();
    if (!filePath) return;
    await fs.promises.appendFile(filePath, line, "utf8");
  } catch (_) {
    // Never let file logging break the import runner.
  }
}

function logSlowXeroCall({
  customerId,
  ptrsId,
  tenantId,
  phase,
  tookMs,
  extra,
}) {
  const slowMs = getXeroLogSlowMs();
  if (tookMs < slowMs) return;

  logger?.warn?.("Slow Xero API call", {
    action: "PtrsV2XeroSlowCall",
    customerId,
    ptrsId,
    tenantId,
    phase,
    tookMs,
    ...extra,
  });

  if (shouldWriteSlowCallsTextLog()) {
    const ts = new Date().toISOString();
    const safeExtra = (() => {
      try {
        return extra ? JSON.stringify(extra) : "";
      } catch {
        return "";
      }
    })();

    // TSV-ish so you can grep/sort easily.
    // Example: 2026-02-07T05:16:14.000Z\tfetchInvoiceById\t3875ms\tcustomer=...\tptrs=...\ttenant=...\t{"invoiceId":"...","url":"..."}
    const line = `${ts}\t${phase}\t${tookMs}ms\tcustomer=${customerId || ""}\tptrs=${ptrsId || ""}\ttenant=${tenantId || ""}\t${safeExtra}\n`;
    void appendSlowCallTextLine(line);
  }
}

function phaseTimer({ customerId, ptrsId, tenantId, phase, extra = {} }) {
  const started = Date.now();
  return {
    end: (result = "ok", more = {}, level = "info") => {
      const tookMs = Date.now() - started;
      logger?.[level]?.("PTRS Xero phase timing", {
        action: "PtrsV2XeroPhaseTiming",
        customerId,
        ptrsId,
        tenantId,
        phase,
        result,
        tookMs,
        ...extra,
        ...more,
      });
      return tookMs;
    },
  };
}

function getXeroExtractLimit() {
  const raw = process.env.XERO_EXTRACT_LIMIT;

  if (raw === undefined || raw === null || raw === "") return null;

  const n = Number(raw);

  if (!Number.isFinite(n) || n <= 0) {
    throw new Error("XERO_EXTRACT_LIMIT must be a positive number if set");
  }

  return Math.floor(n);
}

function applyExtractLimit(items, limit, state) {
  // `state` is a small mutable counter object e.g. { count: 0 }
  if (!limit) return { items, done: false };

  const remaining = Math.max(limit - (state?.count || 0), 0);
  const sliced = remaining > 0 ? items.slice(0, remaining) : [];

  if (state) state.count = (state.count || 0) + sliced.length;

  const done = state?.count >= limit;

  return { items: sliced, done };
}

function getSelectedTenantIds(customerId, ptrsId) {
  const key = statusKey(customerId, ptrsId);
  const selected = selectionStore.get(key);
  return Array.isArray(selected) ? selected.filter(Boolean) : [];
}

function setSelectedTenantIds(customerId, ptrsId, tenantIds) {
  const key = statusKey(customerId, ptrsId);
  selectionStore.set(
    key,
    Array.isArray(tenantIds) ? tenantIds.filter(Boolean) : [],
  );
}

function updateStatus(customerId, ptrsId, patch) {
  const key = statusKey(customerId, ptrsId);
  const current = statusStore.get(key) || {
    ptrsId,
    status: "NOT_STARTED",
    message: null,
    progress: null,
    updatedAt: new Date().toISOString(),
  };

  const next = {
    ...current,
    ...patch,
    progress: {
      ...(current.progress || {}),
      ...(patch.progress || {}),
    },
    updatedAt: new Date().toISOString(),
  };

  statusStore.set(key, next);

  // Socket push (MVP): broadcast status updates so the FE can react without polling.
  // Room convention (server.js): `ptrs:<ptrsId>`
  try {
    const io = global.__socketio;
    if (io && ptrsId) {
      io.to(`ptrs:${ptrsId}`).emit("ptrs:xeroImportStatus", next);
    }
  } catch (_) {
    // Never let websocket issues break the import runner.
  }

  return next;
}

function getHttpErrorMeta(err) {
  const statusCode =
    err?.statusCode ||
    err?.response?.status ||
    err?.response?.statusCode ||
    null;

  const cfg = err?.config || err?.response?.config || null;
  const method = cfg?.method ? String(cfg.method).toUpperCase() : null;
  const url = cfg?.url ? String(cfg.url) : null;

  // Don’t log tokens/headers.
  const responseBody = err?.response?.data ?? err?.data ?? null;

  return { statusCode, method, url, responseBody };
}

function base64(str) {
  return Buffer.from(String(str || ""), "utf8").toString("base64");
}

async function refreshAccessTokenIfNeeded({ customerId, tenantId, tokenRow }) {
  // tokenRow is the active (revoked=null) token row for this tenant/customer.
  // If it isn’t expired yet, return it as-is.
  const now = Date.now();
  const expiresMs = tokenRow?.expires
    ? new Date(tokenRow.expires).getTime()
    : null;

  // If we don't have an expiry stored, assume it's valid for now (MVP).
  if (!expiresMs || Number.isNaN(expiresMs)) return tokenRow;

  // Refresh 60s before expiry to avoid edge timing.
  if (expiresMs - now > 60_000) return tokenRow;

  const clientId = requireEnv("XERO_CLIENT_ID");
  const clientSecret = requireEnv("XERO_CLIENT_SECRET");

  const url = "https://identity.xero.com/connect/token";
  const form = new URLSearchParams();
  form.set("grant_type", "refresh_token");
  form.set("refresh_token", tokenRow.refresh_token);

  const { data } = await xeroApi.post(url, form.toString(), null, null, {
    headers: {
      Authorization: `Basic ${base64(`${clientId}:${clientSecret}`)}`,
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/json",
    },
  });

  if (!data?.access_token || !data?.refresh_token) {
    throw new Error("Failed to refresh Xero access token");
  }

  const XeroToken = db.XeroToken || db.models?.XeroToken;
  if (!XeroToken) {
    throw new Error("XeroToken model not loaded (db.XeroToken missing)");
  }

  const expires = xeroClient.computeExpiresFromToken(data);

  // IMPORTANT (multi-tenant): Xero refresh tokens are rotated.
  // If we store the same refresh token per-tenant, then refreshing for tenant A invalidates
  // the refresh token stored for tenants B..N.
  // MVP fix: when we refresh once, rotate *all* active tenant token rows for this customer
  // to the newly-issued refresh token.
  await withCustomerTxn(customerId, async (t) => {
    const activeRows = await XeroToken.findAll({
      where: { customerId, revoked: null },
      transaction: t,
    });

    const tenantIds = Array.from(
      new Set((activeRows || []).map((r) => r?.tenantId).filter(Boolean)),
    );

    // Revoke all active token rows for this customer (across tenants)
    await XeroToken.update(
      { revoked: new Date(), revokedByIp: "system", replacedByToken: null },
      {
        where: { customerId, revoked: null },
        transaction: t,
      },
    );

    // Create a fresh active token row per tenant using the new refresh token
    for (const tid of tenantIds) {
      const prev = (activeRows || []).find((r) => r?.tenantId === tid) || null;

      await XeroToken.create(
        {
          access_token: data.access_token,
          refresh_token: data.refresh_token,
          scope: data.scope || prev?.scope || tokenRow.scope || "",
          expires,
          created: new Date(),
          createdByIp: "system",
          revoked: null,
          revokedByIp: null,
          replacedByToken: null,
          customerId,
          tenantId: tid,
          ...(prev?.tenantName ? { tenantName: prev.tenantName } : {}),
        },
        { transaction: t },
      );
    }
  });

  // Return a tokenRow-shaped object to the caller.
  return {
    ...tokenRow,
    access_token: data.access_token,
    refresh_token: data.refresh_token,
    expires,
  };
}

async function fetchPaymentsPage({
  customerId,
  tokenRow,
  tenantId,
  periodStart,
  periodEnd,
  page = 1,
}) {
  if (!customerId) throw new Error("Missing customerId");
  if (!tenantId) throw new Error("Missing Xero tenantId");
  if (!tokenRow?.access_token) throw new Error("Missing Xero access token");

  let currentToken = tokenRow;

  const where = buildXeroWhereDateRange("Date", periodStart, periodEnd);

  const url = `https://api.xero.com/api.xro/2.0/Payments?page=${encodeURIComponent(
    page,
  )}${where ? `&where=${encodeURIComponent(where)}` : ""}`;

  const started = Date.now();
  const { data, headers, status } = await callXeroApiWithAutoRefresh(
    () => xeroApi.get(url, currentToken.access_token, tenantId),
    customerId,
    async () => {
      currentToken = await refreshAccessTokenIfNeeded({
        customerId,
        tenantId,
        tokenRow: currentToken,
      });
    },
  );

  const tookMs = Date.now() - started;

  logSlowXeroCall({
    customerId,
    ptrsId: null,
    tenantId,
    phase: "fetchPaymentsPage",
    tookMs,
    extra: {
      page,
      url,
      statusCode: status ?? null,
      xeroCorrelationId: headers?.["xero-correlation-id"] || null,
    },
  });

  // Xero returns { Payments: [...] }
  return {
    items: Array.isArray(data?.Payments) ? data.Payments : [],
    tokenRow: currentToken,
  };
}

async function fetchInvoiceById({ customerId, tokenRow, tenantId, invoiceId }) {
  if (!customerId) throw new Error("Missing customerId");
  if (!tenantId) throw new Error("Missing Xero tenantId");
  if (!invoiceId) throw new Error("Missing invoiceId");
  if (!tokenRow?.access_token) throw new Error("Missing Xero access token");

  let currentToken = tokenRow;

  const url = `https://api.xero.com/api.xro/2.0/Invoices/${encodeURIComponent(
    invoiceId,
  )}`;

  const started = Date.now();
  const { data, headers, status } = await callXeroApiWithAutoRefresh(
    () => xeroApi.get(url, currentToken.access_token, tenantId),
    customerId,
    async () => {
      currentToken = await refreshAccessTokenIfNeeded({
        customerId,
        tenantId,
        tokenRow: currentToken,
      });
    },
  );

  const tookMs = Date.now() - started;

  logSlowXeroCall({
    customerId,
    ptrsId: null,
    tenantId,
    phase: "fetchInvoiceById",
    tookMs,
    extra: {
      invoiceId,
      url,
      statusCode: status ?? null,
      xeroCorrelationId: headers?.["xero-correlation-id"] || null,
    },
  });

  const invoices = Array.isArray(data?.Invoices) ? data.Invoices : [];
  return { item: invoices[0] || null, tokenRow: currentToken };
}

async function fetchContactById({ customerId, tokenRow, tenantId, contactId }) {
  if (!customerId) throw new Error("Missing customerId");
  if (!tenantId) throw new Error("Missing Xero tenantId");
  if (!contactId) throw new Error("Missing contactId");
  if (!tokenRow?.access_token) throw new Error("Missing Xero access token");

  let currentToken = tokenRow;

  const url = `https://api.xero.com/api.xro/2.0/Contacts/${encodeURIComponent(
    contactId,
  )}`;

  const started = Date.now();
  const { data, headers, status } = await callXeroApiWithAutoRefresh(
    () => xeroApi.get(url, currentToken.access_token, tenantId),
    customerId,
    async () => {
      currentToken = await refreshAccessTokenIfNeeded({
        customerId,
        tenantId,
        tokenRow: currentToken,
      });
    },
  );

  const tookMs = Date.now() - started;

  logSlowXeroCall({
    customerId,
    ptrsId: null,
    tenantId,
    phase: "fetchContactById",
    tookMs,
    extra: {
      contactId,
      url,
      statusCode: status ?? null,
      xeroCorrelationId: headers?.["xero-correlation-id"] || null,
    },
  });

  const contacts = Array.isArray(data?.Contacts) ? data.Contacts : [];
  return { item: contacts[0] || null, tokenRow: currentToken };
}

async function fetchBankTransactionsPage({
  customerId,
  tokenRow,
  tenantId,
  periodStart,
  periodEnd,
  page = 1,
}) {
  if (!customerId) throw new Error("Missing customerId");
  if (!tenantId) throw new Error("Missing Xero tenantId");
  if (!tokenRow?.access_token) throw new Error("Missing Xero access token");

  let currentToken = tokenRow;

  const where = buildXeroWhereDateRange("Date", periodStart, periodEnd);

  const url = `https://api.xero.com/api.xro/2.0/BankTransactions?page=${encodeURIComponent(
    page,
  )}${where ? `&where=${encodeURIComponent(where)}` : ""}`;

  const started = Date.now();
  const { data, headers, status } = await callXeroApiWithAutoRefresh(
    () => xeroApi.get(url, currentToken.access_token, tenantId),
    customerId,
    async () => {
      currentToken = await refreshAccessTokenIfNeeded({
        customerId,
        tenantId,
        tokenRow: currentToken,
      });
    },
  );

  const tookMs = Date.now() - started;

  logSlowXeroCall({
    customerId,
    ptrsId: null,
    tenantId,
    phase: "fetchBankTransactionsPage",
    tookMs,
    extra: {
      page,
      url,
      statusCode: status ?? null,
      xeroCorrelationId: headers?.["xero-correlation-id"] || null,
    },
  });

  return {
    items: Array.isArray(data?.BankTransactions) ? data.BankTransactions : [],
    tokenRow: currentToken,
  };
}

async function fetchOrganisationDetails({ customerId, tokenRow, tenantId }) {
  if (!customerId) throw new Error("Missing customerId");
  if (!tenantId) throw new Error("Missing Xero tenantId");
  if (!tokenRow?.access_token) throw new Error("Missing Xero access token");

  let currentToken = tokenRow;

  const url = "https://api.xero.com/api.xro/2.0/Organisation";

  const started = Date.now();
  const { data, headers, status } = await callXeroApiWithAutoRefresh(
    () => xeroApi.get(url, currentToken.access_token, tenantId),
    customerId,
    async () => {
      currentToken = await refreshAccessTokenIfNeeded({
        customerId,
        tenantId,
        tokenRow: currentToken,
      });
    },
  );

  const tookMs = Date.now() - started;

  logSlowXeroCall({
    customerId,
    ptrsId: null,
    tenantId,
    phase: "fetchOrganisationDetails",
    tookMs,
    extra: {
      url,
      statusCode: status ?? null,
      xeroCorrelationId: headers?.["xero-correlation-id"] || null,
    },
  });

  const orgs = Array.isArray(data?.Organisations) ? data.Organisations : [];

  return {
    item: orgs[0] || null,
    tokenRow: currentToken,
  };
}

module.exports = {
  connect,
  handleCallback,
  getOrganisations,
  selectOrganisations,
  removeOrganisation,
  startImport,
  getStatus,
  getImportExceptions,
  getImportExceptionsSummary,
  getImportExceptionsCsv,
};
// ------------------------
// Import Exceptions (PTRS Import Errors)
// ------------------------

async function getImportExceptionsSummary({ customerId, ptrsId }) {
  const PtrsImportException = getModel("PtrsImportException");
  if (!PtrsImportException) {
    throw new Error("PtrsImportException model not loaded");
  }

  // Respect soft delete if present.
  const where = {
    customerId,
    ptrsId,
    ...(PtrsImportException.rawAttributes?.deletedAt
      ? { deletedAt: null }
      : {}),
  };

  return await withCustomerTxn(customerId, async (t) => {
    return await PtrsImportException.count({ where, transaction: t });
  });
}

async function getImportExceptions({ customerId, ptrsId }) {
  const PtrsImportException = getModel("PtrsImportException");
  if (!PtrsImportException) {
    throw new Error("PtrsImportException model not loaded");
  }

  return await withCustomerTxn(customerId, async (t) => {
    return await PtrsImportException.findAll({
      where: { customerId, ptrsId },
      order: [["createdAt", "ASC"]],
      transaction: t,
    });
  });
}

async function getImportExceptionsCsv({ customerId, ptrsId }) {
  const rows = await getImportExceptions({ customerId, ptrsId });

  const headers = [
    "occurredAt",
    "source",
    "phase",
    "severity",
    "statusCode",
    "xeroTenantId",
    "invoiceId",
    "method",
    "url",
    "message",
  ];

  const escape = (v) => {
    if (v === null || v === undefined) return "";
    const s = String(v).replace(/"/g, '""');
    return `"${s}"`;
  };

  const lines = [headers.join(",")];

  for (const r of rows) {
    const j = typeof r.toJSON === "function" ? r.toJSON() : r;
    lines.push(
      headers
        .map((h) => {
          if (h === "occurredAt") return escape(j.createdAt);
          return escape(j[h]);
        })
        .join(","),
    );
  }

  return lines.join("\n");
}

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} is required`);
  return v;
}

function statusKey(customerId, ptrsId) {
  return `${customerId}::${ptrsId}`;
}

async function withCustomerTxn(customerId, work) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const out = await work(t);
    await t.commit();
    return out;
  } catch (err) {
    try {
      if (!t.finished) await t.rollback();
    } catch (_) {}
    throw err;
  }
}

function getModel(name) {
  return db[name] || db.models?.[name] || null;
}

function pickModelFields(model, payload) {
  if (!model || !payload || typeof payload !== "object") return {};
  const attrs = model.rawAttributes || {};
  const out = {};
  for (const [k, v] of Object.entries(payload)) {
    if (k in attrs) out[k] = v;
  }
  return out;
}

// Helper to persist import exceptions, never throws
async function recordImportException({
  customerId,
  ptrsId,
  importRunId,
  source,
  phase,
  severity,
  statusCode,
  method,
  url,
  message,
  xeroTenantId,
  invoiceId,
  responseBody,
  meta,
}) {
  try {
    const PtrsImportException = getModel("PtrsImportException");
    if (!PtrsImportException) {
      logger?.warn?.(
        "PtrsImportException model not loaded; skipping exception persist",
        {
          action: "PtrsV2ImportExceptionModelMissing",
          customerId,
          ptrsId,
          source,
          phase,
        },
      );
      return null;
    }

    if (!customerId)
      throw new Error("recordImportException: customerId is required");
    if (!ptrsId) throw new Error("recordImportException: ptrsId is required");

    const row = pickModelFields(PtrsImportException, {
      customerId,
      ptrsId,
      importRunId: importRunId || ptrsId,
      source: source || null,
      phase: phase || null,
      severity: severity || "error",
      statusCode: Number.isFinite(Number(statusCode))
        ? Number(statusCode)
        : null,
      method: method ? String(method).toUpperCase() : null,
      url: url ? String(url) : null,
      message: message ? String(message) : null,
      xeroTenantId: xeroTenantId || null,
      invoiceId: invoiceId || null,
      responseBody: responseBody ?? null,
      meta: meta ?? null,
      deletedAt: null,
    });

    return await withCustomerTxn(customerId, async (t) => {
      return await PtrsImportException.create(row, { transaction: t });
    });
  } catch (err) {
    // Never let exception logging break the import runner.
    logger?.error?.("PTRS v2 import exception persist failed", {
      action: "PtrsV2ImportExceptionPersistFailed",
      customerId,
      ptrsId,
      error: err?.message,
      stack: err?.stack,
    });
    return null;
  }
}

function getFirstKey(obj, keys = []) {
  for (const k of keys) {
    const v = obj?.[k];
    if (v !== undefined && v !== null && v !== "") return v;
  }
  return null;
}

function parseXeroDotNetDate(value) {
  // Xero often returns dates like "/Date(1398902400000+0000)/"
  if (value == null) return null;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value;

  const s = String(value).trim();
  if (!s) return null;

  const m = /^\/Date\((\d+)(?:[+-]\d+)?\)\/$/.exec(s);
  if (m) {
    const ms = Number(m[1]);
    if (Number.isFinite(ms)) {
      const d = new Date(ms);
      return Number.isNaN(d.getTime()) ? null : d;
    }
  }

  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
}

function toIsoDateOnlyUtc(d) {
  if (!(d instanceof Date) || Number.isNaN(d.getTime())) return null;
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function safeJsonStringify(obj, fallback = null) {
  try {
    return JSON.stringify(obj);
  } catch {
    return fallback;
  }
}

function assertIsoDateOnly(value, fieldName) {
  if (value === undefined || value === null || value === "") return null;
  const s = String(value).trim();
  // Expect DATEONLY coming from Sequelize as YYYY-MM-DD
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    throw new Error(`${fieldName} must be a YYYY-MM-DD date (got: ${s})`);
  }
  return s;
}

function toXeroDateTimeExpr(isoDateOnly) {
  // Xero filter syntax uses DateTime(YYYY,MM,DD)
  const [y, m, d] = String(isoDateOnly)
    .split("-")
    .map((n) => Number(n));
  if (![y, m, d].every((n) => Number.isFinite(n))) return null;
  return `DateTime(${y},${m},${d})`;
}

function buildXeroWhereDateRange(fieldName, periodStart, periodEnd) {
  const start = assertIsoDateOnly(periodStart, "periodStart");
  const end = assertIsoDateOnly(periodEnd, "periodEnd");
  if (!start || !end) return null;

  const startExpr = toXeroDateTimeExpr(start);
  const endExpr = toXeroDateTimeExpr(end);
  if (!startExpr || !endExpr) return null;

  // Inclusive bounds to match common reporting-period expectations.
  // Note: fieldName must be a Xero date field (e.g. "Date").
  return `${fieldName} >= ${startExpr} && ${fieldName} <= ${endExpr}`;
}

function deriveXeroPaymentTermsConfig({
  rawInvoice,
  rawContact,
  rawOrganisation,
}) {
  // Priority (purchases): invoice -> contact -> organisation.
  const inv = rawInvoice?.PaymentTerms || null;
  const con = rawContact?.PaymentTerms || null;
  const org = rawOrganisation?.PaymentTerms || null;

  const pickPurchases = (pt) => pt?.Purchases || pt?.purchases || null;

  const purchases =
    pickPurchases(inv) || pickPurchases(con) || pickPurchases(org) || null;

  // Normalise common shape: { Day: 30, Type: "DAYSAFTERBILLDATE" }
  const dayRaw = purchases?.Day ?? purchases?.day ?? null;
  const typeRaw = purchases?.Type ?? purchases?.type ?? null;

  const days = Number(dayRaw);
  const hasDays = Number.isFinite(days);

  return {
    purchasesConfig: purchases || null,
    purchasesType: typeRaw != null ? String(typeRaw) : null,
    purchasesDay: hasDays ? Math.round(days) : null,
    raw: purchases || null,
  };
}

async function persistPayments({
  customerId,
  ptrsId,
  tenantId,
  payments,
  fetchedAt,
}) {
  const PtrsXeroPayment = getModel("PtrsXeroPayment");
  if (!PtrsXeroPayment) {
    // If the cache model isn't available, we still consider the fetch a success for MVP.
    return {
      inserted: 0,
      skipped: Array.isArray(payments) ? payments.length : 0,
      note: "PtrsXeroPayment model not found",
    };
  }

  const items = Array.isArray(payments) ? payments : [];

  // Guard required fields
  if (!customerId) throw new Error("persistPayments: customerId is required");
  if (!tenantId) throw new Error("persistPayments: tenantId is required");

  let inserted = 0;
  let skipped = 0;
  let loggedFirstError = false;

  // Try to store raw payloads in whatever fields the model supports.
  // We deliberately filter to existing columns to avoid hard failures while schema evolves.
  await withCustomerTxn(customerId, async (t) => {
    for (const p of items) {
      // Xero Payments API uses PaymentID; also contains Date/Amount/CurrencyRate and an Invoice reference.
      const paymentId = getFirstKey(p, [
        "PaymentID",
        "paymentId",
        "paymentID",
        "id",
      ]);

      // Best-effort mapping for model fields.
      const invoiceId = getFirstKey(p?.Invoice || p?.invoice, [
        "InvoiceID",
        "invoiceId",
        "invoiceID",
        "id",
      ]);

      const paymentDateRaw = getFirstKey(p, [
        "Date",
        "paymentDate",
        "payment_date",
      ]);
      const paymentDate = paymentDateRaw ? new Date(paymentDateRaw) : null;

      const amountRaw = getFirstKey(p, ["Amount", "amount"]);
      const amount =
        amountRaw !== null && amountRaw !== undefined && amountRaw !== ""
          ? Number(amountRaw)
          : null;

      const currency =
        getFirstKey(p, ["CurrencyRate", "currency", "Currency"]) || null;

      // These cache models REQUIRE: customerId, xeroTenantId, xeroPaymentId, rawPayload, fetchedAt.
      // Only set fields that exist on the Sequelize model via pickModelFields().
      const candidate = {
        customerId,
        ptrsId,
        xeroTenantId: tenantId,
        xeroPaymentId: paymentId,
        invoiceId,
        paymentDate:
          paymentDate && !Number.isNaN(paymentDate.getTime())
            ? paymentDate
            : null,
        amount: Number.isFinite(amount) ? amount : null,
        currency,
        rawPayload: p,
        fetchedAt: fetchedAt || new Date(),
        deletedAt: null,
      };

      const row = pickModelFields(PtrsXeroPayment, candidate);

      try {
        // Prefer upsert if supported and if we have some kind of stable identifier.
        if (
          typeof PtrsXeroPayment.upsert === "function" &&
          (row.xeroPaymentId || row.xeroPaymentID || row.xeroPaymentIdRaw)
        ) {
          await PtrsXeroPayment.upsert(row, { transaction: t });
        } else {
          await PtrsXeroPayment.create(row, { transaction: t });
        }
        inserted++;
      } catch (e) {
        // If we hit uniqueness or schema/RLS issues, don't kill the whole import.
        skipped++;

        if (!loggedFirstError) {
          loggedFirstError = true;
          logger?.error?.("PTRS v2 Xero payment persist failed (first error)", {
            action: "PtrsV2XeroPersistPaymentFailed",
            customerId,
            ptrsId,
            xeroTenantId: tenantId,
            xeroPaymentId: paymentId,
            error: e?.message,
            name: e?.name,
            code: e?.original?.code || e?.parent?.code || null,
            detail: e?.original?.detail || e?.parent?.detail || null,
            constraint:
              e?.original?.constraint || e?.parent?.constraint || null,
          });
        }
      }
    }
  });

  return { inserted, skipped };
}

async function persistInvoices({
  customerId,
  ptrsId,
  tenantId,
  invoices,
  fetchedAt,
}) {
  const PtrsXeroInvoice = getModel("PtrsXeroInvoice");
  if (!PtrsXeroInvoice) {
    return {
      inserted: 0,
      skipped: Array.isArray(invoices) ? invoices.length : 0,
      note: "PtrsXeroInvoice model not found",
    };
  }

  const items = Array.isArray(invoices) ? invoices.filter(Boolean) : [];

  if (!customerId) throw new Error("persistInvoices: customerId is required");
  if (!tenantId) throw new Error("persistInvoices: tenantId is required");

  let inserted = 0;
  let skipped = 0;
  let loggedFirstError = false;

  await withCustomerTxn(customerId, async (t) => {
    for (const inv of items) {
      const xeroInvoiceId = getFirstKey(inv, [
        "InvoiceID",
        "invoiceId",
        "invoiceID",
        "id",
      ]);
      const invoiceNumber =
        getFirstKey(inv, ["InvoiceNumber", "invoiceNumber"]) || null;

      const contactId =
        getFirstKey(inv?.Contact || inv?.contact, [
          "ContactID",
          "contactId",
          "contactID",
          "id",
        ]) || null;

      const invoiceDateRaw = getFirstKey(inv, [
        "Date",
        "invoiceDate",
        "invoice_date",
      ]);
      const invoiceDate = invoiceDateRaw ? new Date(invoiceDateRaw) : null;

      const dueDateRaw = getFirstKey(inv, ["DueDate", "dueDate", "due_date"]);
      const dueDate = dueDateRaw ? new Date(dueDateRaw) : null;

      const status = getFirstKey(inv, ["Status", "status"]) || null;

      const totalRaw = getFirstKey(inv, ["Total", "total"]);
      const total =
        totalRaw !== null && totalRaw !== undefined && totalRaw !== ""
          ? Number(totalRaw)
          : null;

      const currency =
        getFirstKey(inv, ["CurrencyCode", "currency", "Currency"]) || null;

      const candidate = {
        customerId,
        ptrsId,
        xeroTenantId: tenantId,
        xeroInvoiceId,
        invoiceNumber,
        contactId,
        invoiceDate:
          invoiceDate && !Number.isNaN(invoiceDate.getTime())
            ? invoiceDate
            : null,
        dueDate: dueDate && !Number.isNaN(dueDate.getTime()) ? dueDate : null,
        status,
        total: Number.isFinite(total) ? total : null,
        currency,
        rawPayload: inv,
        fetchedAt: fetchedAt || new Date(),
        deletedAt: null,
      };

      const row = pickModelFields(PtrsXeroInvoice, candidate);

      try {
        if (typeof PtrsXeroInvoice.upsert === "function" && row.xeroInvoiceId) {
          await PtrsXeroInvoice.upsert(row, { transaction: t });
        } else {
          await PtrsXeroInvoice.create(row, { transaction: t });
        }
        inserted++;
      } catch (e) {
        skipped++;

        if (!loggedFirstError) {
          loggedFirstError = true;
          logger?.error?.("PTRS v2 Xero invoice persist failed (first error)", {
            action: "PtrsV2XeroPersistInvoiceFailed",
            customerId,
            xeroTenantId: tenantId,
            xeroInvoiceId,
            error: e?.message,
            name: e?.name,
            code: e?.original?.code || e?.parent?.code || null,
            detail: e?.original?.detail || e?.parent?.detail || null,
            constraint:
              e?.original?.constraint || e?.parent?.constraint || null,
          });
        }
      }
    }
  });

  return { inserted, skipped };
}

async function persistContacts({
  customerId,
  ptrsId,
  tenantId,
  contacts,
  fetchedAt,
}) {
  const PtrsXeroContact = getModel("PtrsXeroContact");
  if (!PtrsXeroContact) {
    return {
      inserted: 0,
      skipped: Array.isArray(contacts) ? contacts.length : 0,
      note: "PtrsXeroContact model not found",
    };
  }

  const items = Array.isArray(contacts) ? contacts.filter(Boolean) : [];

  if (!customerId) throw new Error("persistContacts: customerId is required");
  if (!tenantId) throw new Error("persistContacts: tenantId is required");

  let inserted = 0;
  let skipped = 0;
  let loggedFirstError = false;

  await withCustomerTxn(customerId, async (t) => {
    for (const c of items) {
      const xeroContactId = getFirstKey(c, [
        "ContactID",
        "contactId",
        "contactID",
        "id",
      ]);
      const contactName =
        getFirstKey(c, ["Name", "contactName", "name"]) || null;
      const contactStatus =
        getFirstKey(c, ["ContactStatus", "Status", "status"]) || null;

      const isSupplierRaw = getFirstKey(c, ["IsSupplier", "isSupplier"]);
      const isSupplier =
        typeof isSupplierRaw === "boolean"
          ? isSupplierRaw
          : isSupplierRaw === "true"
            ? true
            : isSupplierRaw === "false"
              ? false
              : null;

      const abn = getFirstKey(c, ["TaxNumber", "ABN", "abn"]) || null;

      const candidate = {
        customerId,
        ptrsId,
        xeroTenantId: tenantId,
        xeroContactId,
        contactName,
        contactStatus,
        isSupplier,
        abn,
        rawPayload: c,
        fetchedAt: fetchedAt || new Date(),
        deletedAt: null,
      };

      const row = pickModelFields(PtrsXeroContact, candidate);

      try {
        if (typeof PtrsXeroContact.upsert === "function" && row.xeroContactId) {
          await PtrsXeroContact.upsert(row, { transaction: t });
        } else {
          await PtrsXeroContact.create(row, { transaction: t });
        }
        inserted++;
      } catch (e) {
        skipped++;

        if (!loggedFirstError) {
          loggedFirstError = true;
          logger?.error?.("PTRS v2 Xero contact persist failed (first error)", {
            action: "PtrsV2XeroPersistContactFailed",
            customerId,
            xeroTenantId: tenantId,
            xeroContactId,
            error: e?.message,
            name: e?.name,
            code: e?.original?.code || e?.parent?.code || null,
            detail: e?.original?.detail || e?.parent?.detail || null,
            constraint:
              e?.original?.constraint || e?.parent?.constraint || null,
          });
        }
      }
    }
  });

  return { inserted, skipped };
}

async function persistBankTransactions({
  customerId,
  ptrsId,
  tenantId,
  bankTransactions,
  fetchedAt,
}) {
  const PtrsXeroBankTransaction = getModel("PtrsXeroBankTransaction");
  if (!PtrsXeroBankTransaction) {
    return {
      inserted: 0,
      skipped: Array.isArray(bankTransactions) ? bankTransactions.length : 0,
      note: "PtrsXeroBankTransaction model not found",
    };
  }

  const items = Array.isArray(bankTransactions)
    ? bankTransactions.filter(Boolean)
    : [];

  if (!customerId)
    throw new Error("persistBankTransactions: customerId is required");
  if (!tenantId)
    throw new Error("persistBankTransactions: tenantId is required");

  let inserted = 0;
  let skipped = 0;

  await withCustomerTxn(customerId, async (t) => {
    for (const bt of items) {
      const xeroBankTransactionId = getFirstKey(bt, [
        "BankTransactionID",
        "bankTransactionId",
        "bankTransactionID",
        "id",
      ]);

      const btType = getFirstKey(bt, ["Type", "type", "bankTransactionType"]);

      const btDateRaw = getFirstKey(bt, [
        "Date",
        "date",
        "bankTransactionDate",
      ]);
      const btDate = btDateRaw ? new Date(btDateRaw) : null;

      const totalRaw = getFirstKey(bt, ["Total", "total"]);
      const total =
        totalRaw !== null && totalRaw !== undefined && totalRaw !== ""
          ? Number(totalRaw)
          : null;

      const currency =
        getFirstKey(bt, ["CurrencyCode", "currency", "Currency"]) || null;

      const candidate = {
        customerId,
        ptrsId,
        xeroTenantId: tenantId,
        xeroBankTransactionId,
        bankTransactionType: btType || null,
        bankTransactionDate:
          btDate && !Number.isNaN(btDate.getTime()) ? btDate : null,
        total: Number.isFinite(total) ? total : null,
        currency,
        rawPayload: bt,
        fetchedAt: fetchedAt || new Date(),
        deletedAt: null,
      };

      const row = pickModelFields(PtrsXeroBankTransaction, candidate);

      try {
        if (typeof PtrsXeroBankTransaction.upsert === "function") {
          await PtrsXeroBankTransaction.upsert(row, { transaction: t });
        } else {
          await PtrsXeroBankTransaction.create(row, { transaction: t });
        }
        inserted++;
      } catch (_) {
        skipped++;
      }
    }
  });

  return { inserted, skipped };
}

async function persistOrganisation({
  customerId,
  ptrsId,
  tenantId,
  organisation,
  fetchedAt,
}) {
  const PtrsXeroOrganisation = getModel("PtrsXeroOrganisation");
  if (!PtrsXeroOrganisation) {
    return {
      inserted: 0,
      skipped: organisation ? 1 : 0,
      note: "PtrsXeroOrganisation model not found",
    };
  }

  if (!organisation) return { inserted: 0, skipped: 0 };

  if (!customerId)
    throw new Error("persistOrganisation: customerId is required");
  if (!tenantId) throw new Error("persistOrganisation: tenantId is required");

  const xeroOrganisationId = getFirstKey(organisation, [
    "OrganisationID",
    "organisationId",
    "organisationID",
    "id",
  ]);

  const name = getFirstKey(organisation, ["Name", "name"]) || null;

  // Model requires these
  if (!xeroOrganisationId) {
    return { inserted: 0, skipped: 1, note: "Missing OrganisationID" };
  }
  if (!name) {
    return { inserted: 0, skipped: 1, note: "Missing organisation name" };
  }

  const candidate = {
    customerId,
    ptrsId,
    xeroTenantId: tenantId,
    xeroOrganisationId,
    name,
    legalName: getFirstKey(organisation, ["LegalName", "legalName"]) || null,
    registrationNumber:
      getFirstKey(organisation, ["RegistrationNumber", "registrationNumber"]) ||
      null,
    taxNumber: getFirstKey(organisation, ["TaxNumber", "taxNumber"]) || null,
    paymentTerms:
      organisation?.PaymentTerms || organisation?.paymentTerms || null,
    rawPayload: organisation,
    fetchedAt: fetchedAt || new Date(),
    deletedAt: null,
  };

  const row = pickModelFields(PtrsXeroOrganisation, candidate);

  let inserted = 0;
  let skipped = 0;

  await withCustomerTxn(customerId, async (t) => {
    try {
      if (typeof PtrsXeroOrganisation.upsert === "function") {
        await PtrsXeroOrganisation.upsert(row, { transaction: t });
      } else {
        await PtrsXeroOrganisation.create(row, { transaction: t });
      }
      inserted = 1;
    } catch (e) {
      skipped = 1;
      logger?.error?.("PTRS v2 Xero organisation persist failed", {
        action: "PtrsV2XeroPersistOrganisationFailed",
        customerId,
        ptrsId,
        xeroTenantId: tenantId,
        xeroOrganisationId,
        error: e?.message,
        name: e?.name,
        code: e?.original?.code || e?.parent?.code || null,
        detail: e?.original?.detail || e?.parent?.detail || null,
        constraint: e?.original?.constraint || e?.parent?.constraint || null,
      });
    }
  });

  return { inserted, skipped };
}

const { Op } = require("sequelize");

async function buildRawDatasetFromXeroCache({
  customerId,
  ptrsId,
  tenantIds,
  importStartedAt,
  limit,
}) {
  const PtrsImportRaw = getModel("PtrsImportRaw");
  if (!PtrsImportRaw) {
    throw new Error(
      "PtrsImportRaw model not loaded (db.PtrsImportRaw missing)",
    );
  }

  const PtrsDataset = getModel("PtrsDataset");
  if (!PtrsDataset) {
    throw new Error("PtrsDataset model not loaded (db.PtrsDataset missing)");
  }

  const PtrsXeroPayment = getModel("PtrsXeroPayment");
  const PtrsXeroInvoice = getModel("PtrsXeroInvoice");
  const PtrsXeroContact = getModel("PtrsXeroContact");
  const PtrsXeroOrganisation = getModel("PtrsXeroOrganisation");
  const PtrsXeroBankTransaction = getModel("PtrsXeroBankTransaction");

  if (!PtrsXeroPayment) {
    throw new Error(
      "PtrsXeroPayment model not loaded (db.PtrsXeroPayment missing)",
    );
  }

  const cleanNumber = (val) => {
    if (val === undefined || val === null || val === "") return null;
    const s = String(val).replace(/\s+/g, "").trim();
    if (!s) return null;
    return s; // keep as string (ABNs can have leading zeros in some systems)
  };

  const getFirstLineItem = (raw) => {
    const items = raw?.LineItems;
    if (!Array.isArray(items) || !items.length) return null;
    return items[0] || null;
  };

  const deriveDescription = ({ rawInvoice, rawBankTransaction }) => {
    const li = getFirstLineItem(rawInvoice || rawBankTransaction);
    const desc = typeof li?.Description === "string" ? li.Description : null;
    if (!desc) return "None provided";
    return desc.length > 255 ? desc.slice(0, 255) : desc;
  };

  const deriveAccountCode = ({ rawInvoice, rawBankTransaction }) => {
    const li = getFirstLineItem(rawInvoice || rawBankTransaction);
    const code = li?.AccountCode;
    if (!code) return null;
    return String(code).slice(0, 20);
  };

  const deriveInvoiceReferenceNumber = ({ inv, rawInvoice }) => {
    const invoiceNumber =
      getFirstKey(rawInvoice, ["InvoiceNumber", "invoiceNumber"]) ||
      getFirstKey(inv, ["invoiceNumber"]) ||
      null;

    if (invoiceNumber) return String(invoiceNumber);

    const stableId =
      getFirstKey(rawInvoice, ["InvoiceID", "invoiceId", "invoiceID"]) ||
      getFirstKey(inv, ["xeroInvoiceId", "invoiceId"]) ||
      null;

    if (!stableId) return "None provided";

    return `sys:${String(stableId).slice(0, 8)}`;
  };

  const deriveBankTxnInvoiceRef = (rawBankTransaction) => {
    const li = getFirstLineItem(rawBankTransaction);
    const id = li?.LineItemID || li?.AccountNumber || li?.AccountCode || null;
    return id ? String(id).slice(0, 255) : "None provided";
  };

  // 🔐 Everything must run inside customer-context txn to satisfy RLS
  return await withCustomerTxn(customerId, async (t) => {
    // Ensure a dataset row exists for this Xero main import so raw rows can be scoped.
    // We use role `main` for multi-main support.
    let datasetId = null;

    const existing = await PtrsDataset.findOne({
      where: { customerId, ptrsId, role: "main" },
      order: [["createdAt", "DESC"]],
      transaction: t,
    });

    if (existing) {
      datasetId = existing.id;
    } else {
      const created = await PtrsDataset.create(
        pickModelFields(PtrsDataset, {
          customerId,
          ptrsId,
          role: "main",
          sourceType: "xero",
          sourceName: "Xero",
          fileName: "Xero import",
          storageRef: null,
          rowsCount: null,
          status: "uploaded",
          meta: {
            createdFrom: "xero_import",
            tenantIds: Array.isArray(tenantIds) ? tenantIds : [],
            importStartedAt: importStartedAt
              ? new Date(importStartedAt).toISOString()
              : null,
          },
        }),
        { transaction: t },
      );
      datasetId = created.id;
    }

    if (!datasetId) {
      throw new Error("Failed to establish datasetId for Xero import");
    }
    const whereBase = {
      customerId,
      ...(Array.isArray(tenantIds) && tenantIds.length
        ? { xeroTenantId: tenantIds }
        : {}),
    };

    const dateWhere = importStartedAt
      ? { fetchedAt: { [Op.gte]: importStartedAt } }
      : {};

    const paymentWhere = {
      ...whereBase,
      ...(PtrsXeroPayment.rawAttributes?.ptrsId ? { ptrsId } : {}),
      ...dateWhere,
    };

    const payments = await PtrsXeroPayment.findAll({
      where: paymentWhere,
      order: [["fetchedAt", "DESC"]],
      limit: limit || undefined,
      transaction: t,
    });

    const paymentRows = payments.map((p) => p.toJSON());

    // Bank transactions are a *separate* type of payment record.
    const bankTxRows = [];
    if (PtrsXeroBankTransaction) {
      const bankTxWhere = {
        ...whereBase,
        ...(PtrsXeroBankTransaction.rawAttributes?.ptrsId ? { ptrsId } : {}),
        ...dateWhere,
      };

      const bankTx = await PtrsXeroBankTransaction.findAll({
        where: bankTxWhere,
        order: [["fetchedAt", "DESC"]],
        limit: limit || undefined,
        transaction: t,
      });

      bankTxRows.push(...bankTx.map((bt) => bt.toJSON()));
    }

    const invoiceIds = Array.from(
      new Set(paymentRows.map((p) => p.invoiceId).filter(Boolean)),
    );

    let invoicesById = new Map();
    if (PtrsXeroInvoice && invoiceIds.length) {
      const invoiceWhere = {
        ...whereBase,
        ...(PtrsXeroInvoice.rawAttributes?.ptrsId ? { ptrsId } : {}),
        xeroInvoiceId: invoiceIds,
        ...dateWhere,
      };

      const invoices = await PtrsXeroInvoice.findAll({
        where: invoiceWhere,
        transaction: t,
      });

      invoicesById = new Map(
        invoices.map((inv) => {
          const j = inv.toJSON();
          return [j.xeroInvoiceId, j];
        }),
      );
    }

    // Collect contact IDs from:
    // - invoices linked to payments
    // - bank transactions' embedded Contact.ContactID
    const contactIdsFromInvoices = Array.from(
      new Set(
        Array.from(invoicesById.values())
          .map((inv) => inv.contactId)
          .filter(Boolean),
      ),
    );

    const contactIdsFromBankTx = Array.from(
      new Set(
        bankTxRows
          .map((bt) => {
            const raw = bt?.rawPayload || null;
            const cId = getFirstKey(raw?.Contact || raw?.contact, [
              "ContactID",
              "contactId",
              "contactID",
              "id",
            ]);
            return cId || null;
          })
          .filter(Boolean),
      ),
    );

    const contactIds = Array.from(
      new Set([...contactIdsFromInvoices, ...contactIdsFromBankTx]),
    );

    let contactsById = new Map();
    if (PtrsXeroContact && contactIds.length) {
      const contactWhere = {
        ...whereBase,
        ...(PtrsXeroContact.rawAttributes?.ptrsId ? { ptrsId } : {}),
        xeroContactId: contactIds,
        ...dateWhere,
      };

      const contacts = await PtrsXeroContact.findAll({
        where: contactWhere,
        transaction: t,
      });

      contactsById = new Map(
        contacts.map((c) => {
          const j = c.toJSON();
          return [j.xeroContactId, j];
        }),
      );
    }

    // Pull organisation details so we can populate payer name/ABN (Xero org) per tenant.
    let orgByTenantId = new Map();
    if (PtrsXeroOrganisation) {
      const orgWhere = {
        ...whereBase,
        ...(PtrsXeroOrganisation.rawAttributes?.ptrsId ? { ptrsId } : {}),
        ...dateWhere,
      };

      const orgs = await PtrsXeroOrganisation.findAll({
        where: orgWhere,
        transaction: t,
      });

      // In case multiple org snapshots exist, prefer the most recently fetched per tenant.
      const tmp = new Map();
      for (const o of orgs) {
        const j = o.toJSON();
        const tid = j.xeroTenantId;
        if (!tid) continue;
        const prev = tmp.get(tid);
        if (!prev) {
          tmp.set(tid, j);
          continue;
        }
        const prevAt = prev.fetchedAt ? new Date(prev.fetchedAt).getTime() : 0;
        const nextAt = j.fetchedAt ? new Date(j.fetchedAt).getTime() : 0;
        if (nextAt >= prevAt) tmp.set(tid, j);
      }
      orgByTenantId = tmp;
    }

    // Make reruns deterministic
    await PtrsImportRaw.destroy({
      where: { customerId, ptrsId, datasetId },
      transaction: t,
    });

    const rowsToInsert = [];
    let rowNo = 1;

    // -------------------------
    // 1) Invoice-linked Payments
    // -------------------------
    for (let i = 0; i < paymentRows.length; i++) {
      const p = paymentRows[i];
      const inv = p.invoiceId ? invoicesById.get(p.invoiceId) : null;
      const c = inv?.contactId ? contactsById.get(inv.contactId) : null;
      const org = p?.xeroTenantId ? orgByTenantId.get(p.xeroTenantId) : null;

      const rawPayment = p?.rawPayload || null;
      const rawInvoice = inv?.rawPayload || null;
      const rawContact = c?.rawPayload || null;
      const rawOrganisation = org?.rawPayload || null;

      const invoiceIssueDate = toIsoDateOnlyUtc(
        parseXeroDotNetDate(rawInvoice?.Date || rawInvoice?.DateString),
      );
      const invoiceReceiptDate = invoiceIssueDate;
      const invoiceDueDate = toIsoDateOnlyUtc(
        parseXeroDotNetDate(rawInvoice?.DueDate || rawInvoice?.DueDateString),
      );
      const paymentDate = toIsoDateOnlyUtc(
        parseXeroDotNetDate(rawPayment?.Date),
      );

      const termCfg = deriveXeroPaymentTermsConfig({
        rawInvoice,
        rawContact,
        rawOrganisation,
      });

      const payeeName =
        getFirstKey(rawContact, ["Name"]) || c?.contactName || null;
      const payeeAbn =
        cleanNumber(getFirstKey(rawContact, ["TaxNumber"])) ||
        cleanNumber(c?.abn) ||
        null;
      const payeeAcnArbn =
        cleanNumber(getFirstKey(rawContact, ["CompanyNumber"])) || null;

      const payerName =
        getFirstKey(rawOrganisation, ["Name", "LegalName"]) ||
        org?.name ||
        org?.legalName ||
        null;
      const payerAbn =
        cleanNumber(getFirstKey(rawOrganisation, ["TaxNumber"])) ||
        cleanNumber(org?.taxNumber) ||
        null;
      const payerAcnArbn =
        cleanNumber(getFirstKey(rawOrganisation, ["RegistrationNumber"])) ||
        cleanNumber(org?.registrationNumber) ||
        null;

      // Contact payment terms (v1-style): PaymentTerms.Bills.Day
      const billsDay =
        rawContact?.PaymentTerms?.Bills &&
        typeof rawContact.PaymentTerms.Bills.Day === "number"
          ? rawContact.PaymentTerms.Bills.Day
          : null;

      const invoiceAmountRaw =
        getFirstKey(rawInvoice, ["Total"]) ?? inv?.total ?? null;
      const invoiceAmount =
        invoiceAmountRaw !== null &&
        invoiceAmountRaw !== undefined &&
        invoiceAmountRaw !== ""
          ? String(invoiceAmountRaw)
          : null;

      const paymentAmountRaw =
        getFirstKey(rawPayment, ["Amount"]) ?? p?.amount ?? null;
      const paymentAmount =
        paymentAmountRaw !== null &&
        paymentAmountRaw !== undefined &&
        paymentAmountRaw !== ""
          ? String(paymentAmountRaw)
          : null;

      rowsToInsert.push({
        customerId,
        ptrsId,
        datasetId,
        rowNo: rowNo++,
        data: {
          // Core identifiers
          source: "xero",
          xeroRecordType: "payment",
          xeroTenantId: p.xeroTenantId,
          xeroPaymentId: p.xeroPaymentId,
          xeroInvoiceId: p.invoiceId || inv?.xeroInvoiceId || null,
          xeroContactId: inv?.contactId || c?.xeroContactId || null,

          // v1-style map-ready fields
          payerEntityName: payerName,
          payerEntityAbn: payerAbn,
          payerEntityAcnArbn: payerAcnArbn,

          payeeEntityName: payeeName,
          payeeEntityAbn: payeeAbn,
          payeeEntityAcnArbn: payeeAcnArbn,

          paymentAmount,
          paymentDate,
          transactionType:
            getFirstKey(rawPayment, ["PaymentType"]) ||
            rawPayment?.PaymentType ||
            null,
          isReconciled:
            typeof rawPayment?.IsReconciled === "boolean"
              ? rawPayment.IsReconciled
              : null,

          description: deriveDescription({ rawInvoice }),
          accountCode: deriveAccountCode({ rawInvoice }),

          invoiceReferenceNumber: deriveInvoiceReferenceNumber({
            inv,
            rawInvoice,
          }),
          invoiceIssueDate,
          invoiceReceiptDate,
          invoiceAmount,
          invoiceDueDate,

          // Payment terms candidates
          contractPoPaymentTerms:
            billsDay !== null && billsDay !== undefined
              ? String(billsDay)
              : null,
          paymentTermsPurchasesDayCandidate:
            termCfg?.purchasesDay !== null &&
            termCfg?.purchasesDay !== undefined
              ? String(termCfg.purchasesDay)
              : null,
          paymentTermsPurchasesTypeCandidate: termCfg?.purchasesType || null,
          rawPaymentTermsCandidate: termCfg?.raw || null,

          // Legacy convenience fields (keep existing FE assumptions alive)
          payerName: payerName,
          payerAbn: payerAbn,
          supplierName: payeeName,
          supplierAbn: payeeAbn,
          invoiceTotal: invoiceAmount,
          dueDate: invoiceDueDate,
          amount: paymentAmount,
          currency: p.currency || null,
          invoiceNumber: inv?.invoiceNumber || null,
          invoiceStatus: inv?.status || null,
          supplierStatus: c?.contactStatus || null,

          // Raw blobs for audit/debug
          rawPayment,
          rawInvoice,
          rawContact,
          rawOrganisation,
        },
      });
    }

    // -------------------------
    // 2) Bank Transactions (separate payment records)
    // -------------------------
    for (let i = 0; i < bankTxRows.length; i++) {
      const bt = bankTxRows[i];
      const rawBankTransaction = bt?.rawPayload || null;

      const org = bt?.xeroTenantId ? orgByTenantId.get(bt.xeroTenantId) : null;
      const rawOrganisation = org?.rawPayload || null;

      const contactId = getFirstKey(
        rawBankTransaction?.Contact || rawBankTransaction?.contact,
        ["ContactID", "contactId", "contactID", "id"],
      );
      const c = contactId ? contactsById.get(contactId) : null;
      const rawContact = c?.rawPayload || null;

      const paymentDate = toIsoDateOnlyUtc(
        parseXeroDotNetDate(rawBankTransaction?.Date),
      );

      const payerName =
        getFirstKey(rawOrganisation, ["Name", "LegalName"]) ||
        org?.name ||
        org?.legalName ||
        null;
      const payerAbn =
        cleanNumber(getFirstKey(rawOrganisation, ["TaxNumber"])) ||
        cleanNumber(org?.taxNumber) ||
        null;
      const payerAcnArbn =
        cleanNumber(getFirstKey(rawOrganisation, ["RegistrationNumber"])) ||
        cleanNumber(org?.registrationNumber) ||
        null;

      const payeeName =
        getFirstKey(rawContact, ["Name"]) || c?.contactName || null;
      const payeeAbn =
        cleanNumber(getFirstKey(rawContact, ["TaxNumber"])) ||
        cleanNumber(c?.abn) ||
        null;
      const payeeAcnArbn =
        cleanNumber(getFirstKey(rawContact, ["CompanyNumber"])) || null;

      const totalRaw =
        getFirstKey(rawBankTransaction, ["Total"]) ?? bt?.total ?? null;
      const paymentAmount =
        totalRaw !== null && totalRaw !== undefined && totalRaw !== ""
          ? String(totalRaw)
          : null;

      const invoiceReferenceNumber =
        deriveBankTxnInvoiceRef(rawBankTransaction);

      rowsToInsert.push({
        customerId,
        ptrsId,
        datasetId,
        rowNo: rowNo++,
        data: {
          source: "xero",
          xeroRecordType: "bankTransaction",
          xeroTenantId: bt.xeroTenantId || null,
          xeroBankTransactionId:
            bt.xeroBankTransactionId ||
            getFirstKey(rawBankTransaction, ["BankTransactionID"]) ||
            null,

          payerEntityName: payerName,
          payerEntityAbn: payerAbn,
          payerEntityAcnArbn: payerAcnArbn,

          payeeEntityName: payeeName,
          payeeEntityAbn: payeeAbn,
          payeeEntityAcnArbn: payeeAcnArbn,

          paymentAmount,
          paymentDate,
          transactionType: getFirstKey(rawBankTransaction, ["Type"]) || null,
          isReconciled:
            typeof rawBankTransaction?.IsReconciled === "boolean"
              ? rawBankTransaction.IsReconciled
              : null,

          description: deriveDescription({ rawBankTransaction }),
          accountCode: deriveAccountCode({ rawBankTransaction }),

          // Bank tx has no invoice; provide sensible candidates to map from.
          invoiceReferenceNumber,
          invoiceIssueDate: null,
          invoiceReceiptDate: paymentDate,
          invoiceAmount: paymentAmount,
          invoiceDueDate: paymentDate,

          // Legacy convenience fields
          payerName: payerName,
          payerAbn: payerAbn,
          supplierName: payeeName,
          supplierAbn: payeeAbn,
          invoiceTotal: paymentAmount,
          dueDate: paymentDate,
          amount: paymentAmount,
          currency: bt.currency || null,

          // Raw blobs
          rawBankTransaction,
          rawContact,
          rawOrganisation,
        },
      });
    }

    if (rowsToInsert.length) {
      await PtrsImportRaw.bulkCreate(rowsToInsert, { transaction: t });
    }

    // Best-effort: update dataset rowsCount
    try {
      await PtrsDataset.update(
        { rowsCount: rowsToInsert.length, updatedAt: new Date() },
        { where: { id: datasetId, customerId, ptrsId }, transaction: t },
      );
    } catch (_) {}

    return { insertedRows: rowsToInsert.length, datasetId };
  });
}

// ------------------------
// OAuth + tenant selection
// ------------------------

async function connect({ customerId, ptrsId, userId }) {
  const state = Buffer.from(
    JSON.stringify({ ptrsId, customerId, ts: Date.now() }),
  ).toString("base64url");

  const redirectUri = requireEnv("XERO_REDIRECT_URI");
  const authUrl = xeroClient.buildAuthUrl({ state, redirectUri });

  logger?.info?.("PTRS v2 Xero connect URL generated", {
    action: "PtrsV2XeroConnectUrl",
    customerId,
    ptrsId,
    userId,
  });

  return { ptrsId, authUrl };
}

async function handleCallback({ ptrsId, code, state }) {
  if (!code) throw new Error("Missing authorisation code");

  let parsedState = null;
  try {
    parsedState = state
      ? JSON.parse(Buffer.from(String(state), "base64url").toString("utf8"))
      : null;
  } catch (_) {
    parsedState = null;
  }

  const effectivePtrsId = ptrsId || parsedState?.ptrsId || null;
  if (!effectivePtrsId) {
    throw new Error(
      "Missing ptrsId in Xero callback. Ensure connect() was called before OAuth redirect.",
    );
  }

  const customerId = parsedState?.customerId || null;
  if (!customerId) {
    throw new Error(
      "Missing customerId in Xero callback state. Ensure connect() was called before OAuth redirect.",
    );
  }

  const redirectUri = requireEnv("XERO_REDIRECT_URI");

  const token = await xeroClient.exchangeAuthCodeForToken({
    code,
    redirectUri,
  });

  const connections = await xeroClient.listConnections({
    accessToken: token.access_token,
  });

  const organisations = (connections || []).map((c) => ({
    tenantId: c.tenantId,
    tenantName: c.tenantName,
  }));

  if (!organisations.length) {
    throw new Error("No Xero organisations returned from connections endpoint");
  }

  const XeroToken = db.XeroToken || db.models?.XeroToken;
  if (!XeroToken) {
    throw new Error("XeroToken model not loaded (db.XeroToken missing)");
  }

  const expires = xeroClient.computeExpiresFromToken(token);

  await withCustomerTxn(customerId, async (t) => {
    // Multi-tenant: replace tokens PER TENANT, not by nuking everything for the customer.
    for (const org of organisations) {
      if (!org?.tenantId) continue;

      await XeroToken.update(
        { revoked: new Date(), revokedByIp: "system" },
        {
          where: { customerId, tenantId: org.tenantId, revoked: null },
          transaction: t,
        },
      );

      await XeroToken.create(
        {
          access_token: token.access_token,
          refresh_token: token.refresh_token,
          scope: token.scope || "",
          expires,
          created: new Date(),
          createdByIp: "system",
          revoked: null,
          revokedByIp: null,
          replacedByToken: null,
          customerId,
          tenantId: org.tenantId,
          // Persist tenantName if the column exists (it does in most schemas)
          ...(org?.tenantName ? { tenantName: org.tenantName } : {}),
        },
        { transaction: t },
      );
    }
  });

  const frontEndBase = process.env.FRONTEND_URL || "http://localhost:3000";
  const orgParam = encodeURIComponent(JSON.stringify(organisations));
  const redirectUrl = `${frontEndBase}/v2/ptrs/xero/select?ptrsId=${encodeURIComponent(
    effectivePtrsId,
  )}&organisations=${orgParam}`;

  return { redirectUrl, organisations };
}

async function getOrganisations({ customerId, ptrsId }) {
  const XeroToken = db.XeroToken || db.models?.XeroToken;
  if (!XeroToken) {
    throw new Error("XeroToken model not loaded (db.XeroToken missing)");
  }

  const rows = await withCustomerTxn(customerId, async (t) => {
    return await XeroToken.findAll({
      where: { customerId, revoked: null },
      order: [["created", "DESC"]],
      limit: 50,
      transaction: t,
    });
  });

  // Prefer the newest row per tenantId (rows are ordered DESC by created)
  const byTenant = new Map();
  for (const r of rows || []) {
    if (!r?.tenantId) continue;
    if (!byTenant.has(r.tenantId)) {
      byTenant.set(r.tenantId, r);
    }
  }

  const tenantIds = Array.from(byTenant.keys());

  // If we have cached organisation records, use their names as the authoritative display label.
  // This avoids relying on tenantName being present in XeroToken rows.
  let orgNameByTenantId = new Map();
  let orgIdByTenantId = new Map();

  const PtrsXeroOrganisation = getModel("PtrsXeroOrganisation");
  if (PtrsXeroOrganisation && tenantIds.length) {
    const orgRows = await withCustomerTxn(customerId, async (t) => {
      return await PtrsXeroOrganisation.findAll({
        where: {
          customerId,
          ...(PtrsXeroOrganisation.rawAttributes?.xeroTenantId
            ? { xeroTenantId: tenantIds }
            : {}),
        },
        order: [["fetchedAt", "DESC"]],
        transaction: t,
      });
    });

    // Prefer most recently fetched org per tenant
    for (const o of orgRows || []) {
      const j = typeof o.toJSON === "function" ? o.toJSON() : o;
      const tid = j?.xeroTenantId || j?.tenantId;
      if (!tid) continue;

      if (!orgNameByTenantId.has(tid)) {
        const name = j?.name || j?.legalName || null;
        if (name) orgNameByTenantId.set(tid, name);

        const oid = j?.xeroOrganisationId || j?.xeroOrganisationID || null;
        if (oid) orgIdByTenantId.set(tid, oid);
      }
    }
  }

  const organisations = tenantIds.map((tid) => {
    const tokenRow = byTenant.get(tid);
    const cachedName = orgNameByTenantId.get(tid) || null;

    return {
      tenantId: tid,
      // Prefer cached org name, then tokenRow.tenantName, then tenantId.
      tenantName: cachedName || tokenRow?.tenantName || tid,
      // Optional extra metadata (harmless for FE; useful for debugging)
      xeroOrganisationId: orgIdByTenantId.get(tid) || null,
      nameSource: cachedName
        ? "cache"
        : tokenRow?.tenantName
          ? "token"
          : "fallback",
    };
  });

  logger?.info?.("PTRS v2 Xero organisations hydrated", {
    action: "PtrsV2XeroOrganisationsHydrated",
    customerId,
    ptrsId,
    count: organisations.length,
    organisations,
  });

  return { ptrsId, status: "READY", organisations };
}

async function selectOrganisations({ customerId, ptrsId, tenantIds, userId }) {
  const selected = Array.isArray(tenantIds) ? tenantIds.filter(Boolean) : [];
  if (!selected.length) throw new Error("tenantIds is required");

  // Persist selection for this run in-memory (MVP)
  setSelectedTenantIds(customerId, ptrsId, selected);

  const PtrsXeroImport = getModel("PtrsXeroImport");
  if (PtrsXeroImport && PtrsXeroImport.rawAttributes?.selectedTenantIds) {
    await withCustomerTxn(customerId, async (t) => {
      await PtrsXeroImport.upsert(
        pickModelFields(PtrsXeroImport, {
          customerId,
          ptrsId,
          selectedTenantIds: selected,
          updatedAt: new Date(),
        }),
        { transaction: t },
      );
    });
  }

  // Multi-tenant MVP: do not revoke non-selected tenants here.
  // Selection persistence can be added later (e.g., on the PTRS run), but for now we
  // keep tokens for all connected tenants and rely on later steps to choose how to
  // import/aggregate.
  logger?.info?.("PTRS v2 Xero organisations selected", {
    action: "PtrsV2XeroOrganisationsSelect",
    customerId,
    ptrsId,
    tenantIds: selected,
    userId,
  });

  return { ptrsId, status: "READY", selectedTenantIds: selected };
}

async function removeOrganisation({ customerId, ptrsId, tenantId, userId }) {
  const XeroToken = db.XeroToken || db.models?.XeroToken;
  if (!XeroToken)
    throw new Error("XeroToken model not loaded (db.XeroToken missing)");

  await withCustomerTxn(customerId, async (t) => {
    await XeroToken.update(
      { revoked: new Date(), revokedByIp: "system" },
      { where: { customerId, tenantId, revoked: null }, transaction: t },
    );
  });

  logger?.info?.("PTRS v2 Xero organisation removed", {
    action: "PtrsV2XeroOrganisationRemove",
    customerId,
    ptrsId,
    tenantId,
    userId,
  });

  return { ptrsId, status: "READY" };
}

// ------------------------
// Import + status (MVP placeholders)
// ------------------------

async function startImport({ customerId, ptrsId, userId }) {
  const importStartedAt = new Date();

  // Source of truth: the PTRS run itself (periodStart/periodEnd)
  const Ptrs = getModel("Ptrs");
  if (!Ptrs) {
    throw new Error("Ptrs model not loaded (db.Ptrs missing)");
  }

  const ptrsRow = await withCustomerTxn(customerId, async (t) => {
    return await Ptrs.findOne({
      where: { customerId, id: ptrsId },
      transaction: t,
    });
  });

  if (!ptrsRow) {
    throw new Error(`PTRS run not found for ptrsId ${ptrsId}`);
  }

  const periodStart = assertIsoDateOnly(ptrsRow.periodStart, "periodStart");
  const periodEnd = assertIsoDateOnly(ptrsRow.periodEnd, "periodEnd");

  if (!periodStart || !periodEnd) {
    const failed = updateStatus(customerId, ptrsId, {
      status: "FAILED",
      message:
        "PTRS reporting period is not set (periodStart/periodEnd). Please set the reporting period in step 1 and try again.",
      progress: { extractedCount: 0, insertedCount: 0 },
    });
    return failed;
  }

  const extractLimit = getXeroExtractLimit();
  let selectedTenantIds = getSelectedTenantIds(customerId, ptrsId);

  if (!selectedTenantIds.length) {
    const PtrsXeroImport = getModel("PtrsXeroImport");
    if (PtrsXeroImport && PtrsXeroImport.rawAttributes?.selectedTenantIds) {
      const row = await withCustomerTxn(customerId, async (t) => {
        return await PtrsXeroImport.findOne({
          where: { customerId, ptrsId },
          transaction: t,
        });
      });
      const persisted = row?.selectedTenantIds;
      if (Array.isArray(persisted) && persisted.length) {
        selectedTenantIds = persisted.filter(Boolean);
        setSelectedTenantIds(customerId, ptrsId, selectedTenantIds);
      }
    }
  }

  if (!selectedTenantIds.length) {
    // Fail loudly: user must select at least one org for MVP.
    const failed = updateStatus(customerId, ptrsId, {
      status: "FAILED",
      message:
        "No organisations selected for this PTRS run. Please select an organisation first.",
      progress: { extractLimit, extractedCount: 0, tenantCount: 0 },
    });
    return failed;
  }

  // Seed status immediately so the FE can render something.
  const seed = updateStatus(customerId, ptrsId, {
    status: "RUNNING",
    message: "Starting Xero import…",
    progress: {
      extractLimit,
      extractedCount: 0,
      tenantCount: selectedTenantIds.length,
      currentTenantIndex: 0,
    },
  });

  // Fire-and-forget runner (MVP). Status is polled via /status.
  setImmediate(async () => {
    try {
      let tenantTmr = null;

      const safeEndTenantTimer = (result = "ok", more = {}, level = "info") => {
        if (tenantTmr && typeof tenantTmr.end === "function") {
          try {
            tenantTmr.end(result, more, level);
          } catch (_) {
            // Never let timer logging crash the import.
          }
        }
      };

      // ---- observability / heartbeat (MVP) ----
      const startedMs = Date.now();
      let lastHeartbeatMs = 0;

      const heartbeat = (message, progressPatch = {}, level = "info") => {
        const now = Date.now();
        // Don't spam: max 1 log line per ~5s unless explicitly forced.
        if (now - lastHeartbeatMs < 5000 && level === "info") return;
        lastHeartbeatMs = now;

        updateStatus(customerId, ptrsId, {
          status: "RUNNING",
          message,
          progress: {
            ...(progressPatch || {}),
            tookMs: now - startedMs,
          },
        });

        logger?.[level]?.("PTRS v2 Xero import heartbeat", {
          action: "PtrsV2XeroImportHeartbeat",
          customerId,
          ptrsId,
          message,
          ...progressPatch,
          tookMs: now - startedMs,
        });
      };

      logger?.info?.("PTRS v2 Xero import runner started", {
        action: "PtrsV2XeroImportRunnerStart",
        customerId,
        ptrsId,
        userId,
        periodStart,
        periodEnd,
        extractLimit,
        tenantCount: selectedTenantIds.length,
      });
      const counter = { count: 0 };
      let insertedCount = 0;

      let paymentsInserted = 0;
      let invoicesInserted = 0;
      let contactsInserted = 0;
      let bankTxInserted = 0;

      // Runner-scope defaults (avoid ReferenceError in outer catch/log paths)
      let paymentsPagesFetched = 0;
      let paymentsFetched = 0;
      let invoicesFetched = 0;
      let contactsFetched = 0;
      let bankTxPagesFetched = 0;
      let bankTxFetched = 0;

      // Track non-fatal fetch issues so the FE can show them.
      let invoiceFetchFailedCount = 0;
      let contactFetchFailedCount = 0;
      let bankTxFetchFailedCount = 0;

      const invoiceFetchFailedSample = [];
      const contactFetchFailedSample = [];
      const bankTxFetchFailedSample = [];

      const addSample = (arr, value, max = 10) => {
        if (!value) return;
        if (arr.length >= max) return;
        if (arr.includes(value)) return;
        arr.push(value);
      };

      const getErrStatusCode = (e) =>
        e?.statusCode || e?.response?.status || e?.response?.statusCode || null;

      try {
        const XeroToken = db.XeroToken || db.models?.XeroToken;
        if (!XeroToken) {
          throw new Error("XeroToken model not loaded (db.XeroToken missing)");
        }

        updateStatus(customerId, ptrsId, {
          status: "RUNNING",
          message: "Import in progress…",
        });

        // Process tenants sequentially (MVP) to avoid rate-limit spikes.
        for (let i = 0; i < selectedTenantIds.length; i++) {
          const tenantId = selectedTenantIds[i];
          tenantTmr = phaseTimer({
            customerId,
            ptrsId,
            tenantId,
            phase: `tenant:${i + 1}/${selectedTenantIds.length}`,
          });
          // Per-tenant progress counters for visibility
          let tenantPaymentsPagesFetched = 0;
          let tenantPaymentsFetched = 0;
          let tenantInvoicesFetched = 0;
          let tenantContactsFetched = 0;
          let tenantBankTxPagesFetched = 0;
          let tenantBankTxFetched = 0;

          // Keep runner-scope copies updated so outer-scope logs never ReferenceError
          paymentsPagesFetched = 0;
          paymentsFetched = 0;
          invoicesFetched = 0;
          contactsFetched = 0;
          bankTxPagesFetched = 0;
          bankTxFetched = 0;

          heartbeat(
            `Tenant ${i + 1}/${selectedTenantIds.length}: starting import…`,
            {
              tenantId,
              currentTenantIndex: i + 1,
              tenantCount: selectedTenantIds.length,
            },
            "info",
          );

          updateStatus(customerId, ptrsId, {
            status: "RUNNING",
            message: `Importing from Xero organisation ${i + 1} of ${
              selectedTenantIds.length
            }…`,
            progress: { currentTenantIndex: i + 1 },
          });

          // Get active token row for this tenant/customer
          let tokenRow = await withCustomerTxn(customerId, async (t) => {
            return await XeroToken.findOne({
              where: { customerId, tenantId, revoked: null },
              order: [["created", "DESC"]],
              transaction: t,
            });
          });

          if (!tokenRow) {
            throw new Error(
              `No active Xero token found for selected tenantId ${tenantId}`,
            );
          }

          // Persist organisation details for this tenant (needed for payer ABN/name + org display)
          try {
            const orgFetchedAt = new Date();
            const orgTmr = phaseTimer({
              customerId,
              ptrsId,
              tenantId,
              phase: "tenant:organisation",
            });

            const { item: organisation, tokenRow: orgTokenRow } =
              await fetchOrganisationDetails({
                customerId,
                tokenRow,
                tenantId,
              });

            // Keep using the freshest token in case auto-refresh occurred
            if (orgTokenRow) tokenRow = orgTokenRow;

            await persistOrganisation({
              customerId,
              ptrsId,
              tenantId,
              organisation,
              fetchedAt: orgFetchedAt,
            });

            heartbeat(
              `Tenant ${i + 1}/${selectedTenantIds.length}: organisation cached`,
              { tenantId, currentTenantIndex: i + 1, orgCached: true },
              "info",
            );

            orgTmr.end("ok", { orgCached: true });
          } catch (e) {
            const orgTmr = phaseTimer({
              customerId,
              ptrsId,
              tenantId,
              phase: "tenant:organisation",
            });
            orgTmr.end(
              "failed",
              { error: e?.message, statusCode: getErrStatusCode(e) },
              "warn",
            );

            // Do not fail the whole import if org fetch/persist fails; log once per tenant
            const meta = getHttpErrorMeta(e);
            logger?.warn?.("PTRS v2 Xero organisation fetch/persist failed", {
              action: "PtrsV2XeroPersistOrganisationFailed",
              customerId,
              ptrsId,
              tenantId,
              error: e?.message,
              ...meta,
            });
          }

          // Refresh token if needed (expiry-aware)
          tokenRow = await refreshAccessTokenIfNeeded({
            customerId,
            tenantId,
            tokenRow,
          });

          // Fetch payments (paged) and apply global extract limit.
          const paymentsTmr = phaseTimer({
            customerId,
            ptrsId,
            tenantId,
            phase: "tenant:payments",
          });
          const tenantPaymentItems = [];
          let paymentsEmpty = false;

          await paginateXeroApi(
            async (pageNum) => {
              const paymentPage = await fetchPaymentsPage({
                customerId,
                tokenRow,
                tenantId,
                periodStart,
                periodEnd,
                page: pageNum,
              });
              tokenRow = paymentPage.tokenRow;
              tenantPaymentsPagesFetched = pageNum;
              paymentsPagesFetched = pageNum;
              return {
                data: { Payments: paymentPage.items },
                headers: {},
                status: 200,
              };
            },
            async (response) => {
              const pageItems = Array.isArray(response?.data?.Payments)
                ? response.data.Payments
                : [];
              if (!pageItems.length) {
                paymentsEmpty = true;
                return;
              }

              const { items: limitedItems, done } = applyExtractLimit(
                pageItems,
                extractLimit,
                counter,
              );

              tenantPaymentItems.push(...limitedItems);
              tenantPaymentsFetched += limitedItems.length;
              paymentsFetched = tenantPaymentsFetched;

              // Heartbeat while paging (prevents the UI feeling dead and helps diagnose stalls)
              heartbeat(
                `Tenant ${i + 1}/${selectedTenantIds.length}: fetching payments… (${tenantPaymentsFetched} so far, page ${tenantPaymentsPagesFetched})`,
                {
                  tenantId,
                  currentTenantIndex: i + 1,
                  tenantCount: selectedTenantIds.length,
                  paymentsPagesFetched: tenantPaymentsPagesFetched,
                  paymentsFetched: tenantPaymentsFetched,
                  extractedCount: counter.count,
                  extractLimit,
                },
                "info",
              );

              if (done) paymentsEmpty = true;
            },
            {
              startPage: 1,
              // Stop when the API returns an empty page, or we hit the global cap.
              hasMoreFn: () => !paymentsEmpty,
            },
          );

          paymentsTmr.end("ok", {
            paymentsPagesFetched: tenantPaymentsPagesFetched,
            paymentsFetched: tenantPaymentsFetched,
            extractedCount: counter.count,
            extractLimit,
          });

          const persistPaymentsTmr = phaseTimer({
            customerId,
            ptrsId,
            tenantId,
            phase: "tenant:persistPayments",
          });

          // Persist payments (best-effort)
          const payPersist = await persistPayments({
            customerId,
            ptrsId,
            tenantId,
            payments: tenantPaymentItems,
            fetchedAt: importStartedAt,
          });

          persistPaymentsTmr.end("ok", {
            inserted: payPersist?.inserted,
            skipped: payPersist?.skipped,
          });

          paymentsInserted += payPersist.inserted || 0;

          insertedCount += payPersist.inserted || 0;

          // Derive invoice IDs from the payments we actually kept.
          const invoiceIds = Array.from(
            new Set(
              tenantPaymentItems
                .map((p) =>
                  getFirstKey(p?.Invoice || p?.invoice, [
                    "InvoiceID",
                    "invoiceId",
                    "invoiceID",
                    "id",
                  ]),
                )
                .filter(Boolean),
            ),
          );

          updateStatus(customerId, ptrsId, {
            status: "RUNNING",
            message: `Fetched ${tenantPaymentItems.length} payments (saved ${payPersist.inserted || 0}) from organisation ${i + 1} of ${
              selectedTenantIds.length
            }. Fetching related invoices/contacts…`,
            progress: {
              extractedCount: counter.count,
              insertedCount,
              lastTenantId: tenantId,
            },
          });

          // Fetch + persist invoices (deduped). This is the minimum we need to later build the PTRS dataset.
          const invoices = [];
          for (const invId of invoiceIds) {
            try {
              const invoiceRes = await fetchInvoiceById({
                customerId,
                tokenRow,
                tenantId,
                invoiceId: invId,
              });
              tokenRow = invoiceRes.tokenRow;
              const inv = invoiceRes.item;
              if (inv) invoices.push(inv);
              tenantInvoicesFetched++;
              invoicesFetched = tenantInvoicesFetched;
              if (tenantInvoicesFetched % 25 === 0) {
                heartbeat(
                  `Tenant ${i + 1}/${selectedTenantIds.length}: fetched ${tenantInvoicesFetched}/${invoiceIds.length} invoices…`,
                  {
                    tenantId,
                    currentTenantIndex: i + 1,
                    invoicesFetched: tenantInvoicesFetched,
                    invoicesTotal: invoiceIds.length,
                    paymentsFetched: tenantPaymentsFetched,
                    paymentsPagesFetched: tenantPaymentsPagesFetched,
                  },
                  "info",
                );
              }
            } catch (e) {
              // Don't kill import; surface failures as warnings in status.
              invoiceFetchFailedCount++;
              addSample(invoiceFetchFailedSample, invId);

              const sc = getErrStatusCode(e);

              const meta = getHttpErrorMeta(e);

              logger?.warn?.("PTRS v2 Xero invoice fetch failed", {
                action: "PtrsV2XeroFetchInvoiceFailed",
                customerId,
                ptrsId,
                xeroTenantId: tenantId,
                invoiceId: invId,
                error: e?.message,
                statusCode: sc,
                ...meta,
              });

              if (meta?.statusCode === 404) {
                await recordImportException({
                  customerId,
                  ptrsId,
                  importRunId: ptrsId, // MVP: group by ptrsId (your current run identifier)
                  source: "xero",
                  phase: "fetchInvoiceById",
                  severity: "error",
                  statusCode: meta.statusCode,
                  method: meta.method,
                  url: meta.url,
                  message:
                    meta.responseBody || e?.message || "Invoice not found",
                  xeroTenantId: tenantId,
                  invoiceId: invId,
                  responseBody: meta.responseBody || null,
                  meta: {
                    error: e?.message || null,
                  },
                });
              }

              // Give the FE a hint without flipping the run to FAILED.
              updateStatus(customerId, ptrsId, {
                status: "RUNNING",
                message:
                  sc === 404
                    ? "Some invoices referenced by payments were not found in Xero (404). Continuing…"
                    : "Some invoices could not be fetched from Xero. Continuing…",
                progress: {
                  extractedCount: counter.count,
                  insertedCount,
                  lastTenantId: tenantId,
                  invoiceFetchFailedCount,
                  invoiceFetchFailedSample,
                },
              });
            }
          }

          const invPersist = await persistInvoices({
            customerId,
            tenantId,
            ptrsId,
            invoices,
            fetchedAt: importStartedAt,
          });

          invoicesInserted += invPersist.inserted || 0;

          // Derive contact IDs from invoices.
          const contactIds = Array.from(
            new Set(
              invoices
                .map((inv) =>
                  getFirstKey(inv?.Contact || inv?.contact, [
                    "ContactID",
                    "contactId",
                    "contactID",
                    "id",
                  ]),
                )
                .filter(Boolean),
            ),
          );

          updateStatus(customerId, ptrsId, {
            status: "RUNNING",
            message: `Saved ${payPersist.inserted || 0} payments, ${invPersist.inserted || 0} invoices. Fetching contacts…`,
            progress: {
              extractedCount: counter.count,
              insertedCount,
              lastTenantId: tenantId,
            },
          });

          const contacts = [];
          for (const cId of contactIds) {
            try {
              const contactRes = await fetchContactById({
                customerId,
                tokenRow,
                tenantId,
                contactId: cId,
              });
              tokenRow = contactRes.tokenRow;
              const c = contactRes.item;
              if (c) contacts.push(c);
              tenantContactsFetched++;
              contactsFetched = tenantContactsFetched;
              if (tenantContactsFetched % 25 === 0) {
                heartbeat(
                  `Tenant ${i + 1}/${selectedTenantIds.length}: fetched ${tenantContactsFetched}/${contactIds.length} contacts…`,
                  {
                    tenantId,
                    currentTenantIndex: i + 1,
                    contactsFetched: tenantContactsFetched,
                    contactsTotal: contactIds.length,
                    invoicesFetched: tenantInvoicesFetched,
                  },
                  "info",
                );
              }
            } catch (e) {
              contactFetchFailedCount++;
              addSample(contactFetchFailedSample, cId);

              const sc = getErrStatusCode(e);

              const meta = getHttpErrorMeta(e);

              logger?.warn?.("PTRS v2 Xero contact fetch failed", {
                action: "PtrsV2XeroFetchContactFailed",
                customerId,
                ptrsId,
                xeroTenantId: tenantId,
                contactId: cId,
                error: e?.message,
                statusCode: sc,
                ...meta,
              });

              updateStatus(customerId, ptrsId, {
                status: "RUNNING",
                message:
                  "Some contacts could not be fetched from Xero. Continuing…",
                progress: {
                  extractedCount: counter.count,
                  insertedCount,
                  lastTenantId: tenantId,
                  contactFetchFailedCount,
                  contactFetchFailedSample,
                },
              });
            }
          }

          const contactPersist = await persistContacts({
            customerId,
            ptrsId,
            tenantId,
            contacts,
            fetchedAt: importStartedAt,
          });

          contactsInserted += contactPersist.inserted || 0;

          // Bank transactions (v1 parity). Paged, but bounded by extractLimit for safety.
          // For MVP we only cache a small sample and do not interpret allocations yet.
          const bankTxItems = [];
          try {
            if (extractLimit) {
              const btCounter = { count: 0 };
              let btEmpty = false;

              await paginateXeroApi(
                async (pageNum) => {
                  const btPageRes = await fetchBankTransactionsPage({
                    customerId,
                    tokenRow,
                    tenantId,
                    periodStart,
                    periodEnd,
                    page: pageNum,
                  });
                  tokenRow = btPageRes.tokenRow;
                  tenantBankTxPagesFetched = pageNum;
                  bankTxPagesFetched = pageNum;
                  return {
                    data: { BankTransactions: btPageRes.items },
                    headers: {},
                    status: 200,
                  };
                },
                async (response) => {
                  const pageItems = Array.isArray(
                    response?.data?.BankTransactions,
                  )
                    ? response.data.BankTransactions
                    : [];
                  if (!pageItems.length) {
                    btEmpty = true;
                    return;
                  }

                  const { items: limitedItems, done } = applyExtractLimit(
                    pageItems,
                    extractLimit,
                    btCounter,
                  );

                  bankTxItems.push(...limitedItems);
                  tenantBankTxFetched += limitedItems.length;
                  bankTxFetched = tenantBankTxFetched;
                  heartbeat(
                    `Tenant ${i + 1}/${selectedTenantIds.length}: fetching bank transactions… (${tenantBankTxFetched} so far, page ${tenantBankTxPagesFetched})`,
                    {
                      tenantId,
                      currentTenantIndex: i + 1,
                      bankTxPagesFetched: tenantBankTxPagesFetched,
                      bankTxFetched: tenantBankTxFetched,
                    },
                    "info",
                  );

                  if (done) btEmpty = true;
                },
                {
                  startPage: 1,
                  hasMoreFn: () => !btEmpty,
                },
              );
            }
          } catch (e) {
            bankTxFetchFailedCount++;
            const sc = getErrStatusCode(e);
            const meta = getHttpErrorMeta(e);

            logger?.warn?.("PTRS v2 Xero bank transactions fetch failed", {
              action: "PtrsV2XeroFetchBankTransactionsFailed",
              customerId,
              ptrsId,
              xeroTenantId: tenantId,
              error: e?.message,
              statusCode: sc,
              ...meta,
            });

            updateStatus(customerId, ptrsId, {
              status: "RUNNING",
              message:
                "Bank transactions could not be fetched from Xero. Continuing…",
              progress: {
                extractedCount: counter.count,
                insertedCount,
                lastTenantId: tenantId,
                bankTxFetchFailedCount,
                bankTxFetchFailedSample,
              },
            });
          }

          const bankPersist = await persistBankTransactions({
            customerId,
            ptrsId,
            tenantId,
            bankTransactions: bankTxItems,
            fetchedAt: importStartedAt,
          });

          bankTxInserted += bankPersist.inserted || 0;

          updateStatus(customerId, ptrsId, {
            status: "RUNNING",
            message: `Organisation ${i + 1} complete: saved ${payPersist.inserted || 0} payments, ${invPersist.inserted || 0} invoices, ${contactPersist.inserted || 0} contacts, ${bankPersist.inserted || 0} bank transactions.`,
            progress: {
              extractedCount: counter.count,
              insertedCount,
              lastTenantId: tenantId,
              invoiceFetchFailedCount,
              invoiceFetchFailedSample,
              contactFetchFailedCount,
              contactFetchFailedSample,
              bankTxFetchFailedCount,
              bankTxFetchFailedSample,
            },
          });

          // If we’ve hit the global cap, stop early.
          if (extractLimit && counter.count >= extractLimit) break;
        }

        heartbeat(
          "All tenants processed. Building PTRS dataset from cached Xero records…",
          {
            tenantCount: selectedTenantIds.length,
            extractedCount: counter.count,
            paymentsInserted,
            invoicesInserted,
            contactsInserted,
            bankTxInserted,
          },
          "info",
        );
        updateStatus(customerId, ptrsId, {
          status: "RUNNING",
          message: "Building PTRS dataset from cached Xero records…",
          progress: {
            extractedCount: counter.count,
            insertedCount,
            paymentsInserted,
            invoicesInserted,
            contactsInserted,
            bankTxInserted,
          },
        });

        const rawBuild = await buildRawDatasetFromXeroCache({
          customerId,
          ptrsId,
          tenantIds: selectedTenantIds,
          importStartedAt,
          limit: extractLimit || null,
        });

        heartbeat(
          `PTRS raw dataset built (${rawBuild?.insertedRows ?? 0} rows). Creating main dataset record…`,
          { insertedRows: rawBuild?.insertedRows ?? 0 },
          "info",
        );

        // Ensure the standard PTRS flow can proceed: create/update a "main" dataset row
        // even when the main input was Xero (i.e. no uploaded CSV file).
        const PtrsDataset = getModel("PtrsDataset");
        if (PtrsDataset) {
          await withCustomerTxn(customerId, async (t) => {
            const candidate = {
              customerId,
              ptrsId,
              role: "main",
              fileName: "Xero import",
              storageRef: null,
              rowsCount: rawBuild?.insertedRows ?? null,
              status: "uploaded",
              meta: {
                source: "xero",
                selectedTenantIds: selectedTenantIds,
                extractLimit: extractLimit ?? null,
                importedAt: importStartedAt?.toISOString?.()
                  ? importStartedAt.toISOString()
                  : new Date().toISOString(),
              },
              createdBy: userId || null,
              updatedBy: userId || null,
            };

            const row = pickModelFields(PtrsDataset, candidate);

            // Prefer upsert; otherwise emulate it (role is expected to be unique per ptrsId).
            if (typeof PtrsDataset.upsert === "function") {
              await PtrsDataset.upsert(row, { transaction: t });
              return;
            }

            const existing = await PtrsDataset.findOne({
              where: { customerId, ptrsId, role: "main" },
              transaction: t,
            });

            if (existing) {
              await existing.update(row, { transaction: t });
            } else {
              await PtrsDataset.create(row, { transaction: t });
            }
          });
        } else {
          logger?.warn?.(
            "PtrsDataset model not loaded; cannot create main dataset for Xero run",
            {
              action: "PtrsV2XeroMainDatasetMissing",
              customerId,
              ptrsId,
            },
          );
        }

        updateStatus(customerId, ptrsId, {
          status: "COMPLETE",
          message: `Xero import completed. Cached ${paymentsInserted} payments, ${invoicesInserted} invoices, ${contactsInserted} contacts, ${bankTxInserted} bank transactions. Built ${rawBuild.insertedRows} PTRS raw rows.`,
          progress: {
            extractedCount: counter.count,
            insertedCount,
            paymentsInserted,
            invoicesInserted,
            contactsInserted,
            bankTxInserted,
            rawRowsInserted: rawBuild.insertedRows,
            invoiceFetchFailedCount,
            invoiceFetchFailedSample,
            contactFetchFailedCount,
            contactFetchFailedSample,
            bankTxFetchFailedCount,
            bankTxFetchFailedSample,
          },
        });
        // --- Socket push (MVP): notify subscribed clients of status changes ---
        try {
          const io = global.__socketio;
          if (io && ptrsId) {
            const room = `ptrs:${ptrsId}`;
            io.to(room).emit("ptrs:xeroImportStatus", {
              ptrsId,
              status: "COMPLETE",
              message: `Xero import completed. Cached ${paymentsInserted} payments, ${invoicesInserted} invoices, ${contactsInserted} contacts, ${bankTxInserted} bank transactions. Built ${rawBuild.insertedRows} PTRS raw rows.`,
              progress: {
                extractedCount: counter.count,
                insertedCount,
                paymentsInserted,
                invoicesInserted,
                contactsInserted,
                bankTxInserted,
                rawRowsInserted: rawBuild.insertedRows,
                invoiceFetchFailedCount,
                invoiceFetchFailedSample,
                contactFetchFailedCount,
                contactFetchFailedSample,
                bankTxFetchFailedCount,
                bankTxFetchFailedSample,
              },
              updatedAt: new Date().toISOString(),
            });
          }
        } catch (_) {
          // Never let socket emission break the import.
        }
      } catch (err) {
        const sc = err?.statusCode || err?.response?.status || null;

        let hint = "";
        if (sc === 401 || sc === 403) {
          hint =
            " Authorisation failed. Try reconnecting to Xero and re-selecting your organisation.";
        } else if (sc === 429) {
          hint =
            " Rate limited by Xero. Reduce polling and try again in a minute.";
        }

        const body = err?.responseBody;
        const bodyStr =
          body && typeof body === "object"
            ? JSON.stringify(body)
            : body
              ? String(body)
              : "";

        const extra = bodyStr ? ` Details: ${bodyStr.slice(0, 500)}` : "";

        updateStatus(customerId, ptrsId, {
          status: "FAILED",
          message: `${err?.message || "Xero import failed."}${hint}${extra}`,
          progress: { extractedCount: counter.count, insertedCount },
        });

        const phaseTimer = ({
          customerId,
          ptrsId,
          tenantId,
          phase,
          extra = {},
        }) => {
          const start = Date.now();
          return {
            end: (result = "ok", more = {}) => {
              const tookMs = Date.now() - start;
              logger?.info?.("PTRS Xero phase timing", {
                action: "PtrsV2XeroPhaseTiming",
                customerId,
                ptrsId,
                tenantId,
                phase,
                result,
                tookMs,
                ...extra,
                ...more,
              });
              return tookMs;
            },
          };
        };

        if (tenantTmr && typeof tenantTmr.end === "function") {
          safeEndTenantTimer("ok", {
            paymentsPagesFetched,
            paymentsFetched,
            invoicesFetched,
            contactsFetched,
            bankTxPagesFetched,
            bankTxFetched,
            extractLimit,
            extractedCount: counter.count,
            insertedCount,
            invoiceFetchFailedCount,
            contactFetchFailedCount,
            bankTxFetchFailedCount,
          });
        }

        const meta = getHttpErrorMeta(err);

        async function recordImportException({
          customerId,
          ptrsId,
          importRunId,
          source,
          phase,
          severity = "error",
          statusCode,
          method,
          url,
          message,
          xeroTenantId,
          invoiceId,
          responseBody,
          meta,
        }) {
          const PtrsImportException = getModel("PtrsImportException");
          if (!PtrsImportException) return null;

          const payload = {
            customerId,
            ptrsId,
            importRunId,
            source,
            phase,
            severity,
            statusCode,
            method,
            url,
            message,
            xeroTenantId,
            invoiceId,
            responseBody,
            meta,
          };

          const row = pickModelFields(PtrsImportException, payload);

          try {
            return await withCustomerTxn(customerId, async (t) =>
              PtrsImportException.create(row, { transaction: t }),
            );
          } catch (e) {
            logger?.warn?.("PTRS v2 import exception persist failed", {
              action: "PtrsV2ImportExceptionPersistFailed",
              customerId,
              ptrsId,
              importRunId,
              source,
              phase,
              statusCode,
              invoiceId,
              xeroTenantId,
              error: e?.message,
            });
            return null;
          }
        }

        logger?.error?.("PTRS v2 Xero import failed", {
          action: "PtrsV2XeroImportFailed",
          customerId,
          ptrsId,
          error: err?.message,
          statusCode: sc,
          url: err?.url || null,
          method: err?.method || null,
          responseBody: err?.responseBody || null,
          meta,
        });
      }
    } catch (err) {
      logger?.error?.("PTRS v2 Xero import runner crashed", {
        action: "PtrsV2XeroImportRunnerCrash",
        customerId,
        ptrsId,
        error: err?.message,
        stack: err?.stack,
      });

      updateStatus(customerId, ptrsId, {
        status: "FAILED",
        message: err?.message || "Import failed unexpectedly",
      });
    }
  });

  // Return the seeded RUNNING status immediately.
  return seed;
}

async function getStatus({ customerId, ptrsId }) {
  const key = statusKey(customerId, ptrsId);

  const current = statusStore.get(key);

  if (current) {
    return {
      ptrsId: current.ptrsId,
      status: current.status,
      message: current.message || null,
      progress: {
        ...(current.progress || {}),
        tenantCount:
          current.progress && typeof current.progress.tenantCount === "number"
            ? current.progress.tenantCount
            : getSelectedTenantIds(customerId, ptrsId).length,
      },
      updatedAt: current.updatedAt,
    };
  }

  return {
    ptrsId,
    status: "NOT_STARTED",
    message: null,
    progress: null,
    updatedAt: new Date().toISOString(),
  };
}

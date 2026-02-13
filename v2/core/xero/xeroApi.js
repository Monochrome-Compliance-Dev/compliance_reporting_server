const axios = require("axios");
const { logger } = require("@/helpers/logger");
require("dotenv").config();

function getXeroLogSlowMs() {
  const raw = process.env.XERO_LOG_SLOW_MS;
  if (raw === undefined || raw === null || raw === "") return 2000;
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 ? Math.floor(n) : 2000;
}

// -----------------------------
// Progress helpers
// -----------------------------
let xeroProcessedCount = 0;
let xeroStartTime = null;
let xeroTotalCount = null;

function formatEta(ms) {
  if (!Number.isFinite(ms)) return null;
  let s = Math.round(ms / 1000);
  const h = Math.floor(s / 3600);
  s %= 3600;
  const m = Math.floor(s / 60);
  const sec = s % 60;
  return h
    ? `${h}h ${m}m ${sec < 10 ? "0" : ""}${sec}s`
    : `${m}m ${sec < 10 ? "0" : ""}${sec}s`;
}

function resetXeroProgress(total = null) {
  xeroProcessedCount = 0;
  xeroStartTime = null;
  xeroTotalCount = Number.isFinite(total) ? total : null;
}

function updateXeroProgress(method, url) {
  if (!xeroStartTime) xeroStartTime = Date.now();
  xeroProcessedCount++;
  const elapsed = Date.now() - xeroStartTime;
  const avg = xeroProcessedCount > 0 ? elapsed / xeroProcessedCount : 0;
  const eta = xeroTotalCount
    ? (xeroTotalCount - xeroProcessedCount) * avg
    : null;

  const payload = {
    type: "xero-progress",
    method,
    endpoint: url,
    current: xeroProcessedCount,
    total: xeroTotalCount,
    eta: eta ? formatEta(eta) : null,
    timestamp: new Date().toISOString(),
  };

  if (xeroQueue) {
    payload.queue = { size: xeroQueue.size, pending: xeroQueue.pending };
  }

  if (global.sendWebSocketUpdate) {
    try {
      global.sendWebSocketUpdate(payload);
    } catch (_) {
      // no-op: websocket issues should never break API calls
    }
  }
}

// -----------------------------
// Queue init with timeout
// -----------------------------
let xeroQueue;
const queueReady = (async () => {
  const { default: PQueue } = await import("p-queue");
  // Concurrency guard ONLY. Rate limiting/backoff handled centrally in xeroApiUtils.
  xeroQueue = new PQueue({ concurrency: 1 });
  return xeroQueue;
})();

async function ensureQueue(timeoutMs = 5000) {
  const start = Date.now();
  // Await initialisation attempt (does not throw synchronously)
  try {
    await queueReady;
  } catch (e) {
    /* if import failed, we handle below by timeout */
  }
  while (!xeroQueue) {
    if (Date.now() - start > timeoutMs) {
      throw new Error("PQueue failed to initialise within timeout");
    }
    await new Promise((res) => setTimeout(res, 10));
  }
  return xeroQueue;
}

// -----------------------------
// Normalisers & config helpers
// -----------------------------
function normalizeAccessToken(input) {
  if (!input) return "";
  if (typeof input === "string") return input;
  return input.access_token || input.accessToken || "";
}

function normalizeTenantId(input) {
  if (!input) return "";
  if (typeof input === "string") return input;
  return input.tenantId || "";
}

function resolveBaseURL(url) {
  const isAbsolute = /^https?:\/\//i.test(url);
  if (isAbsolute) return undefined; // axios will use the absolute URL
  if (url === "/connections" || url.startsWith("/connections/")) {
    return "https://api.xero.com";
  }
  return process.env.XERO_API_BASE_URL;
}

// -----------------------------
// Core request wrapper
// -----------------------------
async function request(
  method,
  url,
  { data, accessToken, tenantId, config = {} } = {},
) {
  await ensureQueue();
  const at = normalizeAccessToken(accessToken);
  const tid = normalizeTenantId(tenantId);

  updateXeroProgress(method, url);

  const headers = {
    ...(config.headers || {}),
    ...(at && { Authorization: `Bearer ${at}` }),
    ...(tid && { "Xero-tenant-id": tid }),
    Accept: "application/json",
  };
  if (data && !headers["Content-Type"])
    headers["Content-Type"] = "application/json";

  const baseURL = resolveBaseURL(url);

  const run = () => {
    const started = Date.now();
    return xeroQueue
      .add(() =>
        axios({
          method,
          url,
          data,
          ...(baseURL && { baseURL }),
          headers,
          ...config,
        }),
      )
      .then((res) => {
        const tookMs = Date.now() - started;
        const slowMs = getXeroLogSlowMs();
        if (tookMs >= slowMs) {
          logger?.warn?.("Slow Xero HTTP request", {
            action: "XeroApiSlowRequest",
            method,
            url,
            tookMs,
            status: res?.status,
            queue: xeroQueue
              ? { size: xeroQueue.size, pending: xeroQueue.pending }
              : null,
          });
        }
        return res;
      });
  };

  try {
    const res = await run();
    return { data: res.data, headers: res.headers, status: res.status };
  } catch (error) {
    // Enrich error context for easier debugging
    const statusCode = error?.response?.status ?? error?.statusCode ?? null;

    error.context = {
      url,
      method,
      status: statusCode,
      statusText: error?.response?.statusText,
      xeroCorrelationId: error?.response?.headers?.["xero-correlation-id"],
    };

    error.responseBody = error?.response?.data;
    throw error;
  }
}

// -----------------------------
// Public API
// -----------------------------
const get = (url, accessToken, tenantId, config = {}) =>
  request("GET", url, { accessToken, tenantId, config });

const post = (url, data, accessToken, tenantId, config = {}) =>
  request("POST", url, { data, accessToken, tenantId, config });

const del = (url, accessToken, tenantId, config = {}) =>
  request("DELETE", url, { accessToken, tenantId, config });

module.exports = { get, post, del, resetXeroProgress };

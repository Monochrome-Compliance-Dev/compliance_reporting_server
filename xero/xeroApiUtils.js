/**
 * Utility functions for Xero API calls - retry, pagination, header handling, rate-limiting, and error handling
 */

const xeroService = require("./xero.service");

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------
const MIN_REMAINING_SOFT_THROTTLE = 60; // When remaining > this, we don't soft-throttle
const MAX_BACKOFF_MS = 30000; // cap for exponential backoff
const BASE_BACKOFF_MS = 1000; // base delay for backoff

// -----------------------------------------------------------------------------
// Exported API
// -----------------------------------------------------------------------------
module.exports = {
  rateLimitHandler,
  extractErrorDetails,
  retryWithExponentialBackoff,
  paginateXeroApi,
  prepareHeaders,
  handle500Error,
  handleXeroApiError,
  callXeroApiWithAutoRefresh,
  handleXeroRateLimitWarnings,
};

// -----------------------------------------------------------------------------
// Normalisers & header prep
// -----------------------------------------------------------------------------
function normalizeAccessToken(token) {
  if (!token) return "";
  if (typeof token === "string") return token;
  return token.access_token || token.accessToken || "";
}

function normalizeTenantId(t) {
  if (!t) return "";
  if (typeof t === "string") return t;
  return t.tenantId || "";
}

function prepareHeaders(accessToken, tenantId) {
  const at = normalizeAccessToken(accessToken);
  const tid = normalizeTenantId(tenantId);
  return {
    ...(at && { Authorization: `Bearer ${at}` }),
    ...(tid && { "Xero-tenant-id": tid }),
    "Content-Type": "application/json",
    Accept: "application/json",
  };
}

// -----------------------------------------------------------------------------
// Error helpers
// -----------------------------------------------------------------------------
function extractErrorDetails(error) {
  if (!error) return "Unknown error";
  return error.response?.data || error.message || JSON.stringify(error);
}

function enrichXeroError(err, extra = {}) {
  const res = err?.response;
  err.context = {
    statusCode: res?.status || err.statusCode || 500,
    statusText: res?.statusText,
    xeroCorrelationId: res?.headers?.["xero-correlation-id"],
    ...extra,
    timestamp: new Date().toISOString(),
  };
  err.responseBody = res?.data;
  return err;
}

function handle500Error(error) {
  // Keep for backwards-compat usage in callers that want to fail fast
  throw enrichXeroError(error, {
    critical: true,
    message: extractErrorDetails(error),
  });
}

function handleXeroApiError(error) {
  throw enrichXeroError(error, { errorDetails: extractErrorDetails(error) });
}

// -----------------------------------------------------------------------------
// Rate limit & backoff helpers
// -----------------------------------------------------------------------------
async function rateLimitHandler(headers) {
  const retryAfter = Number(headers?.["retry-after"]) || 60;
  await new Promise((res) => setTimeout(res, retryAfter * 1000));
}

function backoff(attempt, base = BASE_BACKOFF_MS, cap = MAX_BACKOFF_MS) {
  const exp = Math.min(cap, base * 2 ** attempt);
  const jitter = Math.random() * 0.3 * exp; // -30% jitter
  return Math.round(exp - jitter);
}

function calculateWaitTimeMs(remaining, resetEpoch) {
  const r = Number(remaining);
  const reset = Number(resetEpoch);
  if (!Number.isFinite(r) || !Number.isFinite(reset)) return 0;
  const resetMs = reset > 1e10 ? reset : reset * 1000; // epoch sec → ms
  const now = Date.now();
  const windowMs = Math.max(resetMs - now, 1000);
  if (r > MIN_REMAINING_SOFT_THROTTLE) return 0;
  return Math.ceil(windowMs / Math.max(r, 1));
}

async function handleXeroRateLimitWarnings(headers = {}) {
  const remaining = parseInt(headers["x-minlimit-remaining"], 10);
  const reset = parseInt(headers["x-rate-limit-reset"], 10);
  const waitTime = calculateWaitTimeMs(remaining, reset);
  if (waitTime > 0) {
    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        status: "warning",
        message: `[Xero API] Throttling: waiting ${waitTime}ms (remaining: ${remaining})`,
        timestamp: new Date().toISOString(),
      });
    }
    await new Promise((res) => setTimeout(res, waitTime));
  }
}

// -----------------------------------------------------------------------------
// Retry wrapper with exponential backoff + jitter
// -----------------------------------------------------------------------------
async function retryWithExponentialBackoff(
  fn,
  retries = 3,
  baseDelay = BASE_BACKOFF_MS
) {
  let attempt = 0;
  while (attempt < retries) {
    try {
      // Optional preflight hook to inspect/imitate rate limit headers
      const simulatedHeaders =
        typeof fn.getRateLimitHeaders === "function"
          ? await fn.getRateLimitHeaders()
          : null;
      if (simulatedHeaders) {
        await handleXeroRateLimitWarnings(simulatedHeaders);
        const remaining = parseInt(
          simulatedHeaders["x-minlimit-remaining"],
          10
        );
        if (Number.isFinite(remaining) && remaining <= 3) {
          const waitMs = 1000 * (6 - remaining);
          await new Promise((res) => setTimeout(res, waitMs));
        }
      }

      const response = await fn();
      const { data, headers, status } = response;
      await handleXeroRateLimitWarnings(headers || {});
      return { data, headers, status };
    } catch (error) {
      const statusCode = error?.response?.status || error.statusCode || null;

      // Handle explicit 429 with Retry-After
      if (statusCode === 429) {
        await rateLimitHandler(error?.response?.headers || {});
      } else if (statusCode === 500) {
        // Treat 500s as transient unless it's last attempt
        if (attempt === retries - 1) {
          throw enrichXeroError(error);
        }
      } else if (attempt === retries - 1) {
        // Out of retries; rethrow enriched
        throw enrichXeroError(error);
      }

      const delay = Math.min(
        backoff(attempt, baseDelay, MAX_BACKOFF_MS),
        MAX_BACKOFF_MS
      );
      if (global.sendWebSocketUpdate) {
        global.sendWebSocketUpdate({
          status: "warn",
          message: `Xero API retry (${attempt + 1}): ${error.message}`,
          retryDelay: `${Math.round(delay)}ms`,
          timestamp: new Date().toISOString(),
        });
      }
      await new Promise((res) => setTimeout(res, delay));
      attempt++;
    }
  }
}

// -----------------------------------------------------------------------------
// Pagination helper (configurable)
// -----------------------------------------------------------------------------
async function paginateXeroApi(fetchPageFn, processPageFn, options = {}) {
  const pageSize = options.pageSize ?? 100;
  const hasMoreFn =
    options.hasMoreFn ??
    ((res) => Array.isArray(res?.data) && res.data.length === pageSize);

  let page = options.startPage ?? 1;
  let hasMore = true;

  while (hasMore) {
    try {
      const response = await retryWithExponentialBackoff(() =>
        fetchPageFn(page)
      );
      await handleXeroRateLimitWarnings(response?.headers || {});
      await processPageFn(response, page);
      hasMore = hasMoreFn(response, page);
      page++;
    } catch (error) {
      throw enrichXeroError(error, {
        phase: "pagination",
        page,
        details: extractErrorDetails(error),
      });
    }
  }
}

// -----------------------------------------------------------------------------
// Auto-refresh wrapper
// -----------------------------------------------------------------------------
/**
 * Wrap an API call so that a 401/403 triggers a token refresh and a single retry.
 *
 * Usage patterns supported:
 * 1) callXeroApiWithAutoRefresh(() => apiCall(argsBuiltWithFreshToken), clientId)
 * 2) callXeroApiWithAutoRefresh(apiCallFn, clientId, ...args) // legacy: may reuse stale token
 */
async function callXeroApiWithAutoRefresh(apiCallOrFactory, clientId, ...args) {
  const exec = () =>
    args.length ? apiCallOrFactory(...args) : apiCallOrFactory();
  try {
    const response = await exec();
    await handleXeroRateLimitWarnings(response?.headers || {});
    return response;
  } catch (error) {
    const statusCode = error?.response?.status || error.statusCode || 500;
    if (statusCode === 401 || statusCode === 403) {
      await xeroService.refreshToken(clientId);
      // If caller passed a factory, it should now read the new token
      return await exec();
    }
    throw enrichXeroError(error);
  }
}

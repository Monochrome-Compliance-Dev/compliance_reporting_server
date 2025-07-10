/**
 * Utility functions for Xero API calls - retry, pagination, header handling, rate-limiting, and error handling
 */

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

async function rateLimitHandler(headers) {
  const retryAfter = headers?.["retry-after"] || 60;
  await new Promise((res) => setTimeout(res, retryAfter * 1000));
}

function extractErrorDetails(error) {
  if (!error) return "Unknown error";
  return error.response?.data || error.message || JSON.stringify(error);
}

function handle500Error(error) {
  error.context = {
    critical: true,
    message: extractErrorDetails(error),
    timestamp: new Date().toISOString(),
  };
  throw error;
}

function handleXeroApiError(error) {
  const statusCode = error.response?.status || error.statusCode || 500;
  error.context = {
    statusCode,
    errorDetails: extractErrorDetails(error),
    timestamp: new Date().toISOString(),
  };
  throw error;
}

async function retryWithExponentialBackoff(fn, retries = 3, baseDelay = 1000) {
  let attempt = 0;
  while (attempt < retries) {
    try {
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
        if (!isNaN(remaining) && remaining <= 3) {
          const waitMs = 1000 * (6 - remaining);
          await new Promise((res) => setTimeout(res, waitMs));
        }
      }

      const response = await fn();
      const { data, headers, status } = response;
      await handleXeroRateLimitWarnings(headers);
      return { data, headers, status };
    } catch (error) {
      const statusCode = error.response?.status || error.statusCode || null;
      if (statusCode === 429) {
        await rateLimitHandler(error.response?.headers);
      } else if (statusCode === 500) {
        handle500Error(error);
      } else if (attempt === retries - 1) {
        throw error;
      } else {
        const delay = Math.min(baseDelay * Math.pow(2, attempt), 60000);
        if (global.sendWebSocketUpdate) {
          global.sendWebSocketUpdate({
            status: "warn",
            message: `Xero API retry (${attempt + 1}): ${error.message}`,
            retryDelay: `${delay / 1000}s`,
            timestamp: new Date().toISOString(),
          });
        }
        await new Promise((res) => setTimeout(res, delay));
      }
      attempt++;
    }
  }
}

async function paginateXeroApi(fetchPageFn, processPageFn) {
  let page = 1;
  let hasMore = true;

  while (hasMore) {
    try {
      const response = await retryWithExponentialBackoff(() =>
        fetchPageFn(page)
      );
      await handleXeroRateLimitWarnings(response?.headers);
      await processPageFn(response);
      hasMore = response?.data?.length > 0 && response.data.length === 100;
      page++;
    } catch (error) {
      error.context = {
        phase: "pagination",
        page,
        details: extractErrorDetails(error),
        timestamp: new Date().toISOString(),
      };
      throw error;
    }
  }
}

function prepareHeaders(accessToken, tenantId) {
  return {
    Authorization: `Bearer ${accessToken}`,
    "Xero-tenant-id": tenantId,
    "Content-Type": "application/json",
  };
}

const xeroService = require("./xero.service");

async function callXeroApiWithAutoRefresh(apiCallFn, clientId, ...args) {
  try {
    const response = await apiCallFn(...args);
    await handleXeroRateLimitWarnings(response?.headers);
    return response;
  } catch (error) {
    const statusCode = error.response?.status || error.statusCode || 500;
    if (statusCode === 401 || statusCode === 403) {
      await xeroService.refreshToken(clientId);
      return await apiCallFn(...args);
    }
    throw error;
  }
}

function calculateWaitTime(remaining, reset) {
  if (isNaN(remaining) || isNaN(reset)) return 0;
  const now = Date.now();
  const resetTime = reset > 1000000000 ? reset * 1000 : reset;
  const timeUntilReset = Math.max(resetTime - now, 1000);
  if (remaining > 60) return 0;
  return Math.ceil(timeUntilReset / (remaining || 1));
}

async function handleXeroRateLimitWarnings(headers) {
  const remaining = parseInt(headers?.["x-minlimit-remaining"], 10);
  const reset = parseInt(headers?.["x-rate-limit-reset"], 10);
  const waitTime = calculateWaitTime(remaining, reset);
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

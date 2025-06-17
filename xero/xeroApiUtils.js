/**
 * Utility functions for Xero API calls - retry, pagination, header handling, rate-limiting, and error handling
 */

module.exports = {
  rateLimitHandler,
  extractErrorDetails,
  logApiCall,
  retryWithExponentialBackoff,
  paginateXeroApi,
  prepareHeaders,
  handle500Error,
  handleXeroApiError, // newly added export
  callXeroApiWithAutoRefresh,
  handleXeroRateLimitWarnings,
};

/**
 * Handles Xero API rate limits by pausing execution based on the Retry-After header.
 * @param {Object} headers - Response headers from the Xero API.
 */
async function rateLimitHandler(headers) {
  const retryAfter = headers?.["retry-after"] || 60; // default to 60 seconds
  console.warn(`Rate limit hit. Pausing for ${retryAfter} seconds...`);
  await new Promise((res) => setTimeout(res, retryAfter * 1000));
}

/**
 * Extracts useful error details for better debugging.
 * @param {Error} error
 * @returns {string|Object}
 */
function extractErrorDetails(error) {
  if (!error) return "Unknown error";
  return error.response?.data || error.message || JSON.stringify(error);
}

/**
 * Logs API calls for audit and debugging purposes.
 * @param {string} endpoint
 * @param {string} [method='GET']
 * @param {string} [status='SUCCESS']
 */
function logApiCall(endpoint, method = "GET", status = "SUCCESS") {
  console.log(`[Xero API] ${method} ${endpoint} - Status: ${status}`);
}

/**
 * Handle 500 errors explicitly: log and return error details for service-level handling.
 * @param {Error} error
 * @returns {Object} error details for further handling
 */
function handle500Error(error) {
  const errorDetails = extractErrorDetails(error);
  console.error("❌ Critical 500 error:", errorDetails);
  return {
    status: "error",
    message: `Critical 500 error: ${errorDetails}`,
    code: 500,
  };
}

function handleXeroApiError(error) {
  const statusCode = error.response?.status || error.statusCode || 500;
  const errorDetails = extractErrorDetails(error);

  if (statusCode === 500) {
    console.error("❌ 500 Internal Server Error:", errorDetails);
  } else if (statusCode === 429) {
    console.warn("⚠️ Rate limit reached:", errorDetails);
  } else {
    console.error(`❌ HTTP ${statusCode} error:`, errorDetails);
  }

  if (global.sendWebSocketUpdate) {
    global.sendWebSocketUpdate({
      status: "error",
      message: `Error fetching data from Xero: ${errorDetails}`,
      code: statusCode,
    });
  }
}

/**
 * Retries a function with exponential backoff and Xero API rate-limit handling.
 * @param {Function} fn - The function to execute.
 * @param {number} [retries=3] - Number of retry attempts.
 * @param {number} [baseDelay=1000] - Initial delay in ms.
 */
async function retryWithExponentialBackoff(fn, retries = 3, baseDelay = 1000) {
  let attempt = 0;
  while (attempt < retries) {
    try {
      // Throttle proactively before making the API call if possible
      const simulatedHeaders =
        typeof fn.getRateLimitHeaders === "function"
          ? await fn.getRateLimitHeaders()
          : null;
      if (simulatedHeaders) {
        console.log("🔄 [Backoff] Simulated headers:", simulatedHeaders);
        await handleXeroRateLimitWarnings(simulatedHeaders);
      }

      const response = await fn();

      console.log("Full response headers:", response.headers);

      if (response && response.headers) {
        console.log("--- Rate Limit Headers ---");
        if (typeof response.headers.forEach === "function") {
          response.headers.forEach((value, key) => {
            console.log(`${key}: ${value}`);
          });
        } else if (typeof response.headers === "object") {
          // For Axios-style headers object
          Object.entries(response.headers).forEach(([key, value]) => {
            console.log(`${key}: ${value}`);
          });
        } else {
          console.log("Cannot iterate headers");
        }
        console.log("--------------------------");
      } else {
        console.log("No headers found on response");
      }

      console.log(
        "✅ [Backoff] Response headers after call:",
        response?.headers
      );
      // Double-check rate limits after response
      await handleXeroRateLimitWarnings(response?.headers);
      return response;
    } catch (error) {
      const statusCode = error.response?.status || error.statusCode || null;
      if (statusCode === 429) {
        // Handle rate limit (429 Too Many Requests)
        await rateLimitHandler(error.response?.headers);
      } else if (statusCode === 500) {
        handle500Error(error);
      } else if (attempt === retries - 1) {
        // Last attempt, rethrow
        throw error;
      } else {
        // Non-rate-limit error, backoff before retry
        const delay = baseDelay * Math.pow(2, attempt);
        console.warn(
          `Retrying after ${delay}ms due to error:`,
          extractErrorDetails(error)
        );
        await new Promise((res) => setTimeout(res, delay));
      }
      attempt++;
    }
  }
}

/**
 * Paginates through Xero API endpoints, handling rate limits.
 * @param {Function} fetchPageFn - Function to fetch a page, takes page number.
 * @param {Function} processPageFn - Function to process the API response.
 */
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
      // Xero's pagination: assume 100 per page, so if less than 100, we're done
      hasMore = response?.data?.length > 0 && response.data.length === 100;
      page++;
    } catch (error) {
      console.error("Pagination error:", extractErrorDetails(error));
      throw error;
    }
  }
}

/**
 * Prepares headers for Xero API requests.
 * @param {string} accessToken
 * @param {string} tenantId
 * @returns {Object}
 */
function prepareHeaders(accessToken, tenantId) {
  return {
    Authorization: `Bearer ${accessToken}`,
    "Xero-tenant-id": tenantId,
    "Content-Type": "application/json",
  };
}

const xeroService = require("./xero.service"); // adjust path as needed

/**
 * Calls an Xero API function and automatically refreshes the token if needed.
 * @param {Function} apiCallFn - The Xero API call function (should return a Promise).
 * @param {string} clientId - Client identifier for the refresh token function.
 * @param  {...any} args - Additional arguments to pass to the API call function.
 */
async function callXeroApiWithAutoRefresh(apiCallFn, clientId, ...args) {
  try {
    const response = await apiCallFn(...args);
    await handleXeroRateLimitWarnings(response?.headers);
    return response;
  } catch (error) {
    const statusCode = error.response?.status || error.statusCode || 500;
    if (statusCode === 401 || statusCode === 403) {
      console.log("Token expired. Refreshing and retrying...");
      await xeroService.refreshToken(clientId);
      return await apiCallFn(...args);
    }
    throw error;
  }
}

/**
 * Dynamically calculates wait time to avoid hitting the Xero API rate limit.
 * @param {number} remaining - The number of calls left.
 * @param {number} reset - Epoch time (ms or s) when the rate limit resets.
 * @returns {number} delay in milliseconds
 */
function calculateWaitTime(remaining, reset) {
  if (isNaN(remaining) || isNaN(reset)) return 0;

  const now = Date.now();
  const resetTime = reset > 1000000000 ? reset * 1000 : reset; // handles ms vs s
  const timeUntilReset = Math.max(resetTime - now, 1000); // ensure at least 1s buffer

  // Assume 60 calls remaining is safe, less than that needs spreading
  if (remaining > 60) return 0;

  // Spread remaining calls evenly over the time until reset
  const delayPerCall = Math.ceil(timeUntilReset / (remaining || 1));
  return delayPerCall;
}

/**
 * Proactively handles Xero rate limit warnings based on headers.
 * @param {Object} headers - Response headers from the Xero API.
 */
async function handleXeroRateLimitWarnings(headers) {
  console.log("🔍 [RateLimit] Headers received:", headers);
  const remaining = parseInt(headers?.["x-rate-limit-remaining"], 10);
  const reset = parseInt(headers?.["x-rate-limit-reset"], 10);

  const waitTime = calculateWaitTime(remaining, reset);
  if (waitTime > 0) {
    const msg = `[Xero API] Throttling to avoid rate limit: waiting ${waitTime}ms (remaining: ${remaining})`;
    console.warn(msg);
    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        status: "warning",
        message: msg,
        timestamp: new Date().toISOString(),
      });
    }
    await new Promise((res) => setTimeout(res, waitTime));
  }
}

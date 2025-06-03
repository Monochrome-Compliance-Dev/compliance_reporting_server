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
      return await fn();
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

const axios = require("axios");
require("dotenv").config();

const formatEta = (ms) => {
  if (!ms || isNaN(ms)) return null;
  const totalSeconds = Math.round(ms / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}m ${seconds < 10 ? "0" : ""}${seconds}s`;
};

let xeroQueue;
(async () => {
  const { default: PQueue } = await import("p-queue");
  xeroQueue = new PQueue({
    concurrency: 5,
    interval: 60000,
    intervalCap: 55, // leave some buffer to avoid 429s
  });
})();

let xeroProcessedCount = 0;
let xeroStartTime = null;
let xeroTotalCount = null; // optional, can be set externally if needed

// Wrapper function for GET requests
/**
 * Make a GET request to the Xero API with dynamic accessToken and tenantId.
 * @param {string} url - The endpoint (e.g. /Invoices)
 * @param {string} accessToken - OAuth2 access token
 * @param {string} tenantId - Xero tenant id
 * @param {object} config - Additional axios config (optional)
 */
const get = async (url, accessToken, tenantId, config = {}) => {
  if (typeof accessToken !== "string") {
    console.warn("accessToken is not a string. Fixing it now.");
    accessToken = accessToken?.accessToken || "";
  }
  if (typeof tenantId !== "string") {
    console.warn("tenantId is not a string. Fixing it now.");
    tenantId = tenantId?.tenantId || "";
  }

  try {
    while (!xeroQueue) await new Promise((res) => setTimeout(res, 10));
    if (!xeroStartTime) xeroStartTime = Date.now();
    xeroProcessedCount++;
    const elapsed = Date.now() - xeroStartTime;
    const avgTimePerItem = elapsed / xeroProcessedCount;
    const eta = xeroTotalCount
      ? (xeroTotalCount - xeroProcessedCount) * avgTimePerItem
      : null;

    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        status: "info",
        message: `Xero API request ${xeroProcessedCount}${xeroTotalCount ? ` of ${xeroTotalCount}` : ""}`,
        eta: eta ? formatEta(eta) : null,
        current: xeroProcessedCount,
        total: xeroTotalCount,
        timestamp: new Date().toISOString(),
      });
    }

    const headers = {
      ...(config.headers || {}),
      Authorization: `Bearer ${accessToken}`,
      "Xero-tenant-id": tenantId,
      Accept: "application/json",
    };
    let baseURL;
    if (url === "/connections") {
      baseURL = "https://api.xero.com";
    } else {
      baseURL = process.env.XERO_API_BASE_URL;
    }
    const response = await xeroQueue.add(() =>
      axios.get(url, {
        ...config,
        baseURL,
        headers,
      })
    );
    return {
      data: response.data,
      headers: response.headers,
      status: response.status,
    };
  } catch (error) {
    console.error(`Error GET ${url}:`, error.response?.data || error.message);
    throw error;
  }
};

/**
 * Make a POST request to the Xero API with dynamic accessToken and tenantId.
 * @param {string} url - The endpoint or absolute URL
 * @param {*} data - Body data
 * @param {string} accessToken - OAuth2 access token
 * @param {string} tenantId - Xero tenant id
 * @param {object} config - Additional axios config (optional)
 */
const post = async (url, data, accessToken, tenantId, config = {}) => {
  if (typeof accessToken !== "string") {
    console.warn("accessToken is not a string. Fixing it now.");
    accessToken = accessToken?.accessToken || "";
  }
  try {
    while (!xeroQueue) await new Promise((res) => setTimeout(res, 10));
    if (!xeroStartTime) xeroStartTime = Date.now();
    xeroProcessedCount++;
    const elapsed = Date.now() - xeroStartTime;
    const avgTimePerItem = elapsed / xeroProcessedCount;
    const eta = xeroTotalCount
      ? (xeroTotalCount - xeroProcessedCount) * avgTimePerItem
      : null;

    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        status: "info",
        message: `Xero API request ${xeroProcessedCount}${xeroTotalCount ? ` of ${xeroTotalCount}` : ""}`,
        eta: eta ? formatEta(eta) : null,
        current: xeroProcessedCount,
        total: xeroTotalCount,
        timestamp: new Date().toISOString(),
      });
    }

    // For token endpoint, accessToken and tenantId may be undefined, so only set if provided
    const headers = {
      ...(config.headers || {}),
      ...(accessToken && { Authorization: `Bearer ${accessToken}` }),
      ...(tenantId && { "Xero-tenant-id": tenantId }),
      Accept: "application/json",
    };
    // If url is absolute (token endpoint), baseURL is not set
    const isAbsolute = /^https?:\/\//i.test(url);
    const baseURL = isAbsolute ? undefined : process.env.XERO_API_BASE_URL;
    const response = await xeroQueue.add(() =>
      axios.post(url, data, {
        ...config,
        ...(baseURL && { baseURL }),
        headers,
      })
    );
    return {
      data: response.data,
      headers: response.headers,
      status: response.status,
    };
  } catch (error) {
    console.error(`Error POST ${url}:`, error.response?.data || error.message);
    throw error;
  }
};

/**
 * Make a DELETE request to the Xero API with dynamic accessToken and tenantId.
 * @param {string} url - The endpoint (e.g. /connections/:id)
 * @param {string} accessToken - OAuth2 access token
 * @param {string} tenantId - Xero tenant id (optional for /connections)
 * @param {object} config - Additional axios config (optional)
 */
const del = async (url, accessToken, tenantId, config = {}) => {
  if (typeof accessToken !== "string") {
    console.warn("accessToken is not a string. Fixing it now.");
    accessToken = accessToken?.accessToken || "";
  }
  if (tenantId && typeof tenantId !== "string") {
    tenantId = tenantId?.tenantId || "";
  }
  try {
    while (!xeroQueue) await new Promise((res) => setTimeout(res, 10));
    if (!xeroStartTime) xeroStartTime = Date.now();
    xeroProcessedCount++;
    const elapsed = Date.now() - xeroStartTime;
    const avgTimePerItem = elapsed / xeroProcessedCount;
    const eta = xeroTotalCount
      ? (xeroTotalCount - xeroProcessedCount) * avgTimePerItem
      : null;

    if (global.sendWebSocketUpdate) {
      global.sendWebSocketUpdate({
        status: "info",
        message: `Xero API DELETE request ${xeroProcessedCount}${xeroTotalCount ? ` of ${xeroTotalCount}` : ""}`,
        eta: eta ? formatEta(eta) : null,
        current: xeroProcessedCount,
        total: xeroTotalCount,
        timestamp: new Date().toISOString(),
      });
    }

    const headers = {
      ...(config.headers || {}),
      Authorization: `Bearer ${accessToken}`,
      ...(tenantId && { "Xero-tenant-id": tenantId }),
      Accept: "application/json",
    };
    let baseURL;
    if (url === "/connections" || url.startsWith("/connections/")) {
      baseURL = "https://api.xero.com";
    } else {
      baseURL = process.env.XERO_API_BASE_URL;
    }
    const response = await xeroQueue.add(() =>
      axios.delete(url, {
        ...config,
        baseURL,
        headers,
      })
    );
    return {
      data: response.data,
      headers: response.headers,
      status: response.status,
    };
  } catch (error) {
    console.error(
      `Error DELETE ${url}:`,
      error.response?.data || error.message
    );
    throw error;
  }
};

// Export for reuse
module.exports = { get, post, del };

const axios = require("axios");
require("dotenv").config();

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
    const response = await axios.get(url, {
      ...config,
      baseURL,
      headers,
    });
    return response.data;
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
    const response = await axios.post(url, data, {
      ...config,
      ...(baseURL && { baseURL }),
      headers,
    });
    return response.data;
  } catch (error) {
    console.error(`Error POST ${url}:`, error.response?.data || error.message);
    throw error;
  }
};

// Export for reuse
module.exports = { get, post };

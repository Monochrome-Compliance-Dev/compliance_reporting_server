const axios = require("axios");
require("dotenv").config();

const xeroApi = axios.create({
  baseURL: process.env.XERO_API_BASE_URL,
  headers: {
    Authorization: `Bearer ${process.env.XERO_ACCESS_TOKEN}`,
    "Xero-tenant-id": process.env.XERO_TENANT_ID,
    Accept: "application/json",
  },
});

// Wrapper function for GET requests
const get = async (url) => {
  try {
    const response = await xeroApi.get(url);
    return response.data;
  } catch (error) {
    console.error(`Error GET ${url}:`, error.response?.data || error.message);
    throw error;
  }
};

// Export for reuse
module.exports = { get };

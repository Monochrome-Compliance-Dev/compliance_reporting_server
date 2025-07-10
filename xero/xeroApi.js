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
    intervalCap: 55,
  });
})();

let xeroProcessedCount = 0;
let xeroStartTime = null;
let xeroTotalCount = null;

function updateXeroProgress(method, url) {
  if (!xeroStartTime) xeroStartTime = Date.now();
  xeroProcessedCount++;
  const elapsed = Date.now() - xeroStartTime;
  const avgTimePerItem = elapsed / xeroProcessedCount;
  const eta = xeroTotalCount
    ? (xeroTotalCount - xeroProcessedCount) * avgTimePerItem
    : null;

  if (global.sendWebSocketUpdate) {
    global.sendWebSocketUpdate({
      type: "xero-progress",
      method,
      endpoint: url,
      current: xeroProcessedCount,
      total: xeroTotalCount,
      eta: eta ? formatEta(eta) : null,
      timestamp: new Date().toISOString(),
    });
  }
}

const get = async (url, accessToken, tenantId, config = {}) => {
  if (typeof accessToken !== "string") {
    accessToken = accessToken?.accessToken || "";
  }
  if (typeof tenantId !== "string") {
    tenantId = tenantId?.tenantId || "";
  }

  try {
    while (!xeroQueue) await new Promise((res) => setTimeout(res, 10));
    updateXeroProgress("GET", url);

    const headers = {
      ...(config.headers || {}),
      Authorization: `Bearer ${accessToken}`,
      "Xero-tenant-id": tenantId,
      Accept: "application/json",
    };
    const baseURL =
      url === "/connections"
        ? "https://api.xero.com"
        : process.env.XERO_API_BASE_URL;

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
    error.context = { url, method: "GET" };
    throw error;
  }
};

const post = async (url, data, accessToken, tenantId, config = {}) => {
  if (typeof accessToken !== "string") {
    accessToken = accessToken?.accessToken || "";
  }

  try {
    while (!xeroQueue) await new Promise((res) => setTimeout(res, 10));
    updateXeroProgress("POST", url);

    const headers = {
      ...(config.headers || {}),
      ...(accessToken && { Authorization: `Bearer ${accessToken}` }),
      ...(tenantId && { "Xero-tenant-id": tenantId }),
      Accept: "application/json",
    };
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
    error.context = { url, method: "POST" };
    throw error;
  }
};

const del = async (url, accessToken, tenantId, config = {}) => {
  if (typeof accessToken !== "string") {
    accessToken = accessToken?.accessToken || "";
  }
  if (tenantId && typeof tenantId !== "string") {
    tenantId = tenantId?.tenantId || "";
  }

  try {
    while (!xeroQueue) await new Promise((res) => setTimeout(res, 10));
    updateXeroProgress("DELETE", url);

    const headers = {
      ...(config.headers || {}),
      Authorization: `Bearer ${accessToken}`,
      ...(tenantId && { "Xero-tenant-id": tenantId }),
      Accept: "application/json",
    };
    const baseURL =
      url === "/connections" || url.startsWith("/connections/")
        ? "https://api.xero.com"
        : process.env.XERO_API_BASE_URL;

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
    error.context = { url, method: "DELETE" };
    throw error;
  }
};

module.exports = { get, post, del };

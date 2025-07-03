// data_cleanse.service.js
const { lookupAbnByName } = require("./abn-lookup.util"); // You can rename this from abn-backend.js later

/**
 * Lookup ABN candidates for a given supplier name.
 * @param {string} name - Supplier name to search.
 * @returns {Promise<Object[]>} - Array of ABN match objects (even if empty).
 */
async function getAbnCandidatesForName(name) {
  if (!name || !name.trim()) {
    throw new Error("Supplier name is required");
  }

  const results = await lookupAbnByName(name);

  return results;
}

module.exports = {
  getAbnCandidatesForName,
};

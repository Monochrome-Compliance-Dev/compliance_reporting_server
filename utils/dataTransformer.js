/**
 * Generic data transformation utility.
 * @param {Object} rawData - Raw data from external system (Xero, MYOB, etc.).
 * @param {Object} fieldMapping - Object mapping DB field names to raw field mappings or functions.
 * @returns {Object} Transformed object ready for DB insertion.
 */
function transformData(rawData, fieldMapping) {
  const transformed = {};
  for (const dbField of Object.keys(fieldMapping)) {
    if (dbField === "createdAt" || dbField === "updatedAt") {
      transformed[dbField] = new Date().toISOString();
    } else {
      const mapping = fieldMapping[dbField];
      transformed[dbField] =
        typeof mapping === "function" ? mapping(rawData) : rawData[mapping];
    }
  }
  return transformed;
}

module.exports = { transformData };

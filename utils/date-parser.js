function parseXeroDate(xeroDate, fallback) {
  const match = /\/Date\((\d+)(?:[+-]\d+)?\)\//.exec(xeroDate);
  if (match) return new Date(parseInt(match[1], 10));
  return fallback ? new Date(fallback) : null;
}

module.exports = {
  parseXeroDate,
};

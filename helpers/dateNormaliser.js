// Excel epoch starts 1899-12-30; 25569 = 1970-01-01
const EXCEL_EPOCH_DAYS = 25569;
const MS_PER_DAY = 86400 * 1000;

function parseDateLike(value) {
  if (value == null || value === "") return null;

  // If it looks like a pure Excel serial (number or numeric string)
  if (
    (typeof value === "number" && Number.isFinite(value)) ||
    (/^\d{4,6}$/.test(value) && Number.isFinite(Number(value)))
  ) {
    const serial = Number(value);
    // Guard against accidentally small/huge numbers that aren’t dates
    if (serial > 30000 && serial < 60000) {
      const ms = (serial - EXCEL_EPOCH_DAYS) * MS_PER_DAY;
      return new Date(ms).toISOString(); // UTC ISO
    }
  }

  // Try common string formats (dd/mm/yyyy, yyyy-mm-dd, ISO, etc.)
  // Keep it simple and safe:
  const d = new Date(value);
  if (!isNaN(d.getTime())) return d.toISOString();

  // Couldn’t parse; return null so we don’t insert junk
  return null;
}

module.exports = { parseDateLike };

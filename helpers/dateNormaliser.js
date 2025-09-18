// Excel epoch starts 1899-12-30; 25569 = 1970-01-01
const EXCEL_EPOCH_DAYS = 25569;
const MS_PER_DAY = 86400 * 1000;

function pad2(n) {
  return String(n).padStart(2, "0");
}

function monthFromShortName(m) {
  const map = {
    jan: 1,
    feb: 2,
    mar: 3,
    apr: 4,
    may: 5,
    jun: 6,
    jul: 7,
    aug: 8,
    sep: 9,
    oct: 10,
    nov: 11,
    dec: 12,
  };
  const key = String(m || "")
    .slice(0, 3)
    .toLowerCase();
  return map[key] || null;
}

/**
 * Parse a variety of date-like inputs and return an ISO 8601 string in UTC.
 * Returns null if the value cannot be parsed safely.
 */
function parseDateLike(value) {
  if (value == null || value === "") return null;

  // Numeric Excel serial (e.g., 45123). Accept only a safe window.
  if (
    (typeof value === "number" && Number.isFinite(value)) ||
    (/^\d{4,6}$/.test(value) && Number.isFinite(Number(value)))
  ) {
    const serial = Number(value);
    if (serial > 30000 && serial < 60000) {
      const ms = (serial - EXCEL_EPOCH_DAYS) * MS_PER_DAY;
      return new Date(ms).toISOString(); // UTC ISO
    }
  }

  const s = String(value).trim();

  // ISO date or datetime → trust native parser
  if (/^\d{4}-\d{2}-\d{2}(?:[ T].*)?$/.test(s)) {
    const d = new Date(s);
    return isNaN(d.getTime()) ? null : d.toISOString();
  }

  // dd/mm/yyyy or dd-mm-yyyy
  let m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
  if (m) {
    const [, dd, MM, yyyy] = m;
    const dNum = Number(dd);
    const mNum = Number(MM);
    const yNum = Number(yyyy);
    if (mNum >= 1 && mNum <= 12 && dNum >= 1 && dNum <= 31) {
      // construct as UTC midnight
      return `${yNum}-${pad2(mNum)}-${pad2(dNum)}T00:00:00.000Z`;
    }
  }

  // dd Mon yyyy (e.g., 26 Aug 2024) or dd-Mon-YYYY
  m = s.match(/^(\d{1,2})[\s\-]([A-Za-z]{3,})[\s\-](\d{4})$/);
  if (m) {
    const [, dd, mon, yyyy] = m;
    const mNum = monthFromShortName(mon);
    const dNum = Number(dd);
    const yNum = Number(yyyy);
    if (mNum && dNum >= 1 && dNum <= 31) {
      return `${yNum}-${pad2(mNum)}-${pad2(dNum)}T00:00:00.000Z`;
    }
  }

  // yyyymmdd (compact)
  m = s.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (m) {
    const [, yyyy, MM, dd] = m;
    return `${yyyy}-${MM}-${dd}T00:00:00.000Z`;
  }

  // Last resort: native Date (handles some locale strings safely)
  const d = new Date(s);
  if (!isNaN(d.getTime())) return d.toISOString();

  // Couldn’t parse; return null so we don’t insert junk
  return null;
}

module.exports = { parseDateLike };

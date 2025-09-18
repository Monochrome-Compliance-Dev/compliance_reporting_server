// helpers/amountNormaliser.js
// Normalize amount-like strings while preserving numeric semantics.
// Goal: make values acceptable to Postgres numeric casts and your SQL regex
// without forcing a specific scale/precision.

// Notes:
// - Keep leading minus sign if present; convert accounting parentheses to minus
// - Strip currency symbols and whitespace (incl. NBSP)
// - Drop thousands separators (commas and thin spaces)
// - Do NOT coerce to a fixed decimal; return a cleaned string or null

function normalizeAmountLike(input) {
  if (input == null) return null;
  let s = String(input).trim();
  if (!s) return null;

  // Replace common unicode minus with ASCII hyphen
  s = s.replace(/[\u2212\u2012\u2013\u2014]/g, "-");

  // Detect accounting parentheses, e.g. (1,234.56)
  let negative = false;
  const mParens = s.match(/^\(\s*(.+?)\s*\)$/);
  if (mParens) {
    negative = true;
    s = mParens[1];
  }

  // Remove currency markers and alpha codes: $, A$, AUD, etc.
  s = s.replace(/\bA?UD\b/gi, "");
  s = s.replace(/[£€$¥₤₩₹]/g, "");

  // Remove spaces (incl NBSP/thin)
  s = s.replace(/[\s\u00A0\u2007\u202F]/g, "");

  // If there are both commas and dots, assume commas are thousand separators → drop commas
  if (/,/.test(s) && /\./.test(s)) {
    s = s.replace(/,/g, "");
  } else {
    // If only commas exist, and pattern looks like 1,234,567 or 1,234,567.89 → drop commas
    if (/^[-+]?\d{1,3}(,\d{3})+(\.\d+)?$/.test(s)) {
      s = s.replace(/,/g, "");
    }
  }

  // Remove plus sign if any
  s = s.replace(/^\+/, "");

  // Final sanity: allow only digits, optional single dot, and optional leading minus
  // If we still have stray characters (e.g., commas in uncommon formats), bail out
  if (!/^[-]?\d*(?:\.\d+)?$/.test(s)) {
    return null; // caller can fall back to original to route to error table
  }

  if (negative && !s.startsWith("-")) s = "-" + s;
  if (s === "" || s === "-") return null;
  return s;
}

module.exports = { normalizeAmountLike };

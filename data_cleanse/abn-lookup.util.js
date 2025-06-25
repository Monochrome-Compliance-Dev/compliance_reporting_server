// If NODE_ENV is already set (like by AWS), do not overwrite it
process.env.NODE_ENV = process.env.NODE_ENV || "development";

// Load .env.development only in development; other envs use AWS-injected vars
const dotenv = require("dotenv");
const fs = require("fs");
if (process.env.NODE_ENV === "development") {
  dotenv.config({ path: "../.env.development" });
}

const GUID = process.env.ABR_GUID;

const messyPatterns = [/C\/-/, /LIQUIDATOR/, /AS TRUSTEE/, /TRUSTEE/i];

const isCleanName = (name) =>
  !messyPatterns.some((pattern) => pattern.test(name));

const getStatusLabel = (code) => {
  if (code === "0000000001") return "Active";
  if (code === "0000000002") return "Cancelled";
  return code;
};

const getConfidenceAndComment = (searchTerm, topMatches, selected) => {
  const totalMatches = topMatches.length;
  const exactNameMatches = topMatches.filter(
    (a) => a.Name.trim().toLowerCase() === searchTerm.trim().toLowerCase()
  ).length;

  // Simple heuristics:
  if (totalMatches > 6) {
    return {
      confidence: "Low",
      comment: `Multiple matches (${totalMatches}) with the same score. Common name, hard to isolate. Recommend requesting supplier documentation.`,
    };
  }

  if (exactNameMatches === 1) {
    return {
      confidence: "High",
      comment: `Only one exact match found. Name appears to be unique.`,
    };
  }

  if (totalMatches <= 3) {
    return {
      confidence: "High",
      comment: `Few matches with same score. Selection is likely reliable.`,
    };
  }

  if (isCleanName(selected.Name)) {
    return {
      confidence: "Medium",
      comment: `Selected best clean match among ${totalMatches} candidates.`,
    };
  }

  return {
    confidence: "Low",
    comment: `Best match is ambiguous or includes messy terms. Recommend verifying with supplier documentation.`,
  };
};

const extractBestActiveMatch = (rawResponse, searchTerm) => {
  try {
    const data = JSON.parse(rawResponse.replace(/^callback\((.*)\);?$/, "$1"));
    if (!data.Names || !Array.isArray(data.Names)) return null;

    const active = data.Names.filter(
      (entry) => entry.AbnStatus === "0000000001"
    );
    if (active.length === 0) return null;

    const highestScore = Math.max(...active.map((a) => a.Score));
    const topMatches = active.filter((a) => a.Score === highestScore);
    const preferred =
      topMatches.find((a) => isCleanName(a.Name)) || topMatches[0];

    const { confidence, comment } = getConfidenceAndComment(
      searchTerm,
      topMatches,
      preferred
    );

    return {
      "Search Term": searchTerm,
      Name: preferred.Name,
      "Suggested ABN": preferred.Abn,
      Postcode: preferred.Postcode,
      State: preferred.State,
      "ABN Status": getStatusLabel(preferred.AbnStatus),
      "Confidence Level": confidence,
      Comments: comment,
    };
  } catch (err) {
    console.error(`Error for ${searchTerm}:`, err.message);
    return null;
  }
};

async function lookupAbnByName(name) {
  const url = `https://abr.business.gov.au/json/MatchingNames.aspx?name=${encodeURIComponent(name)}&guid=${GUID}`;
  const res = await fetch(url);
  const text = await res.text();

  const match = extractBestActiveMatch(text, name);
  return match ? [match] : [];
}

module.exports = { lookupAbnByName };

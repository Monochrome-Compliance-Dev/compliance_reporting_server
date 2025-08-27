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

const extractAllCandidates = (rawResponse, searchTerm) => {
  try {
    const data = JSON.parse(rawResponse.replace(/^callback\((.*)\);?$/, "$1"));
    if (!data.Names || !Array.isArray(data.Names)) return [];

    const all = Array.isArray(data.Names) ? data.Names : [];
    if (all.length === 0) return [];

    const highestScore = Math.max(...all.map((a) => a.Score));
    const topMatches = all.filter((a) => a.Score === highestScore);

    const candidates = all.map((cand) => {
      const { confidence, comment } = getConfidenceAndComment(
        searchTerm,
        topMatches,
        cand
      );
      return {
        "Search Term": searchTerm,
        Name: cand.Name,
        "Suggested ABN": cand.Abn,
        Postcode: cand.Postcode,
        State: cand.State,
        "ABN Status": getStatusLabel(cand.AbnStatus),
        Score: cand.Score,
        "Is Top Score": cand.Score === highestScore,
        "Confidence Level": confidence,
        Comments: comment,
      };
    });

    // Sort by score (desc) then name
    candidates.sort(
      (a, b) => b.Score - a.Score || a.Name.localeCompare(b.Name)
    );
    return candidates;
  } catch (err) {
    console.error(`Error for ${searchTerm}:`, err.message);
    return [];
  }
};

async function lookupAbnByName(name) {
  const url = `https://abr.business.gov.au/json/MatchingNames.aspx?name=${encodeURIComponent(name)}&guid=${GUID}`;
  const res = await fetch(url);
  const text = await res.text();

  const matches = extractAllCandidates(text, name);
  return matches;
}

module.exports = { lookupAbnByName };

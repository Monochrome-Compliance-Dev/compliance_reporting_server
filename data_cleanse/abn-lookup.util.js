const messyPatterns = [/C\/-/, /LIQUIDATOR/, /AS TRUSTEE/, /TRUSTEE/i];

const GOVERNMENT_ENTITY_TYPE_CODES = new Set([
  "GOV",
  "FGA",
  "FGD",
  "FSA",
  "CCB",
  "CCC",
  "CCL",
  "CCN",
  "CCO",
  "CCP",
  "CCR",
  "CCS",
  "CCT",
  "CCU",
  "CGA",
  "CGC",
  "CGE",
  "CGP",
  "CGS",
  "CGT",
  "CSA",
  "CSP",
  "CSS",
  "CTC",
  "CTD",
  "CTF",
  "CTH",
  "CTI",
  "CTL",
  "CTQ",
  "CTT",
  "CTU",
  "LOC",
  "LCB",
  "LCC",
  "LCL",
  "LCN",
  "LCO",
  "LCP",
  "LCR",
  "LCS",
  "LCT",
  "LCU",
  "LGA",
  "LGC",
  "LGE",
  "LGP",
  "LGT",
  "LSA",
  "LSP",
  "LSS",
  "LTC",
  "LTD",
  "LTF",
  "LTH",
  "LTI",
  "LTL",
  "LTQ",
  "LTT",
  "LTU",
  "STA",
  "SCB",
  "SCC",
  "SCL",
  "SCN",
  "SCO",
  "SCP",
  "SCR",
  "SCS",
  "SCT",
  "SCU",
  "SGA",
  "SGC",
  "SGE",
  "SGP",
  "SGT",
  "SSA",
  "SSP",
  "SSS",
  "STC",
  "STD",
  "STF",
  "STH",
  "STI",
  "STL",
  "STQ",
  "STT",
  "STU",
  "TER",
  "TCB",
  "TCC",
  "TCL",
  "TCN",
  "TCO",
  "TCP",
  "TCR",
  "TCS",
  "TCT",
  "TCU",
  "TGA",
  "TGC",
  "TGE",
  "TGP",
  "TGS",
  "TGT",
  "TSA",
  "TSP",
  "TSS",
  "TTC",
  "TTD",
  "TTF",
  "TTH",
  "TTI",
  "TTL",
  "TTQ",
  "TTT",
  "TTU",
]);

function isValidAbn(value) {
  const abn = normalizeAbnDigits(value);
  if (!/^\d{11}$/.test(abn)) return false;

  const weights = [10, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19];
  const digits = abn.split("").map(Number);
  digits[0] -= 1;

  return (
    digits.reduce(
      (total, digit, index) => total + digit * weights[index],
      0,
    ) % 89 ===
    0
  );
}

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
  const url = `https://abr.business.gov.au/json/MatchingNames.aspx?name=${encodeURIComponent(name)}&guid=${process.env.ABR_GUID}`;
  const res = await fetch(url);
  const text = await res.text();

  const matches = extractAllCandidates(text, name);
  return matches;
}

function extractTagValue(xml, tag) {
  if (!xml || !tag) return null;
  const escapedTag = String(tag).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = xml.match(
    new RegExp(`<${escapedTag}(?:\\s[^>]*)?>([\\s\\S]*?)</${escapedTag}>`, "i"),
  );
  if (!match) return null;
  return match[1]
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .trim();
}

function extractSection(xml, tag) {
  if (!xml || !tag) return null;
  const escapedTag = String(tag).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = xml.match(
    new RegExp(`<${escapedTag}(?:\\s[^>]*)?>([\\s\\S]*?)</${escapedTag}>`, "i"),
  );
  return match ? match[1] : null;
}

function normalizeAbnDigits(abn) {
  return String(abn || "").replace(/\D/g, "");
}

function buildLegalName(section) {
  if (!section) return null;
  const parts = [
    extractTagValue(section, "givenName"),
    extractTagValue(section, "otherGivenName"),
    extractTagValue(section, "familyName"),
  ].filter(Boolean);
  return parts.length ? parts.join(" ") : null;
}

function classifyGovernmentEntityType(entityTypeCode) {
  const code = String(entityTypeCode || "").trim().toUpperCase();
  return GOVERNMENT_ENTITY_TYPE_CODES.has(code);
}

function parseExactAbnLookupResponse(xml) {
  const businessEntity = extractSection(xml, "businessEntity");
  const exception = extractSection(xml, "exception");

  if (exception) {
    return {
      found: false,
      exception: extractTagValue(exception, "exceptionDescription") || null,
    };
  }

  if (!businessEntity) {
    return {
      found: false,
      exception: "ABR response did not contain a businessEntity",
    };
  }

  const abnSection = extractSection(businessEntity, "ABN");
  const entityTypeSection = extractSection(businessEntity, "entityType");
  const entityStatusSection = extractSection(businessEntity, "entityStatus");
  const mainNameSection = extractSection(businessEntity, "mainName");
  const businessNameSection = extractSection(businessEntity, "businessName");
  const legalNameSection = extractSection(businessEntity, "legalName");

  const abn = normalizeAbnDigits(extractTagValue(abnSection, "identifierValue"));
  const entityTypeCode = extractTagValue(entityTypeSection, "entityTypeCode");
  const entityTypeDescription = extractTagValue(
    entityTypeSection,
    "entityDescription",
  );
  const name =
    extractTagValue(mainNameSection, "organisationName") ||
    extractTagValue(businessNameSection, "organisationName") ||
    buildLegalName(legalNameSection);

  return {
    found: Boolean(abn),
    abn,
    isCurrentAbn: extractTagValue(abnSection, "isCurrentIndicator") === "Y",
    entityStatusCode: extractTagValue(entityStatusSection, "entityStatusCode"),
    entityTypeCode,
    entityTypeDescription,
    name,
    isGovernmentEntity: classifyGovernmentEntityType(entityTypeCode),
  };
}

async function lookupAbnByNumber(abn, { includeHistoricalDetails = "N" } = {}) {
  const normalizedAbn = normalizeAbnDigits(abn);
  if (!isValidAbn(normalizedAbn)) {
    throw new Error("A valid ABN is required");
  }
  const guid = process.env.ABR_GUID;
  if (!guid) {
    throw new Error("ABR_GUID is required for exact ABN lookup");
  }

  const url =
    "https://abr.business.gov.au/ABRXMLSearch/AbrXmlSearch.asmx/ABRSearchByABN" +
    `?searchString=${encodeURIComponent(normalizedAbn)}` +
    `&includeHistoricalDetails=${encodeURIComponent(includeHistoricalDetails)}` +
    `&authenticationGuid=${encodeURIComponent(guid)}`;

  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`ABR exact ABN lookup failed with status ${res.status}`);
  }

  const xml = await res.text();
  const parsed = parseExactAbnLookupResponse(xml);
  return {
    ...parsed,
    requestAbn: normalizedAbn,
  };
}

module.exports = {
  classifyGovernmentEntityType,
  isValidAbn,
  lookupAbnByName,
  lookupAbnByNumber,
  normalizeAbnDigits,
  parseExactAbnLookupResponse,
};

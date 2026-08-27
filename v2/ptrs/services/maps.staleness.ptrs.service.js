const { buildStableInputHash } = require("@/v2/ptrs/services/ptrs.service");

function normHeaderKey(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "")
    .trim();
}

function buildMapMetaFromMappings(
  mappings,
  signature = null,
  updatedAtIso = null,
) {
  const m =
    mappings && typeof mappings === "object" && !Array.isArray(mappings)
      ? mappings
      : {};
  const sourceHeaders = Object.keys(m);
  const sourceHeadersNorm = sourceHeaders.map(normHeaderKey).filter(Boolean);

  const targets = Array.from(
    new Set(
      Object.values(m)
        .map((cfg) => {
          if (cfg == null) return null;
          if (typeof cfg === "string") return cfg;
          return cfg?.field || null;
        })
        .filter((v) => v != null && String(v).trim() !== "")
        .map((v) => String(v).trim()),
    ),
  );

  return {
    version: 1,
    sourceHeaders,
    sourceHeadersNorm,
    targets,
    updatedAt: updatedAtIso || null,
    signature: signature || null,
  };
}

function safeParseJsonObject(v) {
  if (v == null) return null;
  if (typeof v === "object" && !Array.isArray(v)) return v;
  if (typeof v === "string") {
    try {
      const parsed = JSON.parse(v);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed;
      }
    } catch (_) {
      return null;
    }
  }
  return null;
}

function extractMapMetaFromExtras(extras) {
  const obj = safeParseJsonObject(extras) || {};
  const meta = obj?.mapMeta;
  if (!meta || typeof meta !== "object") return null;
  if (meta.version !== 1) return null;
  return meta;
}

function safeParseJsonAny(v) {
  if (v == null) return null;
  if (typeof v === "string") {
    try {
      return JSON.parse(v);
    } catch {
      return v;
    }
  }
  return v;
}

function buildMaterialMapSignature({ mappings, joins, customFields }) {
  return buildStableInputHash({
    mappings: safeParseJsonAny(mappings) || null,
    joins: safeParseJsonAny(joins) || null,
    customFields: safeParseJsonAny(customFields) || null,
  });
}

module.exports = {
  normHeaderKey,
  buildMapMetaFromMappings,
  safeParseJsonObject,
  extractMapMetaFromExtras,
  safeParseJsonAny,
  buildMaterialMapSignature,
};

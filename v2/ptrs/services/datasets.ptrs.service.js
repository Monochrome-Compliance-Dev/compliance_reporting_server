const DATASET_PURPOSES = Object.freeze({
  TRANSACTION: "transaction",
  REFERENCE: "reference",
});

const REFERENCE_KINDS = Object.freeze([
  "vendormaster",
  "termschanges",
  "entitystructure",
  "invoices",
  "other",
]);

const SOURCE_FORMATS = Object.freeze(["csv", "xlsx", "api"]);

function getAdapterContract(adapterType) {
  // Resolve lazily because ptrs.service -> data.ptrs.service -> this module is
  // part of the normal upload dependency chain.
  return require("./canonical.adapters.ptrs.service").getCanonicalAdapterContract(
    adapterType,
  );
}

const XERO_TRANSACTION_DATASET = Object.freeze({
  role: "transaction",
  purpose: "transaction",
  sourceFormat: "api",
  adapterType: "xero_accounting_event",
  sourceType: "xero",
});

function normaliseToken(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function normaliseReferenceKind(value) {
  const compact = normaliseToken(value).replace(/[^a-z0-9]/g, "");
  const aliases = {
    vendormaster: "vendormaster",
    termschanges: "termschanges",
    paymenttermchanges: "termschanges",
    entitystructure: "entitystructure",
    entitymaster: "entitystructure",
    invoices: "invoices",
    invoice: "invoices",
    other: "other",
  };
  return aliases[compact] || null;
}

function validateDatasetClassification({
  purpose,
  sourceFormat,
  referenceKind = null,
  adapterType = null,
  adapterVersion = null,
  sourceGroupScope = null,
}) {
  const normalisedPurpose = normaliseToken(purpose);
  const normalisedSourceFormat = normaliseToken(sourceFormat);

  if (!Object.values(DATASET_PURPOSES).includes(normalisedPurpose)) {
    const error = new Error("purpose must be transaction or reference");
    error.statusCode = 400;
    throw error;
  }
  if (!SOURCE_FORMATS.includes(normalisedSourceFormat)) {
    const error = new Error("sourceFormat must be csv, xlsx or api");
    error.statusCode = 400;
    throw error;
  }

  const normalisedReferenceKind = normaliseReferenceKind(referenceKind);
  if (
    normalisedPurpose === DATASET_PURPOSES.REFERENCE &&
    !normalisedReferenceKind
  ) {
    const error = new Error("referenceKind is required for reference datasets");
    error.statusCode = 400;
    throw error;
  }
  if (
    normalisedPurpose === DATASET_PURPOSES.TRANSACTION &&
    referenceKind != null &&
    String(referenceKind).trim() !== ""
  ) {
    const error = new Error(
      "referenceKind must be empty for transaction datasets",
    );
    error.statusCode = 400;
    throw error;
  }

  const normalisedAdapterType = normaliseToken(adapterType);
  let adapterContract = null;
  if (normalisedPurpose === DATASET_PURPOSES.TRANSACTION) {
    adapterContract = getAdapterContract(normalisedAdapterType);
    if (!adapterContract) {
      const error = new Error(
        "adapterType must identify a supported transaction adapter",
      );
      error.statusCode = 400;
      error.code = "UNSUPPORTED_CANONICAL_ADAPTER";
      throw error;
    }
    const requestedVersion = String(
      adapterVersion || adapterContract.defaultVersion,
    ).trim();
    if (!adapterContract.supportedVersions.includes(requestedVersion)) {
      const error = new Error(
        `Unsupported transaction adapter version: ${normalisedAdapterType}@${requestedVersion}`,
      );
      error.statusCode = 400;
      error.code = "UNSUPPORTED_CANONICAL_ADAPTER_VERSION";
      throw error;
    }
  }

  return {
    purpose: normalisedPurpose,
    sourceFormat: normalisedSourceFormat,
    referenceKind:
      normalisedPurpose === DATASET_PURPOSES.REFERENCE
        ? normalisedReferenceKind
        : null,
    adapterType: normalisedAdapterType || null,
    adapterVersion:
      normalisedPurpose === DATASET_PURPOSES.TRANSACTION
        ? String(adapterVersion || "").trim() ||
          adapterContract.defaultVersion
        : String(adapterVersion || "").trim() || null,
    sourceGroupScope: String(sourceGroupScope || "").trim() || null,
    role:
      normalisedPurpose === DATASET_PURPOSES.TRANSACTION
        ? DATASET_PURPOSES.TRANSACTION
        : normalisedReferenceKind,
  };
}

function isTransactionDataset(dataset) {
  return normaliseToken(dataset?.purpose) === DATASET_PURPOSES.TRANSACTION;
}

function isReferenceDataset(dataset) {
  return normaliseToken(dataset?.purpose) === DATASET_PURPOSES.REFERENCE;
}

module.exports = {
  DATASET_PURPOSES,
  REFERENCE_KINDS,
  SOURCE_FORMATS,
  XERO_TRANSACTION_DATASET,
  normaliseReferenceKind,
  validateDatasetClassification,
  isTransactionDataset,
  isReferenceDataset,
};

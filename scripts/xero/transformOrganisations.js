const { normaliseFakeAbns } = require("../../middleware/sanitise-numbers");

const transformOrganisations = (organisation) => {
  // Remove spaces and convert to number if possible
  const cleanNumber = (val) => {
    if (!val) return null;
    const numStr = String(val).replace(/\s+/g, "");
    return numStr && !isNaN(numStr) ? Number(numStr) : null;
  };

  const transform = (org) => ({
    ...org,
    payerEntityAbn: cleanNumber(org.RegistrationNumber),
    payerEntityAcnArbn: cleanNumber(normaliseFakeAbns(org.TaxNumber)),
    payerEntityName: org.Name || org.LegalName,
  });

  if (Array.isArray(organisation)) {
    return organisation.map(transform);
  }

  return transform(organisation);
};

module.exports = { transformOrganisations };

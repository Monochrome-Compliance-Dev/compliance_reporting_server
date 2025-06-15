const { normaliseFakeAbns } = require("../../middleware/sanitise-numbers");

const transformOrganisations = (organisation) => {
  const transformed = {};
  // console.log("Transforming organisation data...");

  // If organisation is an array, use the first entry
  const org = Array.isArray(organisation) ? organisation[0] : organisation;

  return {
    ...org,
    payerEntityAbn: normaliseFakeAbns(org.TaxNumber) || null,
    payerEntityAcnArbn: org.RegistrationNumber,
    payerEntityName: org.Name || org.LegalName,
  };
};

module.exports = { transformOrganisations };

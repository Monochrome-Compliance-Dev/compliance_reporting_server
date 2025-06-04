const transformOrganisation = (organisation) => {
  const transformed = {};
  console.log("Transforming organisation data...");

  // If organisation is an array, use the first entry
  const org = Array.isArray(organisation) ? organisation[0] : organisation;
  console.log("Organisation to transform:", JSON.stringify(org, null, 2));

  transformed.payerEntityAbn =
    org.TaxNumber || "PAYER_ENTITY_ABN_PLACEHOLDER_BOOOO";

  transformed.payerEntityAcnArbn =
    org.RegistrationNumber || "PAYER_ENTITY_ACN_ARBN_PLACEHOLDER";

  transformed.payerEntityName =
    org.Name || org.LegalName || "PAYER_ENTITY_NAME_PLACEHOLDER";

  return transformed;
};

module.exports = { transformOrganisation };

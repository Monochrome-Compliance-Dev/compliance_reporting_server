const transformOrganisation = (organisation) => {
  const transformed = {};

  // Use TaxNumber as the ABN
  transformed.payerEntityAbn =
    organisation.TaxNumber || "PAYER_ENTITY_ABN_PLACEHOLDER";

  // Use RegistrationNumber as the ACN/ARBN if available
  transformed.payerEntityAcnArbn =
    organisation.RegistrationNumber || "PAYER_ENTITY_ACN_ARBN_PLACEHOLDER";

  // Use Name or LegalName as the entity name
  transformed.payerEntityName =
    organisation.Name ||
    organisation.LegalName ||
    "PAYER_ENTITY_NAME_PLACEHOLDER";

  return transformed;
};

module.exports = { transformOrganisation };

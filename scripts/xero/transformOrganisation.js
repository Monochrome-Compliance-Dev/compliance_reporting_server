const transformOrganisation = (organisation) => {
  const transformed = {};
  console.log("Transforming organisation data...");

  // If organisation is an array, use the first entry
  const org = Array.isArray(organisation) ? organisation[0] : organisation;
  console.log("Organisation to transform:", JSON.stringify(org, null, 2));

  transformed.payerEntityAbn = org.TaxNumber;

  transformed.payerEntityAcnArbn = org.RegistrationNumber;

  transformed.payerEntityName = org.Name || org.LegalName;

  return transformed;
};

module.exports = { transformOrganisation };

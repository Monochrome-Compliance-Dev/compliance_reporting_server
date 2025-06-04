const transformContact = (contact) => {
  return {
    payeeEntityName: contact.Name || "NONE PROVIDED",
    payeeEntityAbn: contact.TaxNumber || "NONE PROVIDED",
    payeeEntityAcnArbn: contact.CompanyNumber || "NONE PROVIDED",
    contractPoPaymentTerms:
      contact.DAYSAFTERBILLDATE ||
      contact.DAYSAFTERBILLMONTH ||
      "NONE PROVIDED",
  };
};

module.exports = {
  transformContact,
};

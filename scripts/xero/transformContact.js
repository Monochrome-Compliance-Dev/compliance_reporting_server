const transformContact = (contact) => {
  return {
    payeeEntityName: contact.Name,
    payeeEntityAbn: contact.TaxNumber,
    payeeEntityAcnArbn: contact.CompanyNumber,
    contractPoPaymentTerms:
      contact.DAYSAFTERBILLDATE || contact.DAYSAFTERBILLMONTH,
  };
};

module.exports = {
  transformContact,
};

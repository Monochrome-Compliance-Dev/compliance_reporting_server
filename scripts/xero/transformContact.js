const transformContact = (contact) => {
  const c = contact?.Contact || contact;

  return {
    ...c,
    payeeEntityName: c?.Name || null,
    payeeEntityAbn: c?.TaxNumber || null,
    payeeEntityAcnArbn: c?.CompanyNumber || null,
    contractPoPaymentTerms:
      c?.DAYSAFTERBILLDATE || c?.DAYSAFTERBILLMONTH || null,
  };
};

module.exports = {
  transformContact,
};

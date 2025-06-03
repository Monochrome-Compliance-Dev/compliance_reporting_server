const transformContact = (contact) => {
  return {
    payeeEntityName: contact.Name || "PLACEHOLDER",
    payeeEntityAbn: contact.TaxNumber || "PLACEHOLDER",
    payeeEntityAcnArbn: contact.CompanyNumber || "PLACEHOLDER",
    contractPoPaymentTerms:
      contact.DaysAfterBillDate || contact.DaysAfterBillMonth || "PLACEHOLDER",
  };
};

module.exports = {
  transformContact,
};

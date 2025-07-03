const transformContact = (contact) => {
  const c = contact?.Contact || contact;

  // Remove spaces for ABN and ACN/ARBN and convert to number if possible
  const cleanNumber = (val) => {
    if (!val) return null;
    const numStr = String(val).replace(/\s+/g, "");
    return numStr && !isNaN(numStr) ? Number(numStr) : null;
  };

  // Extract contractPoPaymentTerms from PaymentTerms.Bills.Day if available
  let contractPoPaymentTerms = null;
  if (c?.PaymentTerms?.Bills && typeof c.PaymentTerms.Bills.Day === "number") {
    contractPoPaymentTerms = c.PaymentTerms.Bills.Day;
  }

  return {
    ...c,
    payeeEntityName: c?.Name || null,
    payeeEntityAbn: cleanNumber(c?.TaxNumber),
    payeeEntityAcnArbn: cleanNumber(c?.CompanyNumber),
    contractPoPaymentTerms:
      contractPoPaymentTerms !== null && contractPoPaymentTerms !== undefined
        ? String(contractPoPaymentTerms)
        : null,
  };
};

module.exports = {
  transformContact,
};

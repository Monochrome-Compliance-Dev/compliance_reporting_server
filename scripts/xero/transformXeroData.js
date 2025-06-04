// const xeroData = require("../../docs/xeroApiDump.json");
const { logger } = require("../../helpers/logger");
const { transformOrganisation } = require("./transformOrganisation");
const { transformContact } = require("./transformContact");
const { transformPayments } = require("./transformPayments");
const { transformInvoices } = require("./transformInvoices");

async function transformXeroData(xeroData) {
  logger.logEvent("info", "Starting Xero data transformation");
  console.log("========== RAW XERO DATA DUMP ==========");
  console.log(JSON.stringify(xeroData.payments.slice(0, 5), null, 2));
  console.log(JSON.stringify(xeroData.invoices.slice(0, 5), null, 2));
  console.log(JSON.stringify(xeroData.contacts.slice(0, 5), null, 2));
  console.log("========== END RAW XERO DATA DUMP ==========");

  const transformedOrganisation = transformOrganisation(xeroData.organisation);

  // Transform payments
  const transformedPayments = transformPayments(xeroData.payments);

  // Transform invoices
  const transformedInvoices = transformInvoices(xeroData.invoices);

  // Build a contact map for easy lookup
  const contactMap = {};
  if (Array.isArray(xeroData.contacts)) {
    xeroData.contacts.forEach((contact) => {
      contactMap[contact.ContactID] = transformContact(contact);
    });
  }

  // Combine transformed payments, invoices, and contacts
  const transformedData = transformedPayments.map((payment, index) => {
    const relatedContact = contactMap[payment.ContactID] || {};
    return {
      ...transformedOrganisation,
      ...payment,
      ...transformedInvoices[index], // Merge corresponding invoice data
      ...relatedContact, // Add transformed contact data
    };
  });

  console.log("========== TRANSFORMED DATA ==========");
  console.log(JSON.stringify(transformedData.slice(0, 5), null, 2));
  console.log("========== END TRANSFORMED DATA ==========");

  logger.logEvent("info", "Xero data transformation complete");
  return transformedData;
}

module.exports = { transformXeroData };

// transformXeroData(xeroData).then(() => console.log("Transform complete."));

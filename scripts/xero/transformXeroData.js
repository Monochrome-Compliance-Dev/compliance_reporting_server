const xeroData = require("../../docs/xeroApiDump.json");
const { logger } = require("../../helpers/logger");
const { transformOrganisation } = require("./transformOrganisation");
const { transformContact } = require("./transformContact");
const { transformInvoices } = require("./transformInvoices");

async function transformXeroData(xeroData) {
  logger.logEvent("info", "Starting Xero data transformation");
  console.log("========== RAW XERO DATA DUMP ==========");
  console.log(JSON.stringify(xeroData.invoices.slice(0, 5), null, 2));
  console.log("========== END RAW XERO DATA DUMP ==========");

  const transformedOrganisation = transformOrganisation(xeroData.organisation);

  // Build a contact map for easy lookup
  const contactMap = {};
  if (Array.isArray(xeroData.contacts)) {
    xeroData.contacts.forEach((contact) => {
      contactMap[contact.ContactID] = transformContact(contact);
    });
  }

  const transformedInvoices = transformInvoices(xeroData.invoices, contactMap);

  const transformedData = transformedInvoices.map((invoice) => ({
    ...transformedOrganisation,
    ...invoice,
  }));

  console.log("========== TRANSFORMED DATA ==========");
  console.log(JSON.stringify(transformedData.slice(0, 5), null, 2));
  console.log("========== END TRANSFORMED DATA ==========");

  logger.logEvent("info", "Xero data transformation complete");
  return transformedData;
}

module.exports = { transformXeroData };

transformXeroData(xeroData).then(() => console.log("Transform complete."));

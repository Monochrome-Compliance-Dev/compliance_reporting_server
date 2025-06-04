// const xeroData = require("../../docs/xeroApiDump.json");

const { logger } = require("../../helpers/logger");
const { transformOrganisation } = require("./transformOrganisation");
const { transformContact } = require("./transformContact");
const { transformPayments } = require("./transformPayments");
const { transformInvoices } = require("./transformInvoices");

const validationWarnings = [];
const isDev = process.env.NODE_ENV === "development";

function normalizeImportedRecord(record) {
  const normalized = { ...record };

  for (const key in normalized) {
    if (typeof normalized[key] === "string" && normalized[key].trim() === "") {
      normalized[key] = null;
    }
  }

  const abnPattern = /^\d{11}$/;
  if (
    normalized.payerEntityAbn &&
    !abnPattern.test(normalized.payerEntityAbn)
  ) {
    validationWarnings.push(
      `Invalid payerEntityAbn: ${normalized.payerEntityAbn}`
    );
  }
  if (
    normalized.payeeEntityAbn &&
    !abnPattern.test(normalized.payeeEntityAbn)
  ) {
    validationWarnings.push(
      `Invalid payeeEntityAbn: ${normalized.payeeEntityAbn}`
    );
  }
  if (!normalized.payerEntityAbn) {
    validationWarnings.push(`Missing payerEntityAbn`);
  }
  if (!normalized.payeeEntityAbn) {
    validationWarnings.push(`Missing payeeEntityAbn`);
  }

  return normalized;
}

async function transformXeroData(xeroData) {
  logger.logEvent("info", "Starting Xero data transformation");
  console.log("========== RAW XERO DATA DUMP ==========");
  console.log(JSON.stringify(xeroData.payments.slice(0, 5), null, 2));
  console.log(JSON.stringify(xeroData.invoices.slice(0, 5), null, 2));
  console.log(JSON.stringify(xeroData.contacts.slice(0, 5), null, 2));
  console.log("========== END RAW XERO DATA DUMP ==========");

  const transformedOrganisation = transformOrganisation(xeroData.organisation);

  // Normalize and transform payments
  const normalizedPayments = xeroData.payments.map(normalizeImportedRecord);
  const normalizedInvoices = xeroData.invoices.map(normalizeImportedRecord);
  const transformedPayments = transformPayments(normalizedPayments);
  const transformedInvoices = transformInvoices(normalizedInvoices);

  // Build a contact map for easy lookup
  const contactMap = {};
  if (Array.isArray(xeroData.contacts)) {
    xeroData.contacts.map(normalizeImportedRecord).forEach((contact) => {
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

  if (validationWarnings.length > 0) {
    logger.logEvent(
      "warn",
      `Validation warnings:\n${validationWarnings.join("\n")}`
    );
    if (isDev) {
      console.warn("========== VALIDATION WARNINGS ==========");
      console.warn(validationWarnings.join("\n"));
      console.warn("========== END VALIDATION WARNINGS ==========");
    }
  }

  logger.logEvent("info", "Xero data transformation complete");
  return transformedData;
}

module.exports = { transformXeroData };

// transformXeroData(xeroData).then(() => console.log("Transform complete."));

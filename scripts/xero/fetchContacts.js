const axios = require("../helpers/xeroApi");
const fs = require("fs");
const pLimit = require("p-limit");
require("dotenv").config();

const contactIds = JSON.parse(
  fs.readFileSync("./scripts/xero/contactIds.json", "utf8")
);
const limit = pLimit(5); // concurrency control

const fetchContactDetails = async (contactId) => {
  try {
    const response = await axios.get(`/Contacts/${contactId}`);
    const contact = response.data.Contacts[0];
    return {
      contactId: contact.ContactID,
      name: contact.Name,
      abn: contact.TaxNumber || "",
      acn: contact.CompanyNumber || "",
      paymentTerms: contact.PaymentTerms || "Unknown Terms",
    };
  } catch (err) {
    console.error(
      `Error fetching ${contactId}:`,
      err.response?.data || err.message
    );
    return null;
  }
};

const fetchAllContacts = async () => {
  const results = await Promise.all(
    contactIds.map((id) => limit(() => fetchContactDetails(id)))
  );
  const contactMap = {};
  results.filter(Boolean).forEach((c) => {
    contactMap[c.contactId] = {
      abn: c.abn,
      acn: c.acn,
      paymentTerms: c.paymentTerms,
    };
  });

  fs.writeFileSync(
    "./scripts/xero/contactMap.json",
    JSON.stringify(contactMap, null, 2)
  );
  console.log("Contact map written to scripts/xero/contactMap.json");
};

fetchAllContacts();

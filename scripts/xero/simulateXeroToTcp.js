// simulateXeroToTcp.js

require("dotenv").config(); // <-- Just use the default
const { sequelize } = require("../../db/database");
const { transformXeroData } = require("./transformXeroData");
const tcpService = require("../../tcp/tcp.service");

// Sequelize models
const defineXeroOrganisation = require("../../xero/xero_organisation.model");
const XeroOrganisation = defineXeroOrganisation(sequelize);

const defineXeroInvoice = require("../../xero/xero_invoice.model");
const XeroInvoice = defineXeroInvoice(sequelize);

const defineXeroPayment = require("../../xero/xero_payment.model");
const XeroPayment = defineXeroPayment(sequelize);

const defineXeroContact = require("../../xero/xero_contact.model");
const XeroContact = defineXeroContact(sequelize);

(async () => {
  await sequelize.authenticate();
  console.log("✅ Connected to DB:", sequelize.config.database);
  try {
    const customerId = "rahfwOLxLN";
    const reportId = "TLxs1MVSIq";
    const createdBy = "j3HJwUR_pi";

    // 1. Fetch raw data
    const organisations = await XeroOrganisation.findAll({
      where: { customerId, reportId },
    });
    const invoices = await XeroInvoice.findAll({
      where: { customerId, reportId },
    });
    const payments = await XeroPayment.findAll({
      where: { customerId, reportId },
    });
    const contacts = await XeroContact.findAll({
      where: { customerId, reportId },
    });

    const xeroData = {
      organisations: organisations.map((o) => o.toJSON()),
      invoices: invoices.map((i) => i.toJSON()),
      payments: payments.map((p) => p.toJSON()),
      contacts: contacts.map((c) => c.toJSON()),
    };

    // 2. Transform data
    const transformedXeroData = await transformXeroData(xeroData);

    console.log(
      "🔍 First record for validation check:",
      JSON.stringify(transformedXeroData[0], null, 2)
    );

    console.log(
      "🔍 First record for validation check:",
      JSON.stringify(transformedXeroData[1], null, 2)
    );
    // 3. Insert into tcp
    const result = await tcpService.saveTransformedDataToTcp(
      transformedXeroData,
      reportId,
      customerId,
      createdBy
    );

    if (result) {
      console.log("✅ TCP records inserted successfully.");
    }
  } catch (err) {
    console.error("❌ Error running simulated Xero to TCP flow:", err);
  } finally {
    await sequelize.close();
  }
})();

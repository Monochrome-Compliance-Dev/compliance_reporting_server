const path = require("path");
const dotenv = require("dotenv");

const envPath = path.resolve(__dirname, "../../.env.development");
const result = dotenv.config({ path: envPath });

if (result.error) {
  console.error("❌ Failed to load .env.development:", result.error);
  process.exit(1);
}

const { transformXeroData } = require("./transformXeroData");
const tcpService = require("../../tcp/tcp.service");
const { logger } = require("../../helpers/logger");
const { sequelize } = require("../../db/database");
const XeroInvoice = require("../../xero/xero_invoice.model")(sequelize);
const XeroPayment = require("../../xero/xero_payment.model")(sequelize);
const XeroContact = require("../../xero/xero_contact.model")(sequelize);
const XeroOrganisation = require("../../xero/xero_organisation.model")(
  sequelize
);
const XeroBankTransaction = require("../../xero/xero_bank_txn.model")(
  sequelize
);
// Add other Xero models as needed

async function replay() {
  const clientId = "rahfwOLxLN"; // replace with actual clientId
  const reportId = "Gj476reIr2"; // replace with actual reportId
  const createdBy = "j3HJwUR_pi"; // replace with actual userId

  try {
    const xeroData = {
      invoices: await XeroInvoice.findAll({
        where: { clientId, reportId },
        raw: true,
      }),
      payments: await XeroPayment.findAll({
        where: { clientId, reportId },
        raw: true,
      }),
      contacts: await XeroContact.findAll({
        where: { clientId, reportId },
        raw: true,
      }),
      organisations: await XeroOrganisation.findAll({
        where: { clientId, reportId },
        raw: true,
      }),
      bankTransactions: await XeroBankTransaction.findAll({
        where: { clientId, reportId },
        raw: true,
      }),
      // Add others here if needed
    };
    // console.log("invoices", xeroData.invoices.slice(0, 5));
    // console.log("payments", xeroData.payments.slice(0, 5));
    // console.log("contacts", xeroData.contacts.slice(0, 5));
    // console.log("organisations", xeroData.organisations.slice(0, 5));
    // console.log("bankTransactions", xeroData.bankTransactions.slice(0, 5));

    const transformed = await transformXeroData(xeroData);
    // console.log("Transformed Data:", transformed.slice(0, 5));

    const result = await tcpService.saveTransformedDataToTcp(
      transformed,
      reportId,
      clientId,
      createdBy
    );
    logger.logEvent("info", "TCP data reloaded from existing Xero records", {
      result,
    });
    console.log("✅ TCP save complete.");
  } catch (err) {
    logger.logEvent("error", "TCP replay failed", { error: err.message });
    console.error("❌ TCP replay failed:", err.message);
  }
}

replay();

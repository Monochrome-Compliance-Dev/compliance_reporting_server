// lastTwoSteps.js
const {
  getAllXeroData,
  transformXeroData,
} = require("../../xero/xero.service");
const db = require("../../db/database");

const clientId = "-R1o0gWoZX";
const reportId = "aKsGqMniuE";

(async () => {
  await db.sequelize.authenticate();
  await db.sequelize.sync(); // Ensure models are loaded and available

  try {
    await getAllXeroData(clientId, reportId, db);
    console.log("✅ Fetched all Xero data");

    await transformXeroData(clientId, reportId, db);
    console.log("✅ Transformed Xero data");

    console.log("🎉 Done!");
  } catch (error) {
    console.error("❌ Error:", error);
  }
})();

// db/initRLS.js
const fs = require("fs");
const path = require("path");
const { sequelize } = require("../models"); // adjust to your Sequelize instance

async function initialiseRLS() {
  const rlsSqlPath = path.join(__dirname, "setup_rls.sql");
  const rlsSql = fs.readFileSync(rlsSqlPath, "utf-8");

  try {
    await sequelize.query(rlsSql);
    console.log("✅ RLS policies have been initialised.");
  } catch (error) {
    console.error("❌ Failed to initialise RLS:", error);
  }
}

module.exports = initialiseRLS;

const path = require("path");
require("dotenv").config({
  path: path.resolve(__dirname, "../.env.development"),
});
const { Sequelize } = require("sequelize");
const adminContentModel = require("../admin/admin.model"); // adjust if needed
const data = require("../docs/blogfaq.json");

const sequelize = new Sequelize(process.env.DB_URL, {
  dialect: "postgres",
  logging: false,
});

(async () => {
  try {
    const AdminContent = adminContentModel(sequelize);
    await sequelize.authenticate();
    console.log("✅ Connected to SIT Postgres");

    await AdminContent.sync(); // optional: ensure model is in sync
    const result = await AdminContent.bulkCreate(data, {
      ignoreDuplicates: true,
    });

    console.log(`✅ Successfully inserted ${result.length} records.`);
  } catch (err) {
    console.error("❌ Import failed:", err);
  } finally {
    await sequelize.close();
  }
})();

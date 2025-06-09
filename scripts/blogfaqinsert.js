const path = require("path");

// Set NODE_ENV if not already set
process.env.NODE_ENV = process.env.NODE_ENV || "development";

// Resolve and load .env file early before importing other modules
const envFilePath = path.resolve(__dirname, `../.env.${process.env.NODE_ENV}`);
require("dotenv").config({ path: envFilePath });

// Debug logs
console.log(`🔧 Running in NODE_ENV=${process.env.NODE_ENV}`);
console.log(`🔧 Loaded env from ${envFilePath}`);
console.log(`🔧 Resolved DB_URL=${process.env.DB_URL}`);

const { Sequelize } = require("sequelize");
const adminContentModel = require("../admin/admin.model");
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

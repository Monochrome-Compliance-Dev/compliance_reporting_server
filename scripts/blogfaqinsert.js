const path = require("path");

// Load .env file conditionally if DB_URL isn't already present
if (!process.env.DB_URL) {
  const nodeEnv = process.env.NODE_ENV;
  if (!nodeEnv) {
    console.error("❌ NODE_ENV not set and DB_URL not provided");
    process.exit(1);
  }

  const envFilePath = path.resolve(__dirname, `../.env.${nodeEnv}`);
  require("dotenv").config({ path: envFilePath });
  console.log(`🔧 Loaded env from ${envFilePath}`);
} else {
  console.log("🔧 Using DB_URL from existing environment");
}

console.log(`🔧 Running in NODE_ENV=${process.env.NODE_ENV}`);
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

const express = require("express");
const { Sequelize, DataTypes } = require("sequelize");

const app = express();
app.use(express.json());

// Middleware to inject a hardcoded customerId for RLS testing within a transaction
app.use(async (req, res, next) => {
  const customerId = "FzNsRwbtXi"; // Hardcoded customerId for testing
  try {
    const transaction = await sequelize.transaction();
    await sequelize.query(
      `SET LOCAL app.current_customer_id = '${customerId}'`,
      {
        transaction,
        raw: true,
      }
    );
    // Save the transaction for downstream use
    req.dbTransaction = transaction;
    req.body.customerId = customerId; // Ensure it's available for validation
    next();
  } catch (error) {
    console.error("Error in customerId middleware:", error.message);
    next(error);
  }
});

const sequelize = new Sequelize(
  "compliance_reporting",
  "appuser",
  process.env.DB_PASSWORD,
  {
    host: "localhost",
    dialect: "postgres",
    logging: false,
    pool: {
      max: 10,
      min: 0,
      acquire: 30000,
      idle: 10000,
    },
  }
);

// User model
const User = sequelize.define("User", {
  id: {
    type: DataTypes.INTEGER,
    primaryKey: true,
    autoIncrement: true,
  },
  email: {
    type: DataTypes.STRING,
    allowNull: false,
  },
  customerId: {
    type: DataTypes.STRING,
    allowNull: false,
  },
});

// GET all users
app.get("/users", async (req, res) => {
  try {
    const users = await User.findAll({ transaction: req.dbTransaction });
    await req.dbTransaction.commit();
    res.json(users);
  } catch (error) {
    console.error("Error:", error);
    await req.dbTransaction.rollback();
    res.status(500).json({ error: "Something went wrong" });
  }
});

// POST a user (with RLS enforced)
app.post("/users", async (req, res) => {
  const { email, customerId } = req.body;
  try {
    const user = await User.create(
      { email, customerId },
      { transaction: req.dbTransaction }
    );
    await req.dbTransaction.commit();
    res.status(201).json(user);
  } catch (error) {
    console.error("Error creating user:", error);
    await req.dbTransaction.rollback();
    res.status(500).json({ error: "Something went wrong" });
  }
});

// Start the server
const PORT = 4000;
app.listen(PORT, async () => {
  try {
    await sequelize.authenticate();
    console.log("✅ Database connection successful.");
    await User.sync();
    await sequelize.query(`
      ALTER TABLE "Users" ENABLE ROW LEVEL SECURITY;
      DROP POLICY IF EXISTS user_customer_rls ON "Users";
      CREATE POLICY user_customer_rls ON "Users"
        USING ("customerId" = current_setting('app.current_customer_id', true)::text)
        WITH CHECK ("customerId" = current_setting('app.current_customer_id', true)::text);
    `);
    console.log("✅ User table synced.");
    console.log(`🚀 Server running at http://localhost:${PORT}`);
  } catch (error) {
    console.error("❌ Startup error:", error);
  }
});

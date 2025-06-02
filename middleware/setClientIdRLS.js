// middleware/setClientIdRLS.js
const { sequelize } = require("../db/database"); // adjust to your Sequelize instance

const setClientIdRLS = async (req, res, next) => {
  try {
    const clientId = req.auth?.clientId; // updated to use req.auth
    if (!clientId) {
      console.warn("No clientId found for RLS - skipping.");
      return next();
    }

    const safeClientId = clientId.replace(/[^a-zA-Z0-9-_]/g, "");

    // If no existing transaction, create a new one
    if (!req.dbTransaction) {
      req.dbTransaction = await sequelize.transaction();
    }

    // Use the same transaction for SET LOCAL
    await sequelize.query(
      `SET LOCAL app.current_client_id = '${safeClientId}'`,
      { transaction: req.dbTransaction }
    );

    next();
  } catch (error) {
    if (error.message.includes("unrecognized configuration parameter")) {
      console.warn(
        "RLS parameter app.current_client_id not found in database, skipping SET LOCAL."
      );
      return next();
    }
    console.error("Error setting RLS clientId:", error);
    res.status(500).json({ message: "Error setting RLS" });
  }
};

module.exports = setClientIdRLS;
